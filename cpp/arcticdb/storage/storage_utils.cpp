#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/async/tasks.hpp>
#include <folly/futures/Future.h>

namespace arcticdb {

std::vector<VariantKey> filter_keys_on_existence(
        const std::vector<VariantKey>& keys, const std::shared_ptr<Store>& store, bool pred
) {
    auto key_existence = folly::collect(store->batch_key_exists(keys)).get();
    std::vector<VariantKey> res;
    for (size_t i = 0; i != keys.size(); i++) {
        if (key_existence[i] == pred) {
            res.push_back(keys[i]);
        }
    }
    return res;
}

void filter_keys_on_existence(std::vector<AtomKey>& keys, const std::shared_ptr<Store>& store, bool pred) {
    std::vector<VariantKey> var_vector;
    var_vector.reserve(keys.size());
    rng::copy(keys, std::back_inserter(var_vector));

    auto key_existence = store->batch_key_exists(var_vector);

    auto keys_itr = keys.begin();
    for (size_t i = 0; i != var_vector.size(); i++) {
        bool resolved = key_existence[i].wait().value();
        if (resolved == pred) {
            *keys_itr = std::move(std::get<AtomKey>(var_vector[i]));
            ++keys_itr;
        }
    }
    keys.erase(keys_itr, keys.end());
}

AtomKey write_table_index_tree_from_source_to_target(
        const std::shared_ptr<Store>& source_store, const std::shared_ptr<Store>& target_store,
        const AtomKey& index_key, std::optional<VersionId> new_version_id
) {
    ARCTICDB_SAMPLE(WriteIndexSourceToTarget, 0)
    // In
    auto [_, index_seg] = source_store->read_sync(index_key);
    index::IndexSegmentReader index_segment_reader{std::move(index_seg)};
    // Out
    index::IndexWriter<stream::RowCountIndex> writer(
            target_store,
            {index_key.id(), new_version_id.value_or(index_key.version_id())},
            std::move(index_segment_reader.mutable_tsd()),
            /*key_type =*/std::nullopt,
            /*sync =*/true
    );

    std::vector<folly::Future<async::CopyCompressedInterStoreTask::ProcessingResult>> futures;

    // Process
    for (auto iter = index_segment_reader.begin(); iter != index_segment_reader.end(); ++iter) {
        auto& sk = *iter;
        auto& key = sk.key();
        std::optional<entity::AtomKey> key_to_write = atom_key_builder()
                                                              .version_id(new_version_id.value_or(key.version_id()))
                                                              .creation_ts(util::SysClock::nanos_since_epoch())
                                                              .start_index(key.start_index())
                                                              .end_index(key.end_index())
                                                              .content_hash(key.content_hash())
                                                              .build(key.id(), key.type());

        writer.add(*key_to_write, sk.slice()); // Both const ref
        futures.emplace_back(submit_io_task(async::CopyCompressedInterStoreTask{
                sk.key(), std::move(key_to_write), false, false, source_store, {target_store}
        }));
    }

    const std::vector<async::CopyCompressedInterStoreTask::ProcessingResult> store_results = collect(futures).get();
    for (const async::CopyCompressedInterStoreTask::ProcessingResult& res : store_results) {
        util::variant_match(
                res,
                [&](const async::CopyCompressedInterStoreTask::FailedTargets& failed) {
                    log::storage().error(
                            "Failed to move targets: {} from {} to {}",
                            failed,
                            source_store->name(),
                            target_store->name()
                    );
                },
                [](const auto&) {}
        );
    }
    // FUTURE: clean up already written keys if exception
    return writer.commit_sync();
}

AtomKey copy_multi_key_from_source_to_target(
        const std::shared_ptr<Store>& source_store, const std::shared_ptr<Store>& target_store,
        const AtomKey& index_key, std::optional<VersionId> new_version_id
) {
    using namespace arcticdb::stream;
    auto [_, index_seg] = source_store->read_sync(index_key);
    std::vector<AtomKey> keys;
    for (size_t idx = 0; idx < index_seg.row_count(); idx++) {
        keys.push_back(stream::read_key_row(index_seg, static_cast<ssize_t>(idx)));
    }
    // Recurse on the index keys inside MULTI_KEY
    std::vector<VariantKey> new_data_keys;
    for (const auto& k : keys) {
        auto new_key = copy_index_key_recursively(source_store, target_store, k, new_version_id);
        new_data_keys.emplace_back(std::move(new_key));
    }
    // Write new MULTI_KEY

    VariantKey multi_key;
    IndexAggregator<RowCountIndex> multi_index_agg(
            index_key.id(),
            [&new_version_id, &index_key, &multi_key, &target_store](auto&& segment) {
                multi_key = target_store->write_sync(
                        KeyType::MULTI_KEY,
                        new_version_id.value_or(index_key.version_id()), // version_id
                        index_key.id(),
                        0, // start_index
                        0, // end_index
                        std::forward<SegmentInMemory>(segment)
                );
            }
    );
    for (auto& key : new_data_keys) {
        multi_index_agg.add_key(to_atom(key));
    }
    if (index_seg.has_metadata()) {
        google::protobuf::Any metadata = *index_seg.metadata();
        multi_index_agg.set_metadata(std::move(metadata));
    }
    if (index_seg.has_index_descriptor()) {
        multi_index_agg.set_timeseries_descriptor(index_seg.index_descriptor());
    }
    multi_index_agg.commit();
    return to_atom(multi_key);
}

AtomKey copy_index_key_recursively(
        const std::shared_ptr<Store>& source_store, const std::shared_ptr<Store>& target_store,
        const AtomKey& index_key, std::optional<VersionId> new_version_id
) {
    ARCTICDB_SAMPLE(RecurseIndexKey, 0)
    if (index_key.type() == KeyType::TABLE_INDEX) {
        return write_table_index_tree_from_source_to_target(source_store, target_store, index_key, new_version_id);
    } else if (index_key.type() == KeyType::MULTI_KEY) {
        return copy_multi_key_from_source_to_target(source_store, target_store, index_key, new_version_id);
    }
    internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
            "Cannot copy index recursively. Unsupported index key type {}", index_key.type()
    );
}

} // namespace arcticdb