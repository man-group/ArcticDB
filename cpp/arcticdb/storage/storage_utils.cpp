#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/async/tasks.hpp>

namespace arcticdb::storage {

AtomKey write_table_index_tree_from_source_to_target(
    const std::shared_ptr<Store>& source_store,
    const std::shared_ptr<Store>& target_store,
    const AtomKey& index_key,
    std::optional<VersionId> new_version_id
) {
    ARCTICDB_SAMPLE(WriteIndexSourceToTarget, 0)
    // In
    auto [_, index_seg] = source_store->read_sync(index_key);
    index::IndexSegmentReader index_segment_reader{std::move(index_seg)};
    // Out
    index::IndexWriter<stream::RowCountIndex> writer(target_store,
            {index_key.id(), new_version_id.value_or(index_key.version_id())},
            std::move(index_segment_reader.mutable_tsd()));
    std::vector<folly::Future<bool>> futures;
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
        futures.emplace_back(async::submit_io_task(async::CopyCompressedInterStoreTask{sk.key(),
                                                                                       std::move(key_to_write),
                                                                                       false,
                                                                                       false,
                                                                                       source_store,
                                                                                       {target_store}}));
    }
    collect(futures).get();
    // FUTURE: clean up already written keys if exception
    return to_atom(writer.commit().get());
}

AtomKey copy_multi_key_from_source_to_target(
        const std::shared_ptr<Store>& source_store,
        const std::shared_ptr<Store>& target_store,
        const AtomKey& index_key,
        std::optional<VersionId> new_version_id) {
    using namespace arcticdb::stream;
    auto fut_index = source_store->read(index_key);
    auto [_, index_seg] = std::move(fut_index).get();
    std::vector<AtomKey> keys;
    for (size_t idx = 0; idx < index_seg.row_count(); idx++) {
        keys.push_back(stream::read_key_row(index_seg, static_cast<ssize_t>(idx)));
    }
    // Recurse on the index keys inside MULTI_KEY
    std::vector<VariantKey> new_data_keys;
    for (const auto &k: keys) {
        auto new_key = copy_index_key_recursively(source_store, target_store, k, new_version_id);
        new_data_keys.emplace_back(std::move(new_key));
    }
    // Write new MULTI_KEY
    google::protobuf::Any metadata = *index_seg.metadata();
    folly::Future<VariantKey> multi_key_fut = folly::Future<VariantKey>::makeEmpty();
    IndexAggregator<RowCountIndex> multi_index_agg(index_key.id(), [&new_version_id, &index_key, &multi_key_fut, &target_store](auto &&segment) {
        multi_key_fut = target_store->write(KeyType::MULTI_KEY,
                                            new_version_id.value_or(index_key.version_id()),  // version_id
                                            index_key.id(),
                                            0,  // start_index
                                            0,  // end_index
                                            std::forward<SegmentInMemory>(segment)).wait();
    });
    for (auto &key: new_data_keys) {
        multi_index_agg.add_key(to_atom(key));
    }
    multi_index_agg.set_metadata(std::move(metadata));
    multi_index_agg.commit();
    return to_atom(multi_key_fut.value());
}

AtomKey copy_index_key_recursively(
        const std::shared_ptr<Store>& source_store,
        const std::shared_ptr<Store>& target_store,
        const AtomKey& index_key,
        std::optional<VersionId> new_version_id) {
    ARCTICDB_SAMPLE(RecurseIndexKey, 0)
    if (index_key.type() == KeyType::TABLE_INDEX) {
        return write_table_index_tree_from_source_to_target(source_store, target_store, index_key, new_version_id);
    } else if (index_key.type() == KeyType::MULTI_KEY) {
        return copy_multi_key_from_source_to_target(source_store, target_store, index_key, new_version_id);
    }
}

}