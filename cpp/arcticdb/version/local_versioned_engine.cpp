/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/util/optional_defaults.hpp>
#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/util/storage_lock.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/version/version_tasks.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/util/container_filter_wrapper.hpp>

namespace arcticdb::version_store {

LocalVersionedEngine::LocalVersionedEngine(
        const std::shared_ptr<storage::Library>& library) :
    store_(std::make_shared<async::AsyncStore<util::SysClock>>(library, codec::default_lz4_codec())),
    symbol_list_(std::make_shared<SymbolList>(version_map_)){
    configure(library->config());
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Created versioned engine at {} for library path {}  with config {}", uintptr_t(this),
                         library->library_path(), [&cfg=cfg_]{  return util::format(cfg); });
#ifdef USE_REMOTERY
    auto temp = RemoteryInstance::instance();
#endif
    ARCTICDB_SAMPLE_THREAD();
    ARCTICDB_SAMPLE(LocalVersionedEngine, 0)
    if(async::TaskScheduler::is_forked()) {
        async::TaskScheduler::set_forked(false);
        async::TaskScheduler::reattach_instance();
    }
}

void LocalVersionedEngine::delete_unreferenced_pruned_indexes(
        const std::vector<AtomKey> &pruned_indexes,
        const AtomKey& key_to_keep
) {
    try {
        if (!pruned_indexes.empty() && !cfg().write_options().delayed_deletes()) {
            auto [not_in_snaps, in_snaps] = get_index_keys_partitioned_by_inclusion_in_snapshots(
                    store(),
                    pruned_indexes.begin()->id(),
                    pruned_indexes);
            in_snaps.insert(key_to_keep);
            PreDeleteChecks checks{false, false, false, false, in_snaps};
            delete_trees_responsibly(not_in_snaps, {}, {}, checks);
        }
    } catch (const std::exception &ex) {
        // Best-effort so deliberately swallow
        log::version().warn("Could not prune previous versions due to: {}", ex.what());
    }
}

FrameAndDescriptor LocalVersionedEngine::read_dataframe_internal(
    const std::variant<VersionedItem, StreamId>& identifier,
    ReadQuery& read_query,
    const ReadOptions& read_options) {
    ARCTICDB_RUNTIME_SAMPLE(ReadDataFrameInternal, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: read_dataframe");
    return read_dataframe_impl(
        store(),
        identifier,
        read_query,
        read_options);
}

std::set<StreamId> LocalVersionedEngine::list_streams_internal(
    std::optional<SnapshotId> snap_name,
    const std::optional<std::string>& regex,
    const std::optional<std::string>& prefix,
    const std::optional<bool>& opt_use_symbol_list,
    const std::optional<bool>& opt_all_symbols
    ) {
    ARCTICDB_SAMPLE(ListStreamsInternal, 0)
    auto res = std::set<StreamId>();
    bool use_symbol_list = opt_use_symbol_list.value_or(cfg().symbol_list());

    if (snap_name) {
        res = list_streams_in_snapshot(store(), snap_name.value());
    } else {
        if(use_symbol_list)
            res = symbol_list().get_symbol_set(store());
        else
            res = list_streams(store(), version_map(), prefix, opt_false(opt_all_symbols));
    }

    if (regex) {
        return filter_by_regex(res, regex);
    } else if (prefix) {
        return filter_by_regex(res, std::optional("^" + prefix.value()));
    }
    return res;
}

std::string LocalVersionedEngine::dump_versions(const StreamId& stream_id) {
    return version_map()->dump_entry(store(), stream_id);
}

std::optional<VersionedItem> LocalVersionedEngine::get_latest_version(
    const StreamId &stream_id,
    const VersionQuery& version_query) {
    auto key = get_latest_undeleted_version(store(), version_map(), stream_id,  opt_true(version_query.skip_compat_), opt_false(version_query.iterate_on_failure_));
    if (!key) {
        ARCTICDB_DEBUG(log::version(), "get_latest_version didn't find version for stream_id: {}", stream_id);
        return std::nullopt;
    }
    return VersionedItem{std::move(key.value())};
}

std::optional<VersionedItem> LocalVersionedEngine::get_specific_version(
    const StreamId &stream_id,
    VersionId version_id,
    const VersionQuery& version_query) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: get_specific_version");
    auto key = ::arcticdb::get_specific_version(store(), version_map(), stream_id, version_id, opt_true(version_query.skip_compat_),
                                                   opt_true(version_query.iterate_on_failure_));
    if (!key) {
        log::version().warn("Version {} for symbol {} is missing, checking snapshots (this can be slow)", version_id, stream_id);
        auto index_keys = get_index_keys_in_snapshots(store(), stream_id);
        auto index_key = std::find_if(index_keys.begin(), index_keys.end(), [version_id](const AtomKey& k) {
            return k.version_id() == version_id;
        });
        if (index_key != index_keys.end()) {
            ARCTICDB_DEBUG(log::version(), "Found version {} for symbol {} in snapshot:", version_id, stream_id);
            key = *index_key;
        } else {
            ARCTICDB_DEBUG(log::version(), "get_specific_version: "
                                 "version id not found for stream {} version {}", stream_id, version_id);
            return std::nullopt;
        }
    }
    return VersionedItem{std::move(key.value())};
}

std::optional<VersionedItem> LocalVersionedEngine::get_version_at_time(
    const StreamId& stream_id,
    timestamp as_of,
    const VersionQuery& version_query
    ) {

    auto version_key =
        get_version_key_from_time(store(), version_map(), stream_id, as_of, false, opt_true(version_query.iterate_on_failure_));
    std::optional<AtomKey> key;
    if (!version_key) {
        auto index_keys = get_index_keys_in_snapshots(store(), stream_id);
        auto vector_index_keys = std::vector<AtomKey>(index_keys.begin(), index_keys.end());
        std::sort(std::begin(vector_index_keys), std::end(vector_index_keys),
                  [](auto& k1, auto& k2) {return k1.creation_ts() > k2.creation_ts();});
        key = get_version_key_from_time_for_versions(as_of, vector_index_keys);
    } else {
        auto version_id = version_key.value().version_id();
        key = ::arcticdb::get_specific_version(store(), version_map(),
            stream_id,
            version_id,
            opt_true(version_query.skip_compat_),
            opt_true(version_query.iterate_on_failure_));
    }

    if (!key) {
        log::version().warn("read_dataframe_timestamp: version id not found for stream {} timestamp {}", stream_id, as_of);
        return std::nullopt;
    }

    return VersionedItem(std::move(key.value()));
}

std::optional<VersionedItem> LocalVersionedEngine::get_version_from_snapshot(
    const StreamId& stream_id,
    const SnapshotId& snap_name
    ) {
    auto opt_snapshot =  get_snapshot(store(), snap_name);
    if (!opt_snapshot) {
        throw storage::NoDataFoundException(snap_name);
    }
    // A snapshot will normally be in a ref key, but for old libraries it still needs to fall back to iteration of
    // atom keys.
    auto segment = opt_snapshot.value().second;
    for (size_t idx = 0; idx < segment.row_count(); idx++) {
        auto stream_index = read_key_row(segment, static_cast<ssize_t>(idx));
        if (stream_index.id() == stream_id) {
            return VersionedItem{std::move(stream_index)};
        }
    }
    ARCTICDB_DEBUG(log::version(), "read_snapshot: {} id not found for snapshot {}", stream_id, snap_name);
    return std::nullopt;

}

std::optional<VersionedItem> LocalVersionedEngine::get_version_to_read(
    const StreamId &stream_id,
    const VersionQuery &version_query
    ) {
    return util::variant_match(version_query.content_,
       [&stream_id, &version_query, that=this](const SpecificVersionQuery &specific) {
            return that->get_specific_version(stream_id, specific.version_id_, version_query);
        },
        [&stream_id, that=this](const SnapshotVersionQuery &snapshot) {
            return that->get_version_from_snapshot(stream_id, snapshot.name_);
        },
        [&stream_id, &version_query, that=this](const TimestampVersionQuery &timestamp) {
            return that->get_version_at_time(stream_id, timestamp.timestamp_, version_query);
        },
        [&stream_id, &version_query, that=this](const std::monostate &) {
            return that->get_latest_version(stream_id, version_query);
    }
    );
}

IndexRange LocalVersionedEngine::get_index_range(
    const StreamId &stream_id,
    const VersionQuery& version_query) {
    auto version = get_version_to_read(stream_id, version_query);
    if(!version)
        return unspecified_range();

    return index::get_index_segment_range(version.value().key_, store());
}

std::pair<VersionedItem, FrameAndDescriptor> LocalVersionedEngine::read_dataframe_version_internal(
    const StreamId &stream_id,
    const VersionQuery& version_query,
    ReadQuery& read_query,
    const ReadOptions& read_options) {
    auto version = get_version_to_read(stream_id, version_query);
    std::variant<VersionedItem, StreamId> identifier;
    if(!version) {
        if(opt_false(read_options.incompletes_)) {
            log::version().warn("No index:  Key not found for {}, will attempt to use incomplete segments.", stream_id);
            identifier = stream_id;
        }
        else
            throw storage::NoDataFoundException(fmt::format("read_dataframe_version: version not found for symbol '{}'", stream_id));
    }
    else  {
        identifier = version.value();
    }

    auto frame_and_descriptor = read_dataframe_internal(identifier, read_query, read_options);
    return std::make_pair(version.value_or(VersionedItem{}), std::move(frame_and_descriptor));
}

std::pair<VersionedItem, std::optional<google::protobuf::Any>> LocalVersionedEngine::read_descriptor_version_internal(
        const StreamId& stream_id,
        const VersionQuery& version_query
    ) {
    ARCTICDB_SAMPLE(ReadDescriptor, 0)

    auto version = get_version_to_read(stream_id, version_query);
    version::check<ErrorCode::E_NO_SUCH_VERSION>(static_cast<bool>(version),
                                                 "Unable to retrieve descriptor data. {}@{}: version not found", stream_id, version_query);

    auto metadata_proto = store()->read_metadata(version->key_).get().second;
    return std::pair{version.value(), metadata_proto};
}

std::vector<std::pair<VersionedItem, std::optional<google::protobuf::Any>>> LocalVersionedEngine::batch_read_descriptor_internal(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries) {
    std::vector<folly::Future<std::pair<VersionedItem, std::optional<google::protobuf::Any>>>> results_fut;
    for (size_t idx=0; idx < stream_ids.size(); idx++) {
        auto version_query = version_queries.size() > idx ? version_queries[idx] : VersionQuery{};
        results_fut.push_back(read_descriptor_version_internal(stream_ids[idx],
                                                              version_query));
    }
    return folly::collect(results_fut).get();
}

void LocalVersionedEngine::flush_version_map() {
    version_map()->flush();
}

std::shared_ptr<DeDupMap> LocalVersionedEngine::get_de_dup_map(
    const StreamId& stream_id,
    const std::optional<AtomKey>& maybe_prev,
    const WriteOptions& write_options
    ){
    auto de_dup_map = std::make_shared<DeDupMap>();
    if (write_options.de_duplication) {
        auto maybe_undeleted_prev = get_latest_undeleted_version(store(), version_map(), stream_id, true, false);
        if (maybe_undeleted_prev) {
            // maybe_undeleted_prev is index key
            auto data_keys = get_data_keys(store(), {maybe_undeleted_prev.value()}, storage::ReadKeyOpts{});
            for (const auto& data_key: data_keys) {
                de_dup_map->insert_key(data_key);
            }
        } else if(maybe_prev && write_options.snapshot_dedup) {
            // This means we don't have any live versions(all tombstoned), so will try to dedup from snapshot versions
            auto snap_versions = get_index_keys_in_snapshots(store(), stream_id);
            auto max_iter = std::max_element(std::begin(snap_versions), std::end(snap_versions),
                                             [](const auto &k1, const auto &k2){return k1.version_id() < k2.version_id();});
            if (max_iter != snap_versions.end()) {
                auto data_keys = get_data_keys(store(), {*max_iter}, storage::ReadKeyOpts{});
                for (const auto& data_key: data_keys) {
                    de_dup_map->insert_key(data_key);
                }
            }
        }
    }
    return de_dup_map;
}


VersionedItem LocalVersionedEngine::sort_index(const StreamId& stream_id, bool dynamic_schema) {
    auto maybe_prev = get_latest_undeleted_version(store(), version_map(), stream_id, true, false);
    util::check(maybe_prev.has_value(), "Cannot delete from non-existent symbol {}", stream_id);
    auto version_id = get_next_version_from_key(maybe_prev.value());
    auto [index_segment_reader, slice_and_keys] = index::read_index_to_vector(store(), maybe_prev.value());
    if(dynamic_schema) {
        std::sort(std::begin(slice_and_keys), std::end(slice_and_keys), [](const auto &left, const auto &right) {
            return left.key().start_index() < right.key().start_index();
        });
    } else {
        std::sort(std::begin(slice_and_keys), std::end(slice_and_keys), [](const auto &left, const auto &right) {
            auto lt = std::tie(left.slice_.col_range.first, left.key().start_index());
            auto rt = std::tie(right.slice_.col_range.first, right.key().start_index());
            return lt < rt;
        });
    }

    auto total_rows = adjust_slice_rowcounts(slice_and_keys);
    auto index = index_type_from_descriptor(index_segment_reader.tsd().stream_descriptor());
    bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    auto time_series = make_descriptor(
        total_rows,
        StreamDescriptor{std::move(*index_segment_reader.mutable_tsd().mutable_stream_descriptor())},
        *index_segment_reader.mutable_tsd().mutable_normalization(),
        std::move(*index_segment_reader.mutable_tsd().mutable_user_meta()),
        std::nullopt,
        bucketize_dynamic);

    auto versioned_item = pipelines::index::index_and_version(index, store(), time_series, std::move(slice_and_keys), stream_id, version_id).get();
    version_map()->write_version(store(), versioned_item.key_);
    ARCTICDB_DEBUG(log::version(), "sorted index of stream_id: {} , version_id: {}", stream_id, version_id);
    return versioned_item;
}

VersionedItem LocalVersionedEngine::delete_range_internal(
    const StreamId& stream_id,
    const UpdateQuery & query,
    bool dynamic_schema) {
    auto maybe_prev = get_latest_undeleted_version(store(), version_map(), stream_id, true, false);
    util::check(maybe_prev.has_value(), "Cannot delete from non-existent symbol {}", stream_id);
    auto versioned_item = delete_range_impl(store(),
                                            maybe_prev.value(),
                                            query,
                                            get_write_options(),
                                            dynamic_schema);
    version_map()->write_version(store(), versioned_item.key_);
    return versioned_item;
}

VersionedItem LocalVersionedEngine::update_internal(
    const StreamId& stream_id,
    const UpdateQuery& query,
    InputTensorFrame&& frame_ref,
    bool upsert,
    bool dynamic_schema,
    bool prune_previous_versions) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: update");
    auto frame = std::move(frame_ref);
    auto update_info = get_latest_undeleted_version_and_next_version_id(store(),
                                                                        version_map(),
                                                                        stream_id,
                                                                        true,
                                                                        false);
    if (update_info.previous_index_key_.has_value()) {
        auto versioned_item = update_impl(store(),
                                          update_info,
                                          query,
                                          std::move(frame),
                                          get_write_options(),
                                          dynamic_schema);
        if (prune_previous_versions) {
            auto pruned_indexes = version_map()->write_and_prune_previous(store(), versioned_item.key_, update_info.previous_index_key_);
            delete_unreferenced_pruned_indexes(pruned_indexes, versioned_item.key_);
        } else {
            version_map()->write_version(store(), versioned_item.key_);
        }
        return versioned_item;
    } else {
        if (upsert) {
            auto write_options = get_write_options();
            write_options.dynamic_schema |= dynamic_schema;
            auto versioned_item =  write_dataframe_impl(store_,
                                                        update_info.next_version_id_,
                                                        std::move(frame),
                                                        write_options,
                                                        std::make_shared<DeDupMap>(),
                                                        false,
                                                        true);

            if(cfg_.symbol_list())
                symbol_list().add_symbol(store_, stream_id);

            version_map()->write_version(store(), versioned_item.key_);
            return versioned_item;
        } else {
            util::raise_rte("Cannot update non-existent symbol {}", stream_id);
        }
    }
}

VersionedItem LocalVersionedEngine::write_versioned_metadata_internal(
    const StreamId& stream_id,
    bool prune_previous_versions,
    arcticdb::proto::descriptors::UserDefinedMetadata&& user_meta
    ) {
    auto update_info = get_latest_undeleted_version_and_next_version_id(store(),
                                                                        version_map(),
                                                                        stream_id,
                                                                        true,
                                                                        false);
    util::check(update_info.previous_index_key_.has_value(), "No previous version exists for write metadata");
    ARCTICDB_DEBUG(log::version(), "write_versioned_dataframe for stream_id: {}", stream_id);
    auto index_key = UpdateMetadataTask{store(),
                                        update_info,
                                        std::move(user_meta)}();

    if(prune_previous_versions) {
        auto versioned_item = VersionedItem{index_key};
        auto pruned_indexes = version_map()->write_and_prune_previous(store(), index_key, update_info.previous_index_key_);
        delete_unreferenced_pruned_indexes(pruned_indexes, versioned_item.key_);
        return versioned_item;
    }
    else {
        version_map()->write_version(store(), index_key);
        return VersionedItem{index_key};
    }
}

VersionedItem LocalVersionedEngine::write_versioned_dataframe_internal(
    const StreamId& stream_id,
    InputTensorFrame&& frame,
    bool prune_previous_versions,
    bool allow_sparse,
    bool validate_index
    ) {
    ARCTICDB_SAMPLE(WriteVersionedDataFrame, 0)

    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write_versioned_dataframe");
    auto maybe_prev = ::arcticdb::get_latest_version(store(), version_map(), stream_id, true, false);
    auto version_id = get_next_version_from_key(maybe_prev);
    ARCTICDB_DEBUG(log::version(), "write_versioned_dataframe for stream_id: {} , version_id = {}", stream_id, version_id);
    auto write_options = get_write_options();
    auto de_dup_map = get_de_dup_map(stream_id, maybe_prev, write_options);

    auto versioned_item = write_dataframe_impl(
        store(),
        version_id,
        std::move(frame),
        write_options,
        de_dup_map,
        allow_sparse,
        validate_index);

    if(prune_previous_versions) {
        auto pruned_indexes = version_map()->write_and_prune_previous(store(), versioned_item.key_, maybe_prev);
        delete_unreferenced_pruned_indexes(pruned_indexes, versioned_item.key_);
        return versioned_item;
    }
    else {
        version_map()->write_version(store(), versioned_item.key_);
        return versioned_item;
    }
}

std::pair<VersionedItem, arcticdb::proto::descriptors::TimeSeriesDescriptor> LocalVersionedEngine::restore_version(
    const StreamId& stream_id,
    const VersionQuery& version_query
    ) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: res    tore_version");
    auto version_to_restore = get_version_to_read(stream_id, version_query);
    version::check<ErrorCode::E_NO_SUCH_VERSION>(static_cast<bool>(version_to_restore),
                                                 "Unable to restore {}@{}: version not found", stream_id, version_query);
    auto maybe_prev = ::arcticdb::get_latest_version(store(), version_map(), stream_id, true, false);
    ARCTICDB_DEBUG(log::version(), "restore for stream_id: {} , version_id = {}", stream_id, version_to_restore->key_.version_id());
    return AsyncRestoreVersionTask{store(), version_map(), stream_id, version_to_restore->key_, maybe_prev}().get();
}

std::pair<VersionedItem, std::vector<AtomKey>> LocalVersionedEngine::write_individual_segment(
    const StreamId& stream_id,
    SegmentInMemory&& segment,
    bool prune_previous_versions
    ) {
    ARCTICDB_SAMPLE(WriteVersionedDataFrame, 0)

    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write_versioned_dataframe");
    auto maybe_prev = ::arcticdb::get_latest_version(store(), version_map(), stream_id, true, false);
    auto version_id = get_next_version_from_key(maybe_prev);
    ARCTICDB_DEBUG(log::version(), "write_versioned_dataframe for stream_id: {} , version_id = {}", stream_id, version_id);
    auto index = index_type_from_descriptor(segment.descriptor());
    auto range = get_range_from_segment(index, segment);

    stream::StreamSink::PartialKey pk{
        KeyType::TABLE_DATA, version_id, stream_id, range.start_, range.end_
    };

    auto frame_slice = FrameSlice{segment};
    auto descriptor = get_timeseries_descriptor(segment.descriptor(), segment.row_count(), std::nullopt, {});
    auto key = store_->write(pk, std::move(segment)).get();
    std::vector sk{SliceAndKey{frame_slice, to_atom(key)}};
    auto index_key_fut = index::write_index(index, std::move(descriptor), std::move(sk), IndexPartialKey{stream_id, version_id}, store_);
    auto versioned_item = VersionedItem{to_atom(std::move(index_key_fut).get())};

    if(prune_previous_versions) {
        auto pruned_indexes = version_map()->write_and_prune_previous(store(), versioned_item.key_, maybe_prev);
        return std::make_pair(versioned_item, std::move(pruned_indexes));
    }
    else {
        version_map()->write_version(store(), versioned_item.key_);
        return std::make_pair(versioned_item, std::vector<AtomKey>{});
    }
}

// Steps of delete_trees_responsibly:
void copy_versions_nearest_to_target(
        const MasterSnapshotMap::value_type::second_type& keys_map,
        const IndexTypeKey& target_key,
        util::ContainerFilterWrapper<std::unordered_set<IndexTypeKey>>& not_to_delete) {
    const auto target_version = target_key.version_id();

    const IndexTypeKey* least_higher_version = nullptr;
    const IndexTypeKey* greatest_lower_version = nullptr;

    for (const auto& pair: keys_map) {
        const auto& key = pair.first;
        if (key != target_key) {
            const auto version = key.version_id();
            if (version > target_version) {
                if (!least_higher_version || least_higher_version->version_id() > version) {
                    least_higher_version = &key;
                }
            } else if (version < target_version) {
                if (!greatest_lower_version || greatest_lower_version->version_id() < version) {
                    greatest_lower_version = &key;
                }
            } else {
                log::version().warn("Found two distinct index keys for the same version in snapshots:\n{}\n{}",
                        key, target_key);
            }
        }
    }

    if (least_higher_version) {
        not_to_delete.insert(*least_higher_version);
    }
    if (greatest_lower_version) {
        not_to_delete.insert(*greatest_lower_version);
    }
}

std::unordered_map<StreamId, VersionId> min_versions_for_each_stream(const std::vector<AtomKey>& keys) {
    std::unordered_map<StreamId, VersionId> out;
    for (auto& key: keys) {
        auto found = out.find(key.id());
        if (found == out.end() || found->second > key.version_id()) {
            out[key.id()] = key.version_id();
        }
    }
    return out;
}

void LocalVersionedEngine::delete_trees_responsibly(
        const std::vector<IndexTypeKey>& orig_keys_to_delete,
        const arcticdb::MasterSnapshotMap& snapshot_map,
        const std::optional<SnapshotId>& snapshot_being_deleted,
        const PreDeleteChecks& check,
        const bool dry_run) {
    ARCTICDB_SAMPLE(DeleteTree, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: delete_tree");

    util::ContainerFilterWrapper keys_to_delete(orig_keys_to_delete);
    util::ContainerFilterWrapper not_to_delete(check.could_share_data);

    // Each section below performs these checks:
    // 1) remove any keys_to_delete that is still visible,
    // 2) find any other index key that could share data with the keys_to_delete

    // Check snapshot:
    if (check.snapshots) {
        keys_to_delete.remove_if([&snapshot_map, &snapshot_being_deleted, &not_to_delete](const auto& target_key) {
            // symbol -> IndexTypeKey -> set<SnapshotId>
            auto find_symbol = snapshot_map.find(target_key.id());
            if (find_symbol != snapshot_map.end()) {
                const auto& keys_map = find_symbol->second;
                auto snaps_itr = keys_map.find(target_key);
                if (snaps_itr != keys_map.end()) {
                    // Check 1)
                    const auto& snaps = snaps_itr->second;
                    auto count_for_snapshot_being_deleted = snapshot_being_deleted ?
                                                            snaps.count(snapshot_being_deleted.value()) : 0;
                    if (snaps.size() > count_for_snapshot_being_deleted) {
                        log::version().debug(
                                "Skipping the deletion of index {}:{} because it exists in another snapshot",
                                target_key.id(), target_key.version_id());
                        return true;
                    }
                }
                copy_versions_nearest_to_target(keys_map, target_key, not_to_delete); // Check 2)
            }
            return false;
        });
    }

    // Check versions:
    auto load_type = check.calc_load_type();
    if (load_type != LoadType::NOT_LOADED) {
        std::unordered_map<StreamId, std::shared_ptr<VersionMapEntry>> entry_map;
        {
            auto min_vers = min_versions_for_each_stream(orig_keys_to_delete);

            async::submit_tasks_for_range(min_vers,
                    [store=store(), version_map=version_map(), load_type](auto& sym_version) {
                        auto load_param = load_type == LoadType::LOAD_DOWNTO
                                ? LoadParameter{load_type, sym_version.second}
                                : LoadParameter{load_type};
                        return async::submit_io_task(CheckReloadTask{store, version_map, sym_version.first,
                                                                     load_param, true});
                    },
                    [&entry_map](auto& sym_version, auto&& entry) {
                        entry_map.emplace(std::move(sym_version.first), entry);
                    });
        }

        keys_to_delete.remove_if([&entry_map, &check, &not_to_delete](const auto& key) {
            const auto entry = entry_map.at(key.id());
            if (check.version_visible && !entry->is_tombstoned(key)) { // Check 1)
                log::version().debug("Skipping the deletion of index {}:{} because it exists in version map",
                        key.id(), key.version_id());
                return true;
            }
            get_matching_prev_and_next_versions(entry, key.version_id(), // Check 2)
                    [](ARCTICDB_UNUSED auto& matching) {},
                    [&check, &not_to_delete](auto& prev) { if (check.prev_version) not_to_delete.insert(prev);},
                    [&check, &not_to_delete](auto& next) { if (check.next_version) not_to_delete.insert(next);},
                    [v=key.version_id()](const AtomKeyImpl& key, const std::shared_ptr<VersionMapEntry>& entry) {
                        // Can't use is_indexish_and_not_tombstoned() because the target version's index key might have
                        // already been tombstoned, so will miss it and thus not able to find the prev/next key.
                        return is_index_key_type(key.type()) && (key.version_id() == v || !entry->is_tombstoned(key));
                    });
            return false;
        });
    }

    // Resolve:
    // Check 2) implementations does not consider that the key they are adding to not_to_delete might actually be in
    // keys_to_delete, so excluding those:
    for (const auto& key: *keys_to_delete) {
        not_to_delete.erase(key);
    }

    ReadKeyOpts read_opts;
    read_opts.ignores_missing_key_ = true;
    auto data_keys_to_be_deleted = get_data_keys_set(store(), *keys_to_delete, read_opts);
    log::version().debug("Candidate: {} total of data keys", data_keys_to_be_deleted.size());

    read_opts.dont_warn_about_missing_key = true;
    auto data_keys_not_to_be_deleted = get_data_keys_set(store(), *not_to_delete, read_opts);
    not_to_delete.clear();
    log::version().debug("Forbidden: {} total of data keys", data_keys_not_to_be_deleted.size());

    RemoveOpts remove_opts;
    remove_opts.ignores_missing_key_ = true;

    std::vector<entity::VariantKey> vks;
    if (!dry_run) {
        std::copy(keys_to_delete->begin(), keys_to_delete->end(), std::back_inserter(vks));
        store()->remove_keys(vks, remove_opts).get();
    }

    vks.clear();
    std::copy_if(std::make_move_iterator(data_keys_to_be_deleted.begin()),
                 std::make_move_iterator(data_keys_to_be_deleted.end()),
                 std::back_inserter(vks),
                 [&](const auto& k) {return !data_keys_not_to_be_deleted.count(k);});
    log::version().debug("Index keys deleted. Proceeding with {} number of data keys", vks.size());
    if (!dry_run) {
        store()->remove_keys(vks, remove_opts).get();
    }
}

void LocalVersionedEngine::remove_incomplete(
    const StreamId& stream_id
    ) {
    stream::remove_incomplete_segments(store_, stream_id);
}

std::set<StreamId> LocalVersionedEngine::get_incomplete_symbols() {
    return stream::get_incomplete_symbols(store_);
}

std::set<StreamId> LocalVersionedEngine::get_incomplete_refs() {
    return stream::get_incomplete_refs(store_);
}

std::set<StreamId> LocalVersionedEngine::get_active_incomplete_refs() {
    return stream::get_active_incomplete_refs(store_);
}

void LocalVersionedEngine::push_incompletes_to_symbol_list(const std::set<StreamId>& incompletes) {
    auto existing = list_streams_internal(std::nullopt, std::nullopt, std::nullopt, cfg().symbol_list(), false);
    for(const auto& incomplete : incompletes) {
        if(existing.find(incomplete) == existing.end())
            symbol_list().add_symbol(store_, incomplete);
    }
}

void LocalVersionedEngine::append_incomplete_frame(
    const StreamId& stream_id,
    InputTensorFrame&& frame) const {
    arcticdb::stream::append_incomplete(store_, stream_id, std::move(frame));
}

void LocalVersionedEngine::append_incomplete_segment(
    const StreamId& stream_id,
    SegmentInMemory &&seg) {
    arcticdb::stream::append_incomplete_segment(store_, stream_id, std::move(seg));
}

void LocalVersionedEngine::write_parallel_frame(
    const StreamId& stream_id,
    InputTensorFrame&& frame) const {
    stream::write_parallel(store_, stream_id, std::move(frame));
}

VersionedItem LocalVersionedEngine::compact_incomplete_dynamic(
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    bool append,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify) {
    log::version().info("Compacting incomplete symbol {}", stream_id);

    auto update_info = get_latest_undeleted_version_and_next_version_id(store(), version_map(), stream_id, true, false);
    auto versioned_item =  compact_incomplete_impl(
            store_, stream_id, user_meta, update_info,
            append, convert_int_to_float, via_iteration, sparsify);

    version_map_->write_version(store_, versioned_item.key_);

    if(cfg_.symbol_list())
        symbol_list().add_symbol(store_, stream_id);

    return versioned_item;
}

folly::Future<FrameAndDescriptor> async_read_direct(
    const std::shared_ptr<Store>& store,
    SegmentInMemory&& index_segment,
    const ReadQuery& read_query,
    std::shared_ptr<BufferHolder> buffers,
    const ReadOptions& read_options) {
    auto index_segment_reader = std::make_shared<index::IndexSegmentReader>(std::move(index_segment));
    auto pipeline_context = std::make_shared<PipelineContext>(StreamDescriptor{*index_segment_reader->mutable_tsd().mutable_stream_descriptor()});
    pipeline_context->set_selected_columns(read_query.columns);
    const bool dynamic_schema = opt_false(read_options.dynamic_schema_);
    const bool bucketize_dynamic = index_segment_reader->bucketize_dynamic();

    auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
        read_query,
        pipeline_context,
        dynamic_schema,
        bucketize_dynamic);

    pipeline_context->slice_and_keys_ = filter_index(*index_segment_reader, combine_filter_functions(queries));

    generate_filtered_field_descriptors(pipeline_context, read_query.columns);
    mark_index_slices(pipeline_context, dynamic_schema, bucketize_dynamic);
    auto frame = allocate_frame(pipeline_context);

    return fetch_data(frame, pipeline_context, store, dynamic_schema, buffers).then(
        [pipeline_context, frame, &read_options] (auto&&) mutable {
            reduce_and_fix_columns(pipeline_context, frame, read_options);
        }).then(
            [index_segment_reader, frame] (auto&&) {
                return FrameAndDescriptor{frame, std::move(index_segment_reader->mutable_tsd()), {}, {}};
            });
}

std::pair<std::vector<AtomKey>, std::vector<FrameAndDescriptor>> LocalVersionedEngine::batch_read_keys(
    const std::vector<AtomKey> &keys,
    const std::vector<ReadQuery> &read_queries,
    const ReadOptions& read_options) {

    std::vector<folly::Future<std::pair<entity::VariantKey, SegmentInMemory>>> index_futures;
    for (auto &index_key: keys) {
        index_futures.push_back(store()->read(index_key));
    }
    auto indexes = folly::collect(index_futures).get();

    std::vector<folly::Future<FrameAndDescriptor>> results_fut;
    auto i = 0u;
    util::check(read_queries.empty() || read_queries.size() == keys.size(), "Expected read queries to either be empty or equal to size of keys");
    for (auto&& [index_key, index_segment]: indexes) {
        results_fut.push_back(async_read_direct(store(), std::move(index_segment), read_queries.empty() ? ReadQuery{} : read_queries[i++], std::make_shared<BufferHolder>(), read_options));
    }
    Allocator::instance()->trim();
    return std::make_pair(keys, folly::collect(results_fut).get());
}

std::vector<std::pair<VersionedItem, FrameAndDescriptor>> LocalVersionedEngine::batch_read_internal(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries,
    std::vector<ReadQuery>& read_queries,
    const ReadOptions& read_options) {

    std::vector<folly::Future<std::pair<VersionedItem, FrameAndDescriptor>>> results_fut;
    for (size_t idx=0; idx < stream_ids.size(); idx++) {
        auto version_query = version_queries.size() > idx ? version_queries[idx] : VersionQuery{};
        auto read_query = read_queries.size() > idx ? read_queries[idx] : ReadQuery{};
        results_fut.push_back(read_dataframe_version_internal(stream_ids[idx],
                                                              version_query,
                                                              read_query,
                                                              read_options));
    }
    return folly::collect(results_fut).get();
}

std::vector<AtomKey> LocalVersionedEngine::batch_write_internal(
    std::vector<VersionId> version_ids,
    const std::vector<StreamId>& stream_ids,
    std::vector<InputTensorFrame> frames,
    std::vector<std::shared_ptr<DeDupMap>> de_dup_maps,
    bool validate_index
) {
    ARCTICDB_SAMPLE(WriteDataFrame, 0)
    ARCTICDB_DEBUG(log::version(), "Batch writing {} dataframes", stream_ids.size());
    std::vector<folly::Future<entity::AtomKey>> results_fut;
    for (size_t idx = 0; idx < stream_ids.size(); idx++) {
        results_fut.push_back(async_write_dataframe_impl(
            store(),
            version_ids[idx],
            std::move(frames[idx]),
            get_write_options(),
            de_dup_maps[idx],
            false,
            validate_index
        ));
    }
    return folly::collect(results_fut).get();
}



VersionedItem LocalVersionedEngine::append_internal(
    const StreamId& stream_id,
    InputTensorFrame&& frame,
    bool upsert,
    bool prune_previous_versions,
    bool validate_index) {

    auto update_info = get_latest_undeleted_version_and_next_version_id(store(),
                                                                        version_map(),
                                                                        stream_id,
                                                                        true,
                                                                        false);

    if(update_info.previous_index_key_.has_value()) {
        auto versioned_item = append_impl(store(),
                                          update_info,
                                          std::move(frame),
                                          get_write_options(),
                                          validate_index);
        if (prune_previous_versions) {
            auto pruned_indexes = version_map()->write_and_prune_previous(store(), versioned_item.key_, update_info.previous_index_key_);
            delete_unreferenced_pruned_indexes(pruned_indexes, versioned_item.key_);
        } else {
            version_map()->write_version(store(), versioned_item.key_);
        }
        return versioned_item;
    } else {
        if(upsert) {
            auto write_options = get_write_options();
            auto versioned_item =  write_dataframe_impl(store_,
                                                        update_info.next_version_id_,
                                                        std::move(frame),
                                                        write_options,
                                                        std::make_shared<DeDupMap>(),
                                                        false,
                                                        validate_index
                                                        );

            if(cfg_.symbol_list())
                symbol_list().add_symbol(store_, stream_id);

            version_map()->write_version(store(), versioned_item.key_);
            return versioned_item;
        } else {
            util::raise_rte( "Cannot append to non-existent symbol {}", stream_id);
        }
    }
}

std::vector<AtomKey> LocalVersionedEngine::batch_append_internal(
    std::vector<VersionId> version_ids,
    const std::vector<StreamId>& stream_ids,
    std::vector<AtomKey> prevs,
    std::vector<InputTensorFrame> frames,
    const WriteOptions& write_options,
    bool validate_index) {

    std::vector<folly::Future<AtomKey>> append_futures;
    for(auto id : folly::enumerate(stream_ids)) {
        UpdateInfo update_info{prevs[id.index], version_ids[id.index]};
        append_futures.emplace_back(async_append_impl(store(), update_info, std::move(frames[id.index]), write_options, validate_index));
    }

    return folly::collect(append_futures).get();
}

struct WarnVersionTypeNotHandled {
    bool warned = false;

    void warn(const StreamId& stream_id) {
        if (!warned) {
            log::version().warn("Only exact version numbers are supported when using batch read calls."
                                "The queries passed for '{}', etc. are ignored and the latest versions will be used!",
                                stream_id);
            warned = true;
        }
    }
};

std::map<StreamId, VersionId> get_sym_versions_from_query(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries) {
    std::map<StreamId, VersionId> sym_versions;
    WarnVersionTypeNotHandled warner;
    for(const auto& stream_id : folly::enumerate(stream_ids)) {
        const auto& query = version_queries[stream_id.index].content_;
        if(std::holds_alternative<SpecificVersionQuery>(query))
            sym_versions[*stream_id] = std::get<SpecificVersionQuery>(query).version_id_;
         else
            warner.warn(*stream_id);
    }
    return sym_versions;
}

std::map<StreamId, VersionVectorType> get_multiple_sym_versions_from_query(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries) {
    std::map<StreamId, VersionVectorType> sym_versions;
    WarnVersionTypeNotHandled warner;
    for(const auto& stream_id : folly::enumerate(stream_ids)) {
        const auto& query = version_queries[stream_id.index].content_;
        if(std::holds_alternative<SpecificVersionQuery>(query))
            sym_versions[*stream_id].push_back(std::get<SpecificVersionQuery>(query).version_id_);
        else
            warner.warn(*stream_id);
    }
    return sym_versions;
}

std::vector<std::pair<VersionedItem, arcticdb::proto::descriptors::TimeSeriesDescriptor>> LocalVersionedEngine::batch_restore_version_internal(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries) {
    util::check(stream_ids.size() == version_queries.size(), "Symbol vs version query size mismatch: {} != {}", stream_ids.size(), version_queries.size());
    auto sym_versions = get_sym_versions_from_query(stream_ids, version_queries);
    util::check(sym_versions.size() == version_queries.size(), "Restore versions requires specific version to be supplied");
    auto previous = batch_get_latest_version(store(), version_map(), stream_ids, false);
    auto versions_to_restore = batch_get_specific_version(store(), version_map(), sym_versions);
    std::vector<folly::Future<std::pair<VersionedItem, arcticdb::proto::descriptors::TimeSeriesDescriptor>>> fut_vec;

    for(const auto& stream_id : folly::enumerate(stream_ids)) {
        auto prev = previous->find(*stream_id);
        auto maybe_prev = prev == std::end(*previous) ? std::nullopt : std::make_optional<AtomKey>(to_atom(prev->second));

        auto version = versions_to_restore->find(*stream_id);
        util::check(version != std::end(*versions_to_restore), "Did not find version for symbol {}", *stream_id);
        fut_vec.emplace_back(async::submit_io_task(AsyncRestoreVersionTask{store(), version_map(), *stream_id, to_atom(version->second), maybe_prev}));
    }
    auto output = folly::collect(fut_vec).get();

    std::vector<folly::Future<folly::Unit>> symbol_write_futs;
    for(const auto& item : output) {
        symbol_write_futs.emplace_back(async::submit_io_task(WriteSymbolTask(store(), symbol_list_ptr(), item.first.key_.id())));
    }
    folly::collect(symbol_write_futs).wait();
    return output;
}

timestamp LocalVersionedEngine::get_update_time_internal(
        const StreamId& stream_id,
        const VersionQuery& version_query
        ) {
    auto version = get_version_to_read(stream_id, version_query);
    if(!version)
        throw NoDataFoundException(fmt::format("get_update_time: version not found for symbol", stream_id));
    return version->key_.creation_ts();
}

std::vector<timestamp> LocalVersionedEngine::batch_get_update_times(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries) {
    util::check(stream_ids.size() == version_queries.size(), "Symbol vs version query size mismatch: {} != {}", stream_ids.size(), version_queries.size());
    std::vector<timestamp> results;
    for(const auto& stream_id : folly::enumerate(stream_ids)) {
        const auto& query = version_queries[stream_id.index];
        results.emplace_back(get_update_time_internal(*stream_id, query));
    }
    return results;
}

SpecificAndLatestVersionKeys LocalVersionedEngine::get_stream_index_map(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries
    ) {
    std::shared_ptr<std::unordered_map<StreamId, AtomKey>> latest_versions;
    std::shared_ptr<std::unordered_map<std::pair<StreamId, VersionId>, AtomKey>> specific_versions;
    if(!version_queries.empty()) {
        auto sym_versions = get_multiple_sym_versions_from_query(stream_ids, version_queries);
        specific_versions = batch_get_specific_versions(store(), version_map(), sym_versions);
        std::vector<StreamId> latest_ids;
        std::copy_if(std::begin(stream_ids), std::end(stream_ids), std::back_inserter(latest_ids), [&sym_versions] (const auto& key) {
            return sym_versions.find(key) == std::end(sym_versions);
        });
        latest_versions = batch_get_latest_version(store(), version_map(), latest_ids, false);
    } else {
        specific_versions = std::make_shared<std::unordered_map<std::pair<StreamId, VersionId>, AtomKey>>();
        latest_versions = batch_get_latest_version(store(), version_map(), stream_ids, false);
    }

    return std::make_pair(specific_versions, latest_versions);
}

std::vector<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>> LocalVersionedEngine::batch_read_metadata_internal(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries
    ) {
    auto [specific_versions_index_map, latest_versions_index_map] = get_stream_index_map(stream_ids, version_queries);
    auto version_queries_is_empty = version_queries.empty();
    std::vector<folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>>> fut_vec;
    for(const auto& stream_id : folly::enumerate(stream_ids)) {
        if(!version_queries_is_empty && std::holds_alternative<SpecificVersionQuery>(version_queries[stream_id.index].content_)){
            const auto& query = version_queries[stream_id.index].content_;
            auto specific_version_map_key = std::make_pair(*stream_id, std::get<SpecificVersionQuery>(query).version_id_);
            auto specific_version_it = specific_versions_index_map->find(specific_version_map_key);
            if (specific_version_it != specific_versions_index_map->end()){
                fut_vec.push_back(store()->read_metadata(specific_version_it->second));
            }else{
                fut_vec.push_back(std::make_pair(std::nullopt, std::nullopt));
            }
        }else{
            auto last_version_it = latest_versions_index_map->find(*stream_id);
            if(last_version_it != latest_versions_index_map->end()) {
                fut_vec.push_back(store()->read_metadata(last_version_it->second));
            }else{
                fut_vec.push_back(std::make_pair(std::nullopt, std::nullopt));   
            }
        }
    }
    return folly::collect(fut_vec).get();
}

VersionedItem LocalVersionedEngine::sort_merge_internal(
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    bool append,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify
    ) {
    auto update_info = get_latest_undeleted_version_and_next_version_id(store(), version_map(), stream_id, true, false);
    auto versioned_item = sort_merge_impl(store_, stream_id, user_meta, update_info, append, convert_int_to_float, via_iteration, sparsify);
    version_map()->write_version(store(), versioned_item.key_);
    return versioned_item;
}

bool LocalVersionedEngine::has_stream(const StreamId & stream_id, const std::optional<bool>& skip_compat, const std::optional<bool>& iterate_on_failure){

    auto opt = get_latest_undeleted_version(store(), version_map(),  stream_id, opt_true(skip_compat), opt_false(iterate_on_failure));
    return opt.has_value();
}

StorageLockWrapper LocalVersionedEngine::get_storage_lock(const StreamId& stream_id) {
    return StorageLockWrapper{stream_id, store_};
}

void LocalVersionedEngine::delete_storage() {
    delete_all(store_, true);
}

void LocalVersionedEngine::configure(const storage::LibraryDescriptor::VariantStoreConfig & cfg){
    util::variant_match(cfg,
                        [](std::monostate){ /* Unknown config */},
                        [&cfg=cfg_, version_map=version_map(), store=store()](const arcticdb::proto::storage::VersionStoreConfig & conf){
        cfg.CopyFrom(conf);
        if(cfg.has_failure_sim()) {
            store->set_failure_sim(cfg.failure_sim());
        }
        if(cfg.write_options().descriptor()->FindFieldByLowercaseName("fast_tombstone_all")) {
          version_map->set_fast_tombstone_all(cfg.write_options().fast_tombstone_all());
        }
        if(cfg.write_options().has_sync_passive()) {
            version_map->set_log_changes(cfg.write_options().sync_passive().enabled());
        }
        if (cfg.has_prometheus_config()) {
            PrometheusConfigInstance::instance()->config.CopyFrom(cfg.prometheus_config());
            ARCTICDB_DEBUG(log::version(), "prometheus configured");
        }
        },
        [](const auto& conf){
        util::raise_rte(
            "Configuration of LocalVersionedEngine must use a VersionStoreConfig, actual {}",
            [&conf]{ util::format(conf); });
    }
    );
}

timestamp LocalVersionedEngine::latest_timestamp(const std::string& symbol) {
    if(auto latest_incomplete = stream::latest_incomplete_timestamp(store(), symbol); latest_incomplete)
        return latest_incomplete.value();

    if(auto latest_key = get_latest_version(symbol, VersionQuery{}); latest_key)
        return latest_key.value().key_.end_time();

    return -1;
}

std::unordered_map<KeyType, std::pair<size_t, size_t>> LocalVersionedEngine::scan_object_sizes() {
    std::unordered_map<KeyType, std::pair<size_t, size_t>> sizes;
    foreach_key_type([&store=store(), &sizes=sizes](KeyType key_type) {
        std::vector<VariantKey> keys;
        store->iterate_type(key_type, [&keys](const VariantKey &&k) {
            keys.emplace_back(std::forward<const VariantKey>(k));
        });
        auto& pair = sizes[key_type];
        pair.first = keys.size();
        std::vector<stream::StreamSource::ReadContinuation> continuations;
        continuations.reserve(keys.size());
        std::atomic<size_t> key_size{0};
        for(auto i = 0u; i < keys.size(); ++i) {
            continuations.emplace_back([&key_size] (auto&& ks) {
                auto key_seg = std::move(ks);
                key_size += key_seg.segment().total_segment_size();
                return key_seg.variant_key();
            });
        }
        store->batch_read_compressed(std::move(keys), std::move(continuations), BatchReadArgs{});
        pair.second = key_size;
    });
    return sizes;
}

void LocalVersionedEngine::move_storage(KeyType key_type, timestamp horizon, size_t storage_index) {
    store_->move_storage(key_type, horizon, storage_index);
}

void LocalVersionedEngine::force_release_lock(const StreamId& name) {
    StorageLock<>::force_release_lock(name, store());
}

WriteOptions LocalVersionedEngine::get_write_options() const  {
    return  WriteOptions::from_proto(cfg().write_options());
}

AtomKey LocalVersionedEngine::_test_write_segment(const std::string& symbol) {
    auto wrapper = SinkWrapper(symbol, {
        scalar_field_proto(DataType::UINT64, "thing1"),
        scalar_field_proto(DataType::UINT64, "thing2"),
        scalar_field_proto(DataType::UINT64, "thing3"),
        scalar_field_proto(DataType::UINT64, "thing4")
    });

    for(size_t j = 0; j < 20; ++j ) {
        wrapper.aggregator_.start_row(timestamp(j))([&](auto& rb) {
            rb.set_scalar(1, j);
            rb.set_scalar(2, j + 1);
            rb.set_scalar(3, j + j);
            rb.set_scalar(4, j * j);
        });
    }

    wrapper.aggregator_.commit();
    return to_atom(store()->write(KeyType::TABLE_DATA, VersionId{}, StreamId{symbol}, 0, 0, std::move(wrapper.segment())).get());
}

std::shared_ptr<VersionMap> LocalVersionedEngine::_test_get_version_map() {
    return version_map();
}

void LocalVersionedEngine::_test_set_store(std::shared_ptr<Store> store) {
    set_store(std::move(store));
}
} // arcticdb::version_store
