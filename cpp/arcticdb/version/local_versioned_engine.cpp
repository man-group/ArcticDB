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
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/storage_lock.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/version/version_tasks.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/util/container_filter_wrapper.hpp>
#include <arcticdb/python/gil_lock.hpp>

namespace arcticdb::version_store {
template<class ClockType>
LocalVersionedEngine::LocalVersionedEngine(
        const std::shared_ptr<storage::Library>& library,
        const ClockType&) :
    store_(std::make_shared<async::AsyncStore<ClockType>>(library, codec::default_lz4_codec(), encoding_version(library->config()))),
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

template LocalVersionedEngine::LocalVersionedEngine(const std::shared_ptr<storage::Library>& library, const util::SysClock&);
template LocalVersionedEngine::LocalVersionedEngine(const std::shared_ptr<storage::Library>& library, const util::ManualClock&);

folly::Future<folly::Unit> LocalVersionedEngine::delete_unreferenced_pruned_indexes(
        const std::vector<AtomKey> &pruned_indexes,
        const AtomKey& key_to_keep
) {
    try {
        if (!pruned_indexes.empty() && !cfg().write_options().delayed_deletes()) {
            // TODO: the following function will load all snapshots, which will be horrifyingly inefficient when called
            // multiple times from batch_*
            auto [not_in_snaps, in_snaps] = get_index_keys_partitioned_by_inclusion_in_snapshots(
                    store(),
                    pruned_indexes.begin()->id(),
                    pruned_indexes);
            in_snaps.insert(key_to_keep);
            PreDeleteChecks checks{false, false, false, false, std::move(in_snaps)};
            return delete_trees_responsibly(not_in_snaps, {}, {}, checks)
                    .thenError(folly::tag_t<std::exception>{}, [](auto const& ex) {
                        log::version().warn("Failed to clean up pruned previous versions due to: {}", ex.what());
                    });
        }
    } catch (const std::exception &ex) {
        // Best-effort so deliberately swallow
        log::version().warn("Failed to clean up pruned previous versions due to: {}", ex.what());
    }
    return folly::Unit();
}

void LocalVersionedEngine::create_column_stats_internal(
    const VersionedItem& versioned_item,
    ColumnStats& column_stats,
    const ReadOptions& read_options) {
    ARCTICDB_RUNTIME_SAMPLE(CreateColumnStatsInternal, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: create_column_stats");
    create_column_stats_impl(store(),
                             versioned_item,
                             column_stats,
                             read_options);
}

void LocalVersionedEngine::create_column_stats_version_internal(
    const StreamId& stream_id,
    ColumnStats& column_stats,
    const VersionQuery& version_query,
    const ReadOptions& read_options) {
    auto versioned_item = get_version_to_read(stream_id, version_query, read_options);
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
            versioned_item.has_value(),
            "create_column_stats_version_internal: version not found for stream '{}'",
            stream_id
            );
    create_column_stats_internal(versioned_item.value(),
                                 column_stats,
                                 read_options);
}

void LocalVersionedEngine::drop_column_stats_internal(
    const VersionedItem& versioned_item,
    const std::optional<ColumnStats>& column_stats_to_drop) {
    ARCTICDB_RUNTIME_SAMPLE(DropColumnStatsInternal, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: drop_column_stats");
    drop_column_stats_impl(store(),
                           versioned_item,
                           column_stats_to_drop);
}

void LocalVersionedEngine::drop_column_stats_version_internal(
    const StreamId& stream_id,
    const std::optional<ColumnStats>& column_stats_to_drop,
    const VersionQuery& version_query) {
    auto versioned_item = get_version_to_read(stream_id, version_query, ReadOptions{});
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
            versioned_item.has_value(),
            "drop_column_stats_version_internal: version not found for stream '{}'",
            stream_id
            );
    drop_column_stats_internal(versioned_item.value(), column_stats_to_drop);
}

FrameAndDescriptor LocalVersionedEngine::read_column_stats_internal(
    const VersionedItem& versioned_item) {
    return read_column_stats_impl(store(), versioned_item);
}

ReadVersionOutput LocalVersionedEngine::read_column_stats_version_internal(
    const StreamId& stream_id,
    const VersionQuery& version_query) {
    auto versioned_item = get_version_to_read(stream_id, version_query, ReadOptions{});
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
            versioned_item.has_value(),
            "read_column_stats_version_internal: version not found for stream '{}'",
            stream_id
            );
    auto frame_and_descriptor = read_column_stats_internal(versioned_item.value());
    return ReadVersionOutput{std::move(versioned_item.value()), std::move(frame_and_descriptor)};
}

ColumnStats LocalVersionedEngine::get_column_stats_info_internal(
    const VersionedItem& versioned_item) {
    return get_column_stats_info_impl(store(), versioned_item);
}

ColumnStats LocalVersionedEngine::get_column_stats_info_version_internal(
    const StreamId& stream_id,
    const VersionQuery& version_query) {
    auto versioned_item = get_version_to_read(stream_id, version_query, ReadOptions{});
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
            versioned_item.has_value(),
            "get_column_stats_info_version_internal: version not found for stream '{}'",
            stream_id
            );
    return get_column_stats_info_internal(versioned_item.value());
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
    const VersionQuery& version_query,
    const ReadOptions& read_options) {
    auto key = get_latest_undeleted_version(store(), version_map(), stream_id,  version_query, read_options);
    if (!key) {
        ARCTICDB_DEBUG(log::version(), "get_latest_version didn't find version for stream_id: {}", stream_id);
        return std::nullopt;
    }
    return VersionedItem{std::move(key.value())};
}

std::optional<VersionedItem> LocalVersionedEngine::get_specific_version(
    const StreamId &stream_id,
    SignedVersionId signed_version_id,
    const VersionQuery& version_query,
    const ReadOptions& read_options) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: get_specific_version");
    auto key = ::arcticdb::get_specific_version(store(), version_map(), stream_id, signed_version_id, version_query, read_options);
    if (!key) {
        VersionId version_id;
        if (signed_version_id >= 0) {
            version_id = static_cast<VersionId>(signed_version_id);
        } else {
            auto opt_latest_key = ::arcticdb::get_latest_version(store(), version_map(), stream_id, version_query, read_options);
            if (opt_latest_key.has_value()) {
                auto opt_version_id = get_version_id_negative_index(opt_latest_key->version_id(), signed_version_id);
                if (opt_version_id.has_value()) {
                    version_id = *opt_version_id;
                }  else {
                    return std::nullopt;
                }
            } else {
                return std::nullopt;
            }
        }
        ARCTICDB_DEBUG(log::version(), "Version {} for symbol {} is missing, checking snapshots:", version_id,
                       stream_id);
        auto index_keys = get_index_keys_in_snapshots(store(), stream_id);
        auto index_key = std::find_if(index_keys.begin(), index_keys.end(), [version_id](const AtomKey &k) {
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
    const VersionQuery& version_query,
    const ReadOptions& read_options
    ) {

    auto index_key = load_index_key_from_time(store(), version_map(), stream_id, as_of, version_query, read_options);
    if (!index_key) {
        auto index_keys = get_index_keys_in_snapshots(store(), stream_id);
        auto vector_index_keys = std::vector<AtomKey>(index_keys.begin(), index_keys.end());
        std::sort(std::begin(vector_index_keys), std::end(vector_index_keys),
                  [](auto& k1, auto& k2) {return k1.creation_ts() > k2.creation_ts();});
        index_key = get_index_key_from_time(as_of, vector_index_keys);
    }

    if (!index_key) {
        log::version().warn("read_dataframe_timestamp: version id not found for stream {} timestamp {}", stream_id, as_of);
        return std::nullopt;
    }

    return VersionedItem(std::move(index_key.value()));
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
    const VersionQuery &version_query,
    const ReadOptions& read_options
    ) {
    return util::variant_match(version_query.content_,
       [&stream_id, &version_query, &read_options, this](const SpecificVersionQuery &specific) {
            return get_specific_version(stream_id, specific.version_id_, version_query, read_options);
        },
        [&stream_id, this](const SnapshotVersionQuery &snapshot) {
            return get_version_from_snapshot(stream_id, snapshot.name_);
        },
        [&stream_id, &version_query, &read_options, this](const TimestampVersionQuery &timestamp) {
            return get_version_at_time(stream_id, timestamp.timestamp_, version_query, read_options);
        },
        [&stream_id, &version_query, &read_options, this](const std::monostate &) {
            return get_latest_version(stream_id, version_query, read_options);
    }
    );
}

IndexRange LocalVersionedEngine::get_index_range(
    const StreamId &stream_id,
    const VersionQuery& version_query) {
    auto version = get_version_to_read(stream_id, version_query, ReadOptions{});
    if(!version)
        return unspecified_range();

    return index::get_index_segment_range(version.value().key_, store());
}

ReadVersionOutput LocalVersionedEngine::read_dataframe_version_internal(
    const StreamId &stream_id,
    const VersionQuery& version_query,
    ReadQuery& read_query,
    const ReadOptions& read_options) {
    auto version = get_version_to_read(stream_id, version_query, ReadOptions{});
    std::variant<VersionedItem, StreamId> identifier;
    if(!version) {
        if(opt_false(read_options.incompletes_)) {
            log::version().warn("No index:  Key not found for {}, will attempt to use incomplete segments.", stream_id);
            identifier = stream_id;
        } else {
            missing_data::raise<ErrorCode::E_NO_SUCH_VERSION>(
                    "read_dataframe_version: version matching query '{}' not found for symbol '{}'", version_query, stream_id);
        }
    }
    else  {
        identifier = version.value();
    }

    auto frame_and_descriptor = read_dataframe_internal(identifier, read_query, read_options);
    return ReadVersionOutput{version.value_or(VersionedItem{}), std::move(frame_and_descriptor)};
}

folly::Future<DescriptorItem> LocalVersionedEngine::get_descriptor(
    AtomKey&& key){
    return store()->read(key)
    .thenValue([](auto&& key_seg_pair) -> DescriptorItem {
        auto key_seg = std::move(std::get<0>(key_seg_pair));
        auto seg = std::move(std::get<1>(key_seg_pair));
        auto timeseries_descriptor = seg.has_metadata() ? std::make_optional<google::protobuf::Any>(*seg.metadata()) : std::nullopt;
        auto start_index = seg.column(position_t(index::Fields::start_index)).type().visit_tag([&](auto column_desc_tag) -> std::optional<NumericIndex> {
            using ColumnDescriptorType = std::decay_t<decltype(column_desc_tag)>;
            using ColumnTagType =  typename ColumnDescriptorType::DataTypeTag;
            if (seg.row_count() == 0) {
                return std::nullopt;
            } else if constexpr (is_numeric_type(ColumnTagType::data_type)) {
                std::optional<NumericIndex> start_index;
                auto column_data = seg.column(position_t(index::Fields::start_index)).data();
                while (auto block = column_data.template next<ColumnDescriptorType>()) {
                    auto ptr = reinterpret_cast<const NumericIndex *>(block.value().data());
                    const auto row_count = block.value().row_count();
                    for (auto i = 0u; i < row_count; ++i) {
                        auto value = *ptr++;
                        start_index = start_index.has_value() ? std::min(start_index.value(), value) : value; 
                    }
                }
                return start_index;
            } else {
                util::raise_rte("Unsupported index type {}", seg.column(position_t(index::Fields::start_index)).type());
            }
        });

        auto end_index = seg.column(position_t(index::Fields::end_index)).type().visit_tag([&](auto column_desc_tag) -> std::optional<NumericIndex> {
            using ColumnDescriptorType = std::decay_t<decltype(column_desc_tag)>;
            using ColumnTagType =  typename ColumnDescriptorType::DataTypeTag;
            if (seg.row_count() == 0) {
                return std::nullopt;
            } else if constexpr (is_numeric_type(ColumnTagType::data_type)) {
                std::optional<NumericIndex> end_index;
                auto column_data = seg.column(position_t(index::Fields::end_index)).data();
                while (auto block = column_data.template next<ColumnDescriptorType>()) {
                    auto ptr = reinterpret_cast<const NumericIndex *>(block.value().data());
                    const auto row_count = block.value().row_count();
                    for (auto i = 0u; i < row_count; ++i) {
                        auto value = *ptr++;
                        end_index = end_index.has_value() ? std::max(end_index.value(), value) : value; 
                    }
                }
                return end_index;
            } else {
                util::raise_rte("Unsupported index type {}", seg.column(position_t(index::Fields::end_index)).type());
            }
        });
        return DescriptorItem{std::move(to_atom(key_seg)), std::move(start_index), std::move(end_index), std::move(timeseries_descriptor)};
    });
}

folly::Future<DescriptorItem> LocalVersionedEngine::get_descriptor_async(
    folly::Future<std::optional<AtomKey>>&& version_fut,
    const StreamId& stream_id,
    const VersionQuery& version_query){
    return  std::move(version_fut)
    .thenValue([this, &stream_id, &version_query](std::optional<AtomKey>&& key){
        missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(key.has_value(),
        "Unable to retrieve descriptor data. {}@{}: version not found", stream_id, version_query);
        return get_descriptor(std::move(key.value()));
    }).via(&async::cpu_executor());
}

DescriptorItem LocalVersionedEngine::read_descriptor_internal(
    const StreamId& stream_id,
    const VersionQuery& version_query,
    const ReadOptions& read_options
    ) {
    ARCTICDB_SAMPLE(ReadDescriptor, 0)
    auto version = get_version_to_read(stream_id, version_query, read_options);
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(version.has_value(),
        "Unable to retrieve descriptor data. {}@{}: version not found", stream_id, version_query);
    return get_descriptor(std::move(version->key_)).get();
}


std::vector<std::variant<DescriptorItem, DataError>> LocalVersionedEngine::batch_read_descriptor_internal(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries,
    const ReadOptions& read_options) {

    internal::check<ErrorCode::E_ASSERTION_FAILURE>(read_options.batch_throw_on_error_.has_value(),
                                                    "ReadOptions::batch_throw_on_error_ should always be set here");

    auto version_futures = batch_get_versions_async(store(), version_map(), stream_ids, version_queries, read_options.read_previous_on_failure_);
    std::vector<folly::Future<DescriptorItem>> descriptor_futures;
    for (auto&& [idx, version_fut]: folly::enumerate(version_futures)) {
        descriptor_futures.push_back(
            get_descriptor_async(std::move(version_fut), stream_ids[idx], version_queries[idx]));
    }
    auto descriptors = folly::collectAll(descriptor_futures).get();
    std::vector<std::variant<DescriptorItem, DataError>> descriptors_or_errors;
    descriptors_or_errors.reserve(descriptors.size());
    for (auto&& [idx, descriptor]: folly::enumerate(descriptors)) {
        if (descriptor.hasValue()) {
            descriptors_or_errors.emplace_back(std::move(descriptor.value()));
        } else {
            if (*read_options.batch_throw_on_error_) {
                descriptor.throwUnlessValue();
            } else {
                auto exception = descriptor.exception();
                DataError data_error(stream_ids[idx], exception.what().toStdString(), version_queries[idx].content_);
                if (exception.is_compatible_with<NoSuchVersionException>()) {
                    data_error.set_error_code(ErrorCode::E_NO_SUCH_VERSION);
                } else if (exception.is_compatible_with<storage::KeyNotFoundException>()) {
                    data_error.set_error_code(ErrorCode::E_KEY_NOT_FOUND);
                }
                descriptors_or_errors.emplace_back(std::move(data_error));
            }
        }
    }
    return descriptors_or_errors;
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
        auto maybe_undeleted_prev = get_latest_undeleted_version(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
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
    auto maybe_prev = get_latest_undeleted_version(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
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

    auto index = index_type_from_descriptor(index_segment_reader.tsd().as_stream_descriptor());
    bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    auto time_series = make_timeseries_descriptor(
        total_rows,
        StreamDescriptor{index_segment_reader.mutable_tsd().as_stream_descriptor()},
        std::move(*index_segment_reader.mutable_tsd().mutable_proto().mutable_normalization()),
        std::move(*index_segment_reader.mutable_tsd().mutable_proto().mutable_user_meta()),
        std::nullopt,
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
    auto maybe_prev = get_latest_undeleted_version(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
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
                                                                        VersionQuery{},
                                                                        ReadOptions{});
    if (update_info.previous_index_key_.has_value()) {
        auto versioned_item = update_impl(store(),
                                          update_info,
                                          query,
                                          std::move(frame),
                                          get_write_options(),
                                          dynamic_schema);
        write_version_and_prune_previous_if_needed(
            prune_previous_versions, versioned_item.key_, update_info.previous_index_key_);
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
                                                                        VersionQuery{},
                                                                        ReadOptions{});
    if(update_info.previous_index_key_.has_value()) {
        ARCTICDB_DEBUG(log::version(), "write_versioned_dataframe for stream_id: {}", stream_id);
        auto index_key = UpdateMetadataTask{store(),
                                            update_info,
                                            std::move(user_meta)}();

        write_version_and_prune_previous_if_needed(prune_previous_versions, index_key, update_info.previous_index_key_);
        return VersionedItem{ std::move(index_key) };
    } else {
        auto frame = convert::py_none_to_frame();
        frame.desc.set_id(stream_id);
        frame.user_meta = std::move(user_meta);
        auto versioned_item = write_versioned_dataframe_internal(stream_id, std::move(frame), prune_previous_versions, false, false);
        if(cfg_.symbol_list())
            symbol_list().add_symbol(store_, stream_id);
        return versioned_item;
    }
}

std::vector<std::variant<VersionedItem, DataError>> LocalVersionedEngine::batch_write_versioned_metadata_internal(
    const std::vector<StreamId>& stream_ids,
    bool prune_previous_versions,
    bool throw_on_error,
    std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>&& user_meta_protos) {
    auto stream_update_info_futures = batch_get_latest_undeleted_version_and_next_version_id_async(store(),
                                                                                                   version_map(),
                                                                                                   stream_ids);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(stream_ids.size() == stream_update_info_futures.size(), "stream_ids and stream_update_info_futures must be of the same size");
    std::vector<folly::Future<VersionedItem>> write_metadata_versions_futs;
    for (const auto&& [idx, stream_update_info_fut] : folly::enumerate(stream_update_info_futures)) {
        write_metadata_versions_futs.push_back(
            std::move(stream_update_info_fut)
            .thenValue([this, prune_previous_versions, user_meta_proto = std::move(user_meta_protos[idx]), &stream_id = stream_ids[idx]](auto&& update_info) mutable -> folly::Future<IndexKeyAndUpdateInfo> {
                auto index_key_fut = folly::Future<AtomKey>::makeEmpty();
                if (update_info.previous_index_key_.has_value()) {
                    index_key_fut = async::submit_io_task(UpdateMetadataTask{store(), update_info, std::move(user_meta_proto)});
                } else {
                    auto frame = convert::py_none_to_frame();
                    frame.desc.set_id(stream_id);
                    frame.user_meta = std::move(user_meta_proto);
                    auto version_id = 0;
                    auto write_options = get_write_options();
                    auto de_dup_map = std::make_shared<DeDupMap>();
                    index_key_fut = async_write_dataframe_impl(store(), version_id, std::move(frame), write_options, de_dup_map, false, false);
                }
                return std::move(index_key_fut)
                .thenValue([update_info = std::move(update_info)](auto&& index_key) mutable -> IndexKeyAndUpdateInfo {
                    return IndexKeyAndUpdateInfo{std::move(index_key), std::move(update_info)};
                });
            })
            .thenValue([this, prune_previous_versions](auto&& index_key_and_update_info){
                auto&& [index_key, update_info] = index_key_and_update_info;
                return write_index_key_to_version_map_async(version_map(), std::move(index_key), std::move(update_info), prune_previous_versions, !update_info.previous_index_key_.has_value());
            }));
    }

    auto write_metadata_versions = folly::collectAll(write_metadata_versions_futs).get();
    std::vector<std::variant<VersionedItem, DataError>> write_metadata_versions_or_errors;
    write_metadata_versions_or_errors.reserve(write_metadata_versions.size());
    for (auto&& [idx, write_metadata_version]: folly::enumerate(write_metadata_versions)) {
        if (write_metadata_version.hasValue()) {
            write_metadata_versions_or_errors.emplace_back(std::move(write_metadata_version.value()));
        } else {
            if (throw_on_error) {
                write_metadata_version.throwUnlessValue();
            } else {
                auto exception = write_metadata_version.exception();
                DataError data_error(stream_ids[idx], exception.what().toStdString());
                if (exception.is_compatible_with<storage::KeyNotFoundException>()) {
                    data_error.set_error_code(ErrorCode::E_KEY_NOT_FOUND);
                }
                write_metadata_versions_or_errors.emplace_back(std::move(data_error));
            }
        }
    }
    return write_metadata_versions_or_errors;
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
    auto maybe_prev = ::arcticdb::get_latest_version(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
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

    write_version_and_prune_previous_if_needed(prune_previous_versions, versioned_item.key_, maybe_prev);
    return versioned_item;
}

std::pair<VersionedItem, TimeseriesDescriptor> LocalVersionedEngine::restore_version(
    const StreamId& stream_id,
    const VersionQuery& version_query
    ) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: restore_version");
    auto version_to_restore = get_version_to_read(stream_id, version_query, ReadOptions{});
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(static_cast<bool>(version_to_restore),
                                                 "Unable to restore {}@{}: version not found", stream_id, version_query);
    auto maybe_prev = ::arcticdb::get_latest_version(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
    ARCTICDB_DEBUG(log::version(), "restore for stream_id: {} , version_id = {}", stream_id, version_to_restore->key_.version_id());
    return AsyncRestoreVersionTask{store(), version_map(), stream_id, version_to_restore->key_, maybe_prev}().get();
}

VersionedItem LocalVersionedEngine::write_individual_segment(
    const StreamId& stream_id,
    SegmentInMemory&& segment,
    bool prune_previous_versions
    ) {
    ARCTICDB_SAMPLE(WriteVersionedDataFrame, 0)

    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write_versioned_dataframe");
    auto maybe_prev = ::arcticdb::get_latest_version(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
    auto version_id = get_next_version_from_key(maybe_prev);
    ARCTICDB_DEBUG(log::version(), "write_versioned_dataframe for stream_id: {} , version_id = {}", stream_id, version_id);
    auto index = index_type_from_descriptor(segment.descriptor());
    auto range = get_range_from_segment(index, segment);

    stream::StreamSink::PartialKey pk{
        KeyType::TABLE_DATA, version_id, stream_id, range.start_, range.end_
    };

    auto frame_slice = FrameSlice{segment};
    auto descriptor = make_timeseries_descriptor(segment.row_count(),segment.descriptor().clone(), {}, std::nullopt,std::nullopt, std::nullopt, false);
    auto key = store_->write(pk, std::move(segment)).get();
    std::vector sk{SliceAndKey{frame_slice, to_atom(key)}};
    auto index_key_fut = index::write_index(index, std::move(descriptor), std::move(sk), IndexPartialKey{stream_id, version_id}, store_);
    auto versioned_item = VersionedItem{to_atom(std::move(index_key_fut).get())};

    write_version_and_prune_previous_if_needed(prune_previous_versions, versioned_item.key_, maybe_prev);
    return versioned_item;
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

folly::Future<folly::Unit> LocalVersionedEngine::delete_trees_responsibly(
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
            auto min_versions = min_versions_for_each_stream(orig_keys_to_delete);
            for (const auto& min : min_versions) {
                auto load_param = load_type == LoadType::LOAD_DOWNTO
                        ? LoadParameter{load_type, static_cast<SignedVersionId>(min.second)}
                        : LoadParameter{load_type};
                const auto entry = version_map()->check_reload(store(), min.first, load_param, __FUNCTION__);
                entry_map.emplace(std::move(min.first), entry);
            }
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
                        // Can't use is_live_index_type_key() because the target version's index key might have
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

    storage::ReadKeyOpts read_opts;
    read_opts.ignores_missing_key_ = true;
    auto data_keys_to_be_deleted = get_data_keys_set(store(), *keys_to_delete, read_opts);
    log::version().debug("Candidate: {} total of data keys", data_keys_to_be_deleted.size());

    read_opts.dont_warn_about_missing_key = true;
    auto data_keys_not_to_be_deleted = get_data_keys_set(store(), *not_to_delete, read_opts);
    not_to_delete.clear();
    log::version().debug("Forbidden: {} total of data keys", data_keys_not_to_be_deleted.size());
    storage::RemoveOpts remove_opts;
    remove_opts.ignores_missing_key_ = true;

    std::vector<entity::VariantKey> vks_column_stats;
    std::transform(keys_to_delete->begin(),
                    keys_to_delete->end(),
                    std::back_inserter(vks_column_stats),
                    [](const IndexTypeKey& index_key) {
        return index_key_to_column_stats_key(index_key);
    });
    log::version().debug("Number of Column Stats keys to be deleted: {}", vks_column_stats.size());

    std::vector<entity::VariantKey> vks_to_delete;
    std::copy(std::make_move_iterator(keys_to_delete->begin()),
                std::make_move_iterator(keys_to_delete->end()),
                std::back_inserter(vks_to_delete));
    log::version().debug("Number of Index keys to be deleted: {}", vks_to_delete.size());

    std::vector<entity::VariantKey> vks_data_to_delete;
    std::copy_if(std::make_move_iterator(data_keys_to_be_deleted.begin()),
                std::make_move_iterator(data_keys_to_be_deleted.end()),
                std::back_inserter(vks_data_to_delete),
                [&](const auto& k) {return !data_keys_not_to_be_deleted.count(k);});
    log::version().debug("Number of Data keys to be deleted: {}", vks_data_to_delete.size());

    folly::Future<folly::Unit> remove_keys_fut;
    if (!dry_run) {
        // Delete any associated column stats keys first
        remove_keys_fut = store()->remove_keys(std::move(vks_column_stats), remove_opts)
        .thenValue([this, vks_to_delete = std::move(vks_to_delete), remove_opts](auto&& ) mutable {
            log::version().debug("Column Stats keys deleted.");
            return store()->remove_keys(std::move(vks_to_delete), remove_opts);
        })
        .thenValue([this, vks_data_to_delete = std::move(vks_data_to_delete), remove_opts](auto&&) mutable {
            log::version().debug("Index keys deleted.");
            return store()->remove_keys(std::move(vks_data_to_delete), remove_opts);
        })
        .thenValue([](auto&&){
            log::version().debug("Data keys deleted.");
            return folly::Unit();
        });
    }
    return remove_keys_fut;
}


void LocalVersionedEngine::remove_incomplete(
    const StreamId& stream_id
    ) {
    remove_incomplete_segments(store_, stream_id);
}

std::set<StreamId> LocalVersionedEngine::get_incomplete_symbols() {
    return ::arcticdb::get_incomplete_symbols(store_);
}

std::set<StreamId> LocalVersionedEngine::get_incomplete_refs() {
    return ::arcticdb::get_incomplete_refs(store_);
}

std::set<StreamId> LocalVersionedEngine::get_active_incomplete_refs() {
    return ::arcticdb::get_active_incomplete_refs(store_);
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
    arcticdb::append_incomplete(store_, stream_id, std::move(frame));
}

void LocalVersionedEngine::append_incomplete_segment(
    const StreamId& stream_id,
    SegmentInMemory &&seg) {
    arcticdb::append_incomplete_segment(store_, stream_id, std::move(seg));
}

void LocalVersionedEngine::write_parallel_frame(
    const StreamId& stream_id,
    InputTensorFrame&& frame) const {
    write_parallel(store_, stream_id, std::move(frame));
}

VersionedItem LocalVersionedEngine::compact_incomplete_dynamic(
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    bool append,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify,
    bool prune_previous_versions) {
    log::version().info("Compacting incomplete symbol {}", stream_id);

    auto update_info = get_latest_undeleted_version_and_next_version_id(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
    auto versioned_item =  compact_incomplete_impl(
            store_, stream_id, user_meta, update_info,
            append, convert_int_to_float, via_iteration, sparsify, get_write_options());

    write_version_and_prune_previous_if_needed(
        prune_previous_versions, versioned_item.key_, update_info.previous_index_key_);

    if(cfg_.symbol_list())
        symbol_list().add_symbol(store_, stream_id);

    return versioned_item;
}

bool LocalVersionedEngine::is_symbol_fragmented(const StreamId& stream_id, std::optional<size_t> segment_size) {
    auto update_info = get_latest_undeleted_version_and_next_version_id(
            store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
    auto pre_defragmentation_info = get_pre_defragmentation_info(
        store(), stream_id, update_info, get_write_options(), segment_size.value_or(cfg_.write_options().segment_row_size()));
    return is_symbol_fragmented_impl(pre_defragmentation_info.segments_need_compaction);
}

VersionedItem LocalVersionedEngine::defragment_symbol_data(const StreamId& stream_id, std::optional<size_t> segment_size) {
    log::version().info("Defragmenting data for symbol {}", stream_id);

    // Currently defragmentation only for latest version - is there a use-case to allow compaction for older data?
    auto update_info = get_latest_undeleted_version_and_next_version_id(
        store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});

    auto versioned_item = defragment_symbol_data_impl(
            store(), stream_id, update_info, get_write_options(),
            segment_size.has_value() ? segment_size.value() : cfg_.write_options().segment_row_size());

    version_map_->write_version(store_, versioned_item.key_);

    if(cfg_.symbol_list())
        symbol_list().add_symbol(store_, stream_id);
    
    return versioned_item;
}

folly::Future<ReadVersionOutput> async_read_direct(
    const std::shared_ptr<Store>& store,
    const VariantKey& index_key,
    SegmentInMemory&& index_segment,
    const ReadQuery& read_query,
    std::shared_ptr<BufferHolder> buffers,
    const ReadOptions& read_options) {
    auto index_segment_reader = std::make_shared<index::IndexSegmentReader>(std::move(index_segment));

    check_column_and_date_range_filterable(*index_segment_reader, read_query);
    add_index_columns_to_query(read_query, index_segment_reader->tsd());

    auto pipeline_context = std::make_shared<PipelineContext>(StreamDescriptor{index_segment_reader->tsd().as_stream_descriptor()});
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

    return fetch_data(frame, pipeline_context, store, dynamic_schema, buffers).thenValue(
        [pipeline_context, frame, read_options](auto &&) mutable {
            ScopedGILLock gil_lock;
            reduce_and_fix_columns(pipeline_context, frame, read_options);
        }).thenValue(
        [index_segment_reader, frame, index_key, buffers](auto &&) {
            return ReadVersionOutput{VersionedItem{to_atom(index_key)},
                                  FrameAndDescriptor{frame, std::move(index_segment_reader->mutable_tsd()), {}, buffers}};
        });
}

std::vector<ReadVersionOutput> LocalVersionedEngine::batch_read_keys(
    const std::vector<AtomKey> &keys,
    const std::vector<ReadQuery> &read_queries,
    const ReadOptions& read_options) {
    py::gil_scoped_release release_gil;
    std::vector<folly::Future<std::pair<entity::VariantKey, SegmentInMemory>>> index_futures;
    for (auto &index_key: keys) {
        index_futures.push_back(store()->read(index_key));
    }
    auto indexes = folly::collect(index_futures).get();

    std::vector<folly::Future<ReadVersionOutput>> results_fut;
    auto i = 0u;
    util::check(read_queries.empty() || read_queries.size() == keys.size(), "Expected read queries to either be empty or equal to size of keys");
    for (auto&& [index_key, index_segment] : indexes) {
        results_fut.push_back(async_read_direct(store(), keys[i], std::move(index_segment), read_queries.empty() ? ReadQuery{} : read_queries[i], std::make_shared<BufferHolder>(), read_options));
        ++i;
    }
    Allocator::instance()->trim();
    return folly::collect(results_fut).get();
}

std::vector<std::variant<ReadVersionOutput, DataError>> LocalVersionedEngine::temp_batch_read_internal_direct(
    const std::vector<StreamId> &stream_ids,
    const std::vector<VersionQuery> &version_queries,
    std::vector<ReadQuery> &read_queries,
    const ReadOptions &read_options) {
    py::gil_scoped_release release_gil;

    auto versions = batch_get_versions_async(store(), version_map(), stream_ids, version_queries, read_options.read_previous_on_failure_);
    std::vector<folly::Future<ReadVersionOutput>> read_versions_futs;
    for (auto&& [idx, version] : folly::enumerate(versions)) {
        auto read_query = read_queries.empty() ? ReadQuery{} : read_queries[idx];
        read_versions_futs.emplace_back(std::move(version)
            .thenValue([store = store()](auto&& maybe_index_key) {
                           missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
                                   maybe_index_key.has_value(),
                                   "Version not found for symbol");
                           return store->read(*maybe_index_key);
                       })
            .thenValue([store = store(), read_query, read_options](auto&& key_segment_pair) {
                auto [index_key, index_segment] = std::move(key_segment_pair);
                return async_read_direct(store,
                                         std::move(index_key),
                                         std::move(index_segment),
                                         read_query,
                                         std::make_shared<BufferHolder>(),
                                         read_options);
            })
        );
    }
    // TODO: https://github.com/man-group/ArcticDB/issues/241
    // Move everything from here to the end of the function out into batch_read_internal as part of #241
    auto read_versions = folly::collectAll(read_versions_futs).get();
    std::vector<std::variant<ReadVersionOutput, DataError>> read_versions_or_errors;
    read_versions_or_errors.reserve(read_versions.size());
    for (auto&& [idx, read_version]: folly::enumerate(read_versions)) {
        if (read_version.hasValue()) {
            read_versions_or_errors.emplace_back(std::move(read_version.value()));
        } else {
            if (*read_options.batch_throw_on_error_) {
                read_version.throwUnlessValue();
            } else {
                auto exception = read_version.exception();
                DataError data_error(stream_ids[idx], exception.what().toStdString(), version_queries[idx].content_);
                if (exception.is_compatible_with<NoSuchVersionException>()) {
                    data_error.set_error_code(ErrorCode::E_NO_SUCH_VERSION);
                } else if (exception.is_compatible_with<storage::KeyNotFoundException>()) {
                    data_error.set_error_code(ErrorCode::E_KEY_NOT_FOUND);
                }
                read_versions_or_errors.emplace_back(std::move(data_error));
            }
        }
    }
    return read_versions_or_errors;
}

std::vector<std::variant<ReadVersionOutput, DataError>> LocalVersionedEngine::batch_read_internal(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries,
    std::vector<ReadQuery>& read_queries,
    const ReadOptions& read_options) {
    // This read option should always be set when calling batch_read
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(read_options.batch_throw_on_error_.has_value(),
                                                    "ReadOptions::batch_throw_on_error_ should always be set here");

    if(std::none_of(std::begin(read_queries), std::end(read_queries), [] (const auto& read_query) {
        return !read_query.clauses_.empty();
    })) {
        return temp_batch_read_internal_direct(stream_ids, version_queries, read_queries, read_options);
    }

    std::vector<std::variant<ReadVersionOutput, DataError>> read_versions_or_errors;
    read_versions_or_errors.reserve(stream_ids.size());
    for (size_t idx=0; idx < stream_ids.size(); idx++) {
        auto version_query = version_queries.size() > idx ? version_queries[idx] : VersionQuery{};
        auto read_query = read_queries.size() > idx ? read_queries[idx] : ReadQuery{};
        // TODO: https://github.com/man-group/ArcticDB/issues/241
        // Remove this try-catch in favour of the implementation in temp_batch_read_internal_direct as part of #241
        try {
            auto read_version = read_dataframe_version_internal(stream_ids[idx],
                                                                version_query,
                                                                read_query,
                                                                read_options);
            read_versions_or_errors.emplace_back(std::move(read_version));
        } catch (const NoSuchVersionException& e) {
            if (*read_options.batch_throw_on_error_) {
                throw;
            }
            read_versions_or_errors.emplace_back(DataError(stream_ids[idx],
                                                           e.what(),
                                                           version_query.content_,
                                                           ErrorCode::E_NO_SUCH_VERSION));
        } catch (const storage::NoDataFoundException& e) {
            if (*read_options.batch_throw_on_error_) {
                throw;
            }
            read_versions_or_errors.emplace_back(DataError(stream_ids[idx],
                                                           e.what(),
                                                           version_query.content_,
                                                           ErrorCode::E_KEY_NOT_FOUND));
        } catch (const storage::KeyNotFoundException& e) {
            if (*read_options.batch_throw_on_error_) {
                throw;
            }
            read_versions_or_errors.emplace_back(DataError(stream_ids[idx],
                                                           e.what(),
                                                           version_query.content_,
                                                           ErrorCode::E_KEY_NOT_FOUND));
        } catch (const std::exception& e) {
            if (*read_options.batch_throw_on_error_) {
                throw;
            }
            read_versions_or_errors.emplace_back(DataError(stream_ids[idx],
                                                           e.what(),
                                                           version_query.content_));
        }
    }
    return read_versions_or_errors;
}

void LocalVersionedEngine::write_version_and_prune_previous_if_needed(
        bool prune_previous_versions,
        const AtomKey& new_version,
        const std::optional<IndexTypeKey>& previous_key) {
    if (prune_previous_versions) {
        auto pruned_indexes = version_map()->write_and_prune_previous(store(), new_version, previous_key);
        delete_unreferenced_pruned_indexes(pruned_indexes, new_version).get();
    }
    else {
        version_map()->write_version(store(), new_version);
    }
}

std::vector<folly::Future<folly::Unit>> LocalVersionedEngine::batch_write_version_and_prune_if_needed(
    const std::vector<AtomKey>& index_keys,
    const std::vector<UpdateInfo>& stream_update_info_vector,
    bool prune_previous_versions) {
    if(prune_previous_versions) {
        std::vector<folly::Future<folly::Unit>> pruned_keys_futures;
        auto pruned_versions_futures = batch_write_and_prune_previous(store(), version_map(), index_keys, stream_update_info_vector);
        for(auto&& [idx, pruned_version_fut] : folly::enumerate(pruned_versions_futures)) {
            pruned_keys_futures.push_back(
                std::move(pruned_version_fut)
                .thenValue([this, &index_key = index_keys[idx]](auto&& atom_key_vec){
                    return delete_unreferenced_pruned_indexes(std::move(atom_key_vec), index_key);
                })
            );
        }
        return pruned_keys_futures;
    }
    else {
        return batch_write_version(store(), version_map(), index_keys);
    }
}

folly::Future<VersionedItem> LocalVersionedEngine::write_index_key_to_version_map_async(
    const std::shared_ptr<VersionMap> &version_map,
    AtomKey&& index_key,
    UpdateInfo&& stream_update_info,
    bool prune_previous_versions,
    bool add_new_symbol = true) {

    folly::Future<folly::Unit> write_version_fut;

    if(prune_previous_versions) {
        write_version_fut = async::submit_io_task(WriteAndPrunePreviousTask{store(), version_map, index_key, std::move(stream_update_info.previous_index_key_)})
        .thenValue([this, index_key](auto&& atom_key_vec){
            return delete_unreferenced_pruned_indexes(std::move(atom_key_vec), index_key);
        });
    } else {
        write_version_fut = async::submit_io_task(WriteVersionTask{store(), version_map, index_key});
    }

    if(add_new_symbol){
        write_version_fut = std::move(write_version_fut)
        .then([this, index_key_id = index_key.id()](auto &&) {
            return async::submit_io_task(WriteSymbolTask(store(), symbol_list_ptr(), index_key_id));
        });
    }
    return std::move(write_version_fut)
    .thenValue([index_key = std::move(index_key)](auto &&) mutable {
        return VersionedItem(std::move(index_key));
    });
}

std::vector<folly::Future<AtomKey>> LocalVersionedEngine::batch_write_internal(
    std::vector<VersionId> version_ids,
    const std::vector<StreamId>& stream_ids,
    std::vector<InputTensorFrame>&& frames,
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
    return results_fut;
}

VersionIdAndDedupMapInfo LocalVersionedEngine::create_version_id_and_dedup_map(
    const version_store::UpdateInfo&& update_info, 
    const StreamId& stream_id, 
    const WriteOptions& write_options){
    if(cfg().write_options().de_duplication()) {
        return VersionIdAndDedupMapInfo{update_info.next_version_id_, get_de_dup_map(stream_id, update_info.previous_index_key_, write_options), std::move(update_info)};
    } else {
        return VersionIdAndDedupMapInfo{update_info.next_version_id_, std::make_shared<DeDupMap>(), std::move(update_info)};
    }
}

std::vector<std::variant<VersionedItem, DataError>> LocalVersionedEngine::batch_write_versioned_dataframe_internal(
    const std::vector<StreamId>& stream_ids,
    std::vector<InputTensorFrame>&& frames,
    bool prune_previous_versions,
    bool validate_index,
    bool throw_on_error
) {
    py::gil_scoped_release release_gil;

    auto write_options = get_write_options();
    auto update_info_futs = batch_get_latest_undeleted_version_and_next_version_id_async(store(),
                                                                                         version_map(),
                                                                                         stream_ids);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(stream_ids.size() == update_info_futs.size(), "stream_ids and update_info_futs must be of the same size");
    std::vector<folly::Future<VersionedItem>> version_futures;
    for(auto&& update_info_fut : folly::enumerate(update_info_futs)) {
        auto idx = update_info_fut.index;
        version_futures.push_back(std::move(*update_info_fut)
            .thenValue([this, &stream_id = stream_ids[idx], &write_options](auto&& update_info){
                return create_version_id_and_dedup_map(std::move(update_info), stream_id, write_options);
            }).via(&async::cpu_executor())
            .thenValue([this, &stream_id = stream_ids[idx], &write_options, &validate_index, &frame = frames[idx]](
                auto&& version_id_and_dedup_map){
                    auto& [version_id, de_dup_map, update_info] = version_id_and_dedup_map;
                    ARCTICDB_SAMPLE(WriteDataFrame, 0)
                    ARCTICDB_DEBUG(log::version(), "Writing dataframe for stream id {}", stream_id);
                    auto write_fut = async_write_dataframe_impl( store(),
                        version_id,
                        std::move(frame),
                        write_options,
                        de_dup_map,
                        false,
                        validate_index
                    );
                    return std::move(write_fut)
                    .thenValue([update_info = std::move(update_info)](auto&& index_key) mutable {
                        return IndexKeyAndUpdateInfo{std::move(index_key), std::move(update_info)};
                    });
            })
            .thenValue([this, prune_previous_versions](auto&& index_key_and_update_info){
                auto&& [index_key, update_info] = index_key_and_update_info;
                return write_index_key_to_version_map_async(version_map(), std::move(index_key), std::move(update_info), prune_previous_versions);
            })
        );
    }
    auto write_versions = folly::collectAll(version_futures).get();
    std::vector<std::variant<VersionedItem, DataError>> write_versions_or_errors;
    write_versions_or_errors.reserve(write_versions.size());
    for (auto&& [idx, write_version]: folly::enumerate(write_versions)) {
        if (write_version.hasValue()) {
            write_versions_or_errors.emplace_back(std::move(write_version.value()));
        } else {
            if (throw_on_error) {
                write_version.throwUnlessValue();
            }
            auto exception = write_version.exception();
            DataError data_error(stream_ids[idx], exception.what().toStdString());
            if (exception.is_compatible_with<storage::KeyNotFoundException>()) {
                data_error.set_error_code(ErrorCode::E_KEY_NOT_FOUND);
            }
            write_versions_or_errors.emplace_back(std::move(data_error));
        }
    }
    return write_versions_or_errors;
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
                                                                        VersionQuery{},
                                                                        ReadOptions{});

    if(update_info.previous_index_key_.has_value()) {
        auto versioned_item = append_impl(store(),
                                          update_info,
                                          std::move(frame),
                                          get_write_options(),
                                          validate_index);
        write_version_and_prune_previous_if_needed(
            prune_previous_versions, versioned_item.key_, update_info.previous_index_key_);
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

std::vector<std::variant<VersionedItem, DataError>> LocalVersionedEngine::batch_append_internal(
    const std::vector<StreamId>& stream_ids,
    std::vector<InputTensorFrame>&& frames,
    bool prune_previous_versions,
    bool validate_index,
    bool upsert,
    bool throw_on_error) {
    py::gil_scoped_release release_gil;

    auto stream_update_info_futures = batch_get_latest_undeleted_version_and_next_version_id_async(store(),
                                                                                                    version_map(),
                                                                                                    stream_ids);
    std::vector<folly::Future<VersionedItem>> append_versions_futs;
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(stream_ids.size() == stream_update_info_futures.size(), "stream_ids and stream_update_info_futures must be of the same size");
    for (const auto&& [idx, stream_update_info_fut] : folly::enumerate(stream_update_info_futures)) {
        append_versions_futs.push_back(
            std::move(stream_update_info_fut)
            .thenValue([this, frame = std::move(frames[idx]), validate_index, stream_id = stream_ids[idx], upsert](auto&& update_info) mutable -> folly::Future<IndexKeyAndUpdateInfo> {
                auto index_key_fut = folly::Future<AtomKey>::makeEmpty();
                auto write_options = get_write_options();
                if (update_info.previous_index_key_.has_value()) {
                    index_key_fut = async_append_impl(store(), update_info, std::move(frame), write_options, validate_index);
                } else {
                    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
                    upsert,
                    "Cannot append to non-existent symbol {}", stream_id);
                    auto version_id = 0;
                    auto de_dup_map = std::make_shared<DeDupMap>();
                    index_key_fut = async_write_dataframe_impl(store(), version_id, std::move(frame), write_options, de_dup_map, false, validate_index);
                }
                return std::move(index_key_fut)
                .thenValue([update_info = std::move(update_info)](auto&& index_key) mutable -> IndexKeyAndUpdateInfo {
                    return IndexKeyAndUpdateInfo{std::move(index_key), std::move(update_info)};
                });
            })
            .thenValue([this, prune_previous_versions](auto&& index_key_and_update_info)  -> folly::Future<VersionedItem> {
                auto&& [index_key, update_info] = index_key_and_update_info;
                return write_index_key_to_version_map_async(version_map(), std::move(index_key), std::move(update_info), prune_previous_versions);
            })
        );
    }

    auto append_versions = folly::collectAll(append_versions_futs).get();
    std::vector<std::variant<VersionedItem, DataError>> append_versions_or_errors;
    append_versions_or_errors.reserve(append_versions.size());
    for (auto&& [idx, append_version]: folly::enumerate(append_versions)) {
        if (append_version.hasValue()) {
            append_versions_or_errors.emplace_back(std::move(append_version.value()));
        } else {
            if (throw_on_error) {
                append_version.throwUnlessValue();
            } else {
                auto exception = append_version.exception();
                DataError data_error(stream_ids[idx], exception.what().toStdString());
                if (exception.is_compatible_with<NoSuchVersionException>()) {
                    data_error.set_error_code(ErrorCode::E_NO_SUCH_VERSION);
                } else if (exception.is_compatible_with<storage::KeyNotFoundException>()) {
                    data_error.set_error_code(ErrorCode::E_KEY_NOT_FOUND);
                }
                append_versions_or_errors.emplace_back(std::move(data_error));
            }
        }
    }
    return append_versions_or_errors;
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

std::vector<std::pair<VersionedItem, TimeseriesDescriptor>> LocalVersionedEngine::batch_restore_version_internal(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries) {
    util::check(stream_ids.size() == version_queries.size(), "Symbol vs version query size mismatch: {} != {}", stream_ids.size(), version_queries.size());
    auto sym_versions = get_sym_versions_from_query(stream_ids, version_queries);
    util::check(sym_versions.size() == version_queries.size(), "Restore versions requires specific version to be supplied");
    auto previous = batch_get_latest_version(store(), version_map(), stream_ids, false);
    auto versions_to_restore = batch_get_specific_version(store(), version_map(), sym_versions);
    std::vector<folly::Future<std::pair<VersionedItem, TimeseriesDescriptor>>> fut_vec;

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
    auto version = get_version_to_read(stream_id, version_query, ReadOptions{});
    if(!version)
        throw storage::NoDataFoundException(fmt::format("get_update_time: version not found for symbol", stream_id));
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

folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>> LocalVersionedEngine::get_metadata(
    std::optional<AtomKey>&& key){
    if (key.has_value()){
        return store()->read_metadata(key.value());
    }else{
        return folly::makeFuture(std::make_pair(std::nullopt, std::nullopt));
    }
}

folly::Future<std::pair<VariantKey, std::optional<google::protobuf::Any>>> LocalVersionedEngine::get_metadata_async(
    folly::Future<std::optional<AtomKey>>&& version_fut,
    const StreamId& stream_id,
    const VersionQuery& version_query
    ) {
    return  std::move(version_fut)
    .thenValue([this, &stream_id, &version_query](std::optional<AtomKey>&& key){
        missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(key.has_value(),
        "Unable to retrieve  metadata. {}@{}: version not found", stream_id, version_query);
        return get_metadata(std::move(key));
    })
    .thenValue([](auto&& metadata){
        auto&& [opt_key, meta_proto] = metadata;
        return std::make_pair(std::move(*opt_key), std::move(meta_proto));
    });
}
 
std::vector<std::variant<std::pair<VariantKey, std::optional<google::protobuf::Any>>, DataError>> LocalVersionedEngine::batch_read_metadata_internal(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries,
    const ReadOptions& read_options
    ) {
    // This read option should always be set when calling batch_read_metadata
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(read_options.batch_throw_on_error_.has_value(),
                                                    "ReadOptions::batch_throw_on_error_ should always be set here");
    auto version_futures = batch_get_versions_async(store(), version_map(), stream_ids, version_queries, read_options.read_previous_on_failure_);
    std::vector<folly::Future<std::pair<VariantKey, std::optional<google::protobuf::Any>>>> metadata_futures;
    for (auto&& [idx, version]: folly::enumerate(version_futures)) {
        metadata_futures.push_back(get_metadata_async(std::move(version), stream_ids[idx], version_queries[idx]));
    }

    auto metadatas = folly::collectAll(metadata_futures).get();
    std::vector<std::variant<std::pair<VariantKey, std::optional<google::protobuf::Any>>, DataError>> metadatas_or_errors;
    metadatas_or_errors.reserve(metadatas.size());
    for (auto&& [idx, metadata]: folly::enumerate(metadatas)) {
        if (metadata.hasValue()) {
            metadatas_or_errors.emplace_back(std::move(metadata.value()));
        } else {
            auto exception = metadata.exception();
            // For historical reasons, batch_read_metadata does not raise if the version does not exist (unlike batch_read)
            if (*read_options.batch_throw_on_error_ && !exception.is_compatible_with<NoSuchVersionException>()) {
                metadata.throwUnlessValue();
            } else {
                DataError data_error(stream_ids[idx], exception.what().toStdString(), version_queries[idx].content_);
                if (exception.is_compatible_with<NoSuchVersionException>()) {
                    data_error.set_error_code(ErrorCode::E_NO_SUCH_VERSION);
                } else if (exception.is_compatible_with<storage::KeyNotFoundException>()) {
                    data_error.set_error_code(ErrorCode::E_KEY_NOT_FOUND);
                }
                metadatas_or_errors.emplace_back(std::move(data_error));
            }
        }
    }
    return metadatas_or_errors;
}

std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>> LocalVersionedEngine::read_metadata_internal(
    const StreamId& stream_id,
    const VersionQuery& version_query,
    const ReadOptions& read_options
    ) {
    auto version = get_version_to_read(stream_id, version_query, read_options);
    std::optional<AtomKey> key = version.has_value() ? std::make_optional<AtomKey>(version->key_) : std::nullopt;
    return get_metadata(std::move(key)).get();
}


VersionedItem LocalVersionedEngine::sort_merge_internal(
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    bool append,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify
    ) {
    auto update_info = get_latest_undeleted_version_and_next_version_id(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
    auto versioned_item = sort_merge_impl(store_, stream_id, user_meta, update_info, append, convert_int_to_float, via_iteration, sparsify);
    version_map()->write_version(store(), versioned_item.key_);
    return versioned_item;
}

bool LocalVersionedEngine::has_stream(const StreamId & stream_id){
    auto opt = get_latest_undeleted_version(store(), version_map(),  stream_id, VersionQuery{}, ReadOptions{});
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
    if(auto latest_incomplete = latest_incomplete_timestamp(store(), symbol); latest_incomplete)
        return latest_incomplete.value();

    if(auto latest_key = get_latest_version(symbol, VersionQuery{}, ReadOptions{}); latest_key)
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
        std::atomic<size_t> key_size{0};
        for(auto&& key ARCTICDB_UNUSED : keys) {
            continuations.emplace_back([&key_size] (auto&& ks) {
                auto key_seg = std::move(ks);
                key_size += key_seg.segment().total_segment_size();
                return key_seg.variant_key();
            });
        }
        store->batch_read_compressed(std::move(keys), std::move(continuations),BatchReadArgs{});
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

std::shared_ptr<VersionMap> LocalVersionedEngine::_test_get_version_map() {
    return version_map();
}

void LocalVersionedEngine::_test_set_store(std::shared_ptr<Store> store) {
    set_store(std::move(store));
}
} // arcticdb::version_store
