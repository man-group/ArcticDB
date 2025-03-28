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
#include <arcticdb/util/allocation_tracing.hpp>
#include <bitset>

namespace arcticdb::version_store {

template<class ClockType>
LocalVersionedEngine::LocalVersionedEngine(
        const std::shared_ptr<storage::Library>& library,
        const ClockType&) :
    store_(std::make_shared<async::AsyncStore<ClockType>>(library, codec::default_lz4_codec(), encoding_version(library->config()))),
    symbol_list_(std::make_shared<SymbolList>(version_map_)){
    initialize(library);
}

template<class ClockType>
LocalVersionedEngine::LocalVersionedEngine(
    const std::shared_ptr<Store>& store,
    const ClockType&) :
    store_(store),
    symbol_list_(std::make_shared<SymbolList>(version_map_)){
}

void LocalVersionedEngine::initialize(const std::shared_ptr<storage::Library>& library) {
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
    (void)async::TaskScheduler::instance();
#ifdef ARCTICDB_COUNT_ALLOCATIONS
    (void)AllocationTracker::instance();
    AllocationTracker::start();
#endif
}

template LocalVersionedEngine::LocalVersionedEngine(const std::shared_ptr<storage::Library>& library, const util::SysClock&);
template LocalVersionedEngine::LocalVersionedEngine(const std::shared_ptr<Store>& library, const util::SysClock&);
template LocalVersionedEngine::LocalVersionedEngine(const std::shared_ptr<storage::Library>& library, const util::ManualClock&);

struct TransformBatchResultsFlags {
    /// If true processing of batch results will throw exception on the first error it observes and stop processing
    /// further results (V1 Library API behavior)
    /// If false it will create a DataError recording the exception information and continue processing further results
    /// (V2 Library API behavior)
    bool throw_on_error_{false};
    /// Applies only if TransformBatchResultsFlags::throw_on_error_ is true
    /// For historical reasons batch reading of metadata in V1 Library API does not throw when the symbol version is
    /// missing even if throw on error is true. Every other operation must throw in this case.
    bool throw_on_missing_symbol_{true};
    /// Applies only if TransformBatchResultsFlags::throw_on_error_ is true
    /// Used only by batch read in order to preserve the API. For historical reasons batch_read converts
    /// ErrorCategory::MISSING_DATA to ErrorCode::E_KEY_NOT_FOUND
    bool convert_no_data_found_to_key_not_found_{false};
};

/// Used by batch_[append/update/read/append] methods to process the individual results of a batch query.
/// @param throw_on_error The V1 Library API aborts on the first exception while V2 processes everything and converts
///     exceptions to DataError
/// @param stream_ids i-th element of stream_ids corresponds to i-th element of batch_request_versions
/// @param versoin_queries i-th element corresponds to the i-th element of batch_request_versions. Use to record the
///     the version query in the DataError for a batch read request
template<typename ResultValueType>
std::vector<std::variant<ResultValueType, DataError>> transform_batch_items_or_throw(
    std::vector<folly::Try<ResultValueType>>&& batch_request_versions,
    std::span<const StreamId> stream_ids,
    TransformBatchResultsFlags flags,
    std::span<const VersionQuery> versoin_queries = {}
) {
    std::vector<std::variant<ResultValueType, DataError>> result;
    result.reserve(batch_request_versions.size());
    for (auto&& [idx, version_or_exception]: enumerate(batch_request_versions)) {
        if (version_or_exception.hasValue()) {
            result.emplace_back(std::move(version_or_exception.value()));
        } else {
            auto exception = version_or_exception.exception();
            const bool is_missing_version_exception = exception.template is_compatible_with<NoSuchVersionException>();
            const bool throw_on_missing_symbol = (is_missing_version_exception && flags.throw_on_missing_symbol_);
            if (flags.throw_on_error_ && (!is_missing_version_exception || throw_on_missing_symbol)) {
                version_or_exception.throwUnlessValue();
            } else {
                DataError data_error = versoin_queries.empty() ?
                    DataError(stream_ids[idx], exception.what().toStdString()) :
                    DataError(stream_ids[idx], exception.what().toStdString(), versoin_queries[idx].content_);
                if (exception.template is_compatible_with<NoSuchVersionException>()) {
                    data_error.set_error_code(ErrorCode::E_NO_SUCH_VERSION);
                } else if (exception.template is_compatible_with<storage::KeyNotFoundException>()) {
                    data_error.set_error_code(ErrorCode::E_KEY_NOT_FOUND);
                } else if(flags.convert_no_data_found_to_key_not_found_ &&
                        exception.template is_compatible_with<storage::NoDataFoundException>()) {
                    data_error.set_error_code(ErrorCode::E_KEY_NOT_FOUND);
                }
                result.emplace_back(std::move(data_error));
            }
        }
    }
    return result;
}

folly::Future<folly::Unit> LocalVersionedEngine::delete_unreferenced_pruned_indexes(
        std::vector<AtomKey>&& pruned_indexes,
        const AtomKey& key_to_keep
) {
    try {
        if (!pruned_indexes.empty() && !cfg().write_options().delayed_deletes()) {
            // TODO: the following function will load all snapshots, which will be horrifyingly inefficient when called
            // multiple times from batch_*
            auto [not_in_snaps, in_snaps] = get_index_keys_partitioned_by_inclusion_in_snapshots(
                    store(),
                    pruned_indexes.begin()->id(),
                    std::move(pruned_indexes));
            in_snaps.insert(key_to_keep);
            PreDeleteChecks checks{false, false, false, false, std::move(in_snaps)};
            return delete_trees_responsibly(store(), version_map(), not_in_snaps, {}, {}, checks)
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
    auto versioned_item = get_version_to_read(stream_id, version_query);
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
    auto versioned_item = get_version_to_read(stream_id, version_query);
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
    auto versioned_item = get_version_to_read(stream_id, version_query);
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
    auto versioned_item = get_version_to_read(stream_id, version_query);
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
            versioned_item.has_value(),
            "get_column_stats_info_version_internal: version not found for stream '{}'",
            stream_id
            );
    return get_column_stats_info_internal(versioned_item.value());
}

std::set<StreamId> LocalVersionedEngine::list_streams_internal(
    std::optional<SnapshotId> snap_name,
    const std::optional<std::string>& regex,
    const std::optional<std::string>& prefix,
    const std::optional<bool>& use_symbol_list,
    const std::optional<bool>& all_symbols
    ) {
    ARCTICDB_SAMPLE(ListStreamsInternal, 0)
    auto res = std::set<StreamId>();

    if (snap_name) {
        res = list_streams_in_snapshot(store(), *snap_name);
    } else {
        if(use_symbol_list.value_or(cfg().symbol_list()))
            res = symbol_list().get_symbol_set(store());
        else
            res = list_streams(store(), version_map(), prefix, all_symbols.value_or(false));
    }

    if (regex) {
        return filter_by_regex(res, regex);
    } else if (prefix) {
        return filter_by_regex(res, std::optional("^" + *prefix));
    }
    return res;
}

size_t LocalVersionedEngine::compact_symbol_list_internal() {
    ARCTICDB_SAMPLE(CompactSymbolListInternal, 0)
    return symbol_list().compact(store());
}

std::string LocalVersionedEngine::dump_versions(const StreamId& stream_id) {
    return version_map()->dump_entry(store(), stream_id);
}

std::optional<VersionedItem> LocalVersionedEngine::get_latest_version(
    const StreamId &stream_id) {
    auto key = get_latest_undeleted_version(store(), version_map(), stream_id);
    if (!key) {
        ARCTICDB_DEBUG(log::version(), "get_latest_version didn't find version for stream_id: {}", stream_id);
        return std::nullopt;
    }
    return VersionedItem{std::move(*key)};
}

std::optional<VersionedItem> LocalVersionedEngine::get_specific_version(
    const StreamId &stream_id,
    SignedVersionId signed_version_id,
    const VersionQuery& version_query) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: get_specific_version");
    auto key = ::arcticdb::get_specific_version(store(), version_map(), stream_id, signed_version_id);
    if (key) {
        return VersionedItem{std::move(key.value())};
    } else if (std::get<SpecificVersionQuery>(version_query.content_).iterate_snapshots_if_tombstoned) {
        VersionId version_id;
        if (signed_version_id >= 0) {
            version_id = static_cast<VersionId>(signed_version_id);
        } else {
            auto [opt_latest_key, deleted] = ::arcticdb::get_latest_version(store(), version_map(), stream_id);
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
            return VersionedItem{std::move(*index_key)};
        } else {
            ARCTICDB_DEBUG(log::version(), "get_specific_version: "
                                 "version id not found for stream {} version {}", stream_id, version_id);
            return std::nullopt;
        }
    } else {
        return std::nullopt;
    }
}

std::optional<VersionedItem> LocalVersionedEngine::get_version_at_time(
    const StreamId& stream_id,
    timestamp as_of,
    const VersionQuery& version_query
    ) {

    auto index_key = load_index_key_from_time(store(), version_map(), stream_id, as_of);
    if (!index_key && std::get<TimestampVersionQuery>(version_query.content_).iterate_snapshots_if_tombstoned) {
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

    return VersionedItem(std::move(*index_key));
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
    auto segment = opt_snapshot->second;
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
       [&stream_id, &version_query, this](const SpecificVersionQuery &specific) {
            return get_specific_version(stream_id, specific.version_id_, version_query);
        },
        [&stream_id, this](const SnapshotVersionQuery &snapshot) {
            return get_version_from_snapshot(stream_id, snapshot.name_);
        },
        [&stream_id, &version_query, this](const TimestampVersionQuery &timestamp) {
            return get_version_at_time(stream_id, timestamp.timestamp_, version_query);
        },
        [&stream_id, this](const std::monostate &) {
            return get_latest_version(stream_id);
    }
    );
}

IndexRange LocalVersionedEngine::get_index_range(
    const StreamId &stream_id,
    const VersionQuery& version_query) {
    auto version = get_version_to_read(stream_id, version_query);
    if(!version)
        return unspecified_range();

    return index::get_index_segment_range(version->key_, store());
}

std::variant<VersionedItem, StreamId> get_version_identifier(
        const StreamId& stream_id,
        const VersionQuery& version_query,
        const ReadOptions& read_options,
        const std::optional<VersionedItem>& version) {
    if (!version) {
        if (opt_false(read_options.incompletes())) {
            log::version().warn("No index: Key not found for {}, will attempt to use incomplete segments.", stream_id);
            return stream_id;
        } else {
            missing_data::raise<ErrorCode::E_NO_SUCH_VERSION>(
                "read_dataframe_version: version matching query '{}' not found for symbol '{}'",
                version_query,
                stream_id
            );
        }
    }
    return *version;
}

ReadVersionOutput LocalVersionedEngine::read_dataframe_version_internal(
    const StreamId &stream_id,
    const VersionQuery& version_query,
    const std::shared_ptr<ReadQuery>& read_query,
    const ReadOptions& read_options,
    std::any& handler_data) {
    py::gil_scoped_release release_gil;
    auto version = get_version_to_read(stream_id, version_query);
    const auto identifier = get_version_identifier(stream_id, version_query, read_options, version);
    return read_frame_for_version(store(), identifier, read_query, read_options, handler_data).get();
}

folly::Future<DescriptorItem> LocalVersionedEngine::get_descriptor(
    AtomKey&& k){
    const auto key = std::move(k);
    return store()->read(key)
    .thenValue([](auto&& key_seg_pair) -> DescriptorItem {
        auto key = to_atom(std::move(key_seg_pair.first));
        auto seg = std::move(key_seg_pair.second);
        std::optional<TimeseriesDescriptor> timeseries_descriptor;
        if (seg.has_index_descriptor())
            timeseries_descriptor.emplace(seg.index_descriptor());

        std::optional<timestamp> start_index;
        std::optional<timestamp> end_index;
        if (seg.row_count() > 0) {
            const auto& start_index_column = seg.column(position_t(index::Fields::start_index));
            details::visit_type(start_index_column.type().data_type(), [&start_index_column, &start_index](auto column_desc_tag) {
                using type_info = ScalarTypeInfo<decltype(column_desc_tag)>;
                if constexpr (is_time_type(type_info::data_type)) {
                    start_index = start_index_column.template scalar_at<timestamp>(0);
                }
            });

            const auto& end_index_column = seg.column(position_t(index::Fields::end_index));
            details::visit_type(end_index_column.type().data_type(), [&end_index_column, &end_index, row_count=seg.row_count()](auto column_desc_tag) {
                using type_info = ScalarTypeInfo<decltype(column_desc_tag)>;
                if constexpr (is_time_type(type_info::data_type)) {
                    // -1 as the end timestamp in the data keys is one nanosecond greater than the last value in the index column
                    end_index = *end_index_column.template scalar_at<timestamp>(row_count - 1) - 1;
                }
            });
        }
        return DescriptorItem{std::move(key), start_index, end_index, std::move(timeseries_descriptor)};
    });
}

folly::Future<DescriptorItem> LocalVersionedEngine::get_descriptor_async(
    folly::Future<std::optional<AtomKey>>&& opt_index_key_fut,
    const StreamId& stream_id,
    const VersionQuery& version_query){
    return std::move(opt_index_key_fut)
    .thenValue([this, &stream_id, &version_query](std::optional<AtomKey>&& opt_index_key){
        missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(opt_index_key.has_value(),
        "Unable to retrieve descriptor data. {}@{}: version not found", stream_id, version_query);
        return get_descriptor(std::move(*opt_index_key));
    }).via(&async::cpu_executor());
}

DescriptorItem LocalVersionedEngine::read_descriptor_internal(
    const StreamId& stream_id,
    const VersionQuery& version_query
    ) {
    ARCTICDB_SAMPLE(ReadDescriptor, 0)
    auto version = get_version_to_read(stream_id, version_query);
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(version.has_value(),
        "Unable to retrieve descriptor data. {}@{}: version not found", stream_id, version_query);
    return get_descriptor(std::move(version->key_)).get();
}


std::vector<std::variant<DescriptorItem, DataError>> LocalVersionedEngine::batch_read_descriptor_internal(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries,
    const ReadOptions& read_options) {

    internal::check<ErrorCode::E_ASSERTION_FAILURE>(read_options.batch_throw_on_error().has_value(),
                                                    "ReadOptions::batch_throw_on_error_ should always be set here");

    auto opt_index_key_futs = batch_get_versions_async(store(), version_map(), stream_ids, version_queries);
    std::vector<folly::Future<DescriptorItem>> descriptor_futures;
    for (auto&& [idx, opt_index_key_fut]: folly::enumerate(opt_index_key_futs)) {
        descriptor_futures.emplace_back(
            get_descriptor_async(std::move(opt_index_key_fut), stream_ids[idx], version_queries[idx]));
    }
    auto descriptors = collectAll(descriptor_futures).get();
    TransformBatchResultsFlags flags;
    flags.throw_on_error_ = *read_options.batch_throw_on_error();
    return transform_batch_items_or_throw(std::move(descriptors), stream_ids, flags, version_queries);
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
        auto maybe_undeleted_prev = get_latest_undeleted_version(store(), version_map(), stream_id);
        if (maybe_undeleted_prev) {
            // maybe_undeleted_prev is index key
            auto data_keys = get_data_keys(store(), {*maybe_undeleted_prev}, storage::ReadKeyOpts{});
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

VersionedItem LocalVersionedEngine::sort_index(const StreamId& stream_id, bool dynamic_schema, bool prune_previous_versions) {
    auto update_info = get_latest_undeleted_version_and_next_version_id(store(), version_map(), stream_id);
    util::check(update_info.previous_index_key_.has_value(), "Cannot sort_index a non-existent symbol {}", stream_id);
    auto [index_segment_reader, slice_and_keys] = index::read_index_to_vector(store(), *update_info.previous_index_key_);
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

    auto total_rows = adjust_slice_rowcounts(slice_and_keys, std::make_optional<size_t>(0U));

    auto index = index_type_from_descriptor(index_segment_reader.tsd().as_stream_descriptor());
    bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    auto& tsd = index_segment_reader.mutable_tsd();
    auto time_series = make_timeseries_descriptor(
        total_rows,
        StreamDescriptor{tsd.as_stream_descriptor()},
        std::move(*tsd.mutable_proto().mutable_normalization()),
        std::move(*tsd.mutable_proto().mutable_user_meta()),
        std::nullopt,
        std::nullopt,
        bucketize_dynamic);

    auto versioned_item = pipelines::index::index_and_version(index, store(), time_series, std::move(slice_and_keys), stream_id, update_info.next_version_id_).get();
    write_version_and_prune_previous(prune_previous_versions, versioned_item.key_, update_info.previous_index_key_);
    ARCTICDB_DEBUG(log::version(), "sorted index of stream_id: {} , version_id: {}", stream_id, update_info.next_version_id_);
    return versioned_item;
}

VersionedItem LocalVersionedEngine::delete_range_internal(
    const StreamId& stream_id,
    const UpdateQuery & query,
    const DeleteRangeOptions& option) {
    auto update_info = get_latest_undeleted_version_and_next_version_id(store(), version_map(), stream_id);
    auto versioned_item = delete_range_impl(store(),
                                            stream_id,
                                            update_info,
                                            query,
                                            get_write_options(),
                                            option.dynamic_schema_);
    write_version_and_prune_previous(option.prune_previous_versions_, versioned_item.key_, update_info.previous_index_key_);
    return versioned_item;
}

VersionedItem LocalVersionedEngine::update_internal(
    const StreamId& stream_id,
    const UpdateQuery& query,
    const std::shared_ptr<InputTensorFrame>& frame,
    bool upsert,
    bool dynamic_schema,
    bool prune_previous_versions) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: update");
    py::gil_scoped_release release_gil;
    auto update_info = get_latest_undeleted_version_and_next_version_id(store(), version_map(), stream_id);
    if (update_info.previous_index_key_.has_value()) {
        if (frame->empty()) {
            ARCTICDB_DEBUG(log::version(), "Updating existing data with an empty item has no effect. \n"
                                           "No new version is being created for symbol='{}', "
                                           "and the last version is returned", stream_id);
            return VersionedItem(*update_info.previous_index_key_);
        }

        auto versioned_item = update_impl(store(),
                                          update_info,
                                          query,
                                          frame,
                                          get_write_options(),
                                          dynamic_schema,
                                          cfg().write_options().empty_types());
        write_version_and_prune_previous(
            prune_previous_versions, versioned_item.key_, update_info.previous_index_key_);
        return versioned_item;
    } else {
        if (upsert) {
            auto write_options = get_write_options();
            write_options.dynamic_schema |= dynamic_schema;
            auto versioned_item =  write_dataframe_impl(store_,
                                                        update_info.next_version_id_,
                                                        frame,
                                                        write_options,
                                                        std::make_shared<DeDupMap>(),
                                                        false,
                                                        true);

            if(cfg_.symbol_list())
                symbol_list().add_symbol(store_, stream_id, update_info.next_version_id_);

            version_map()->write_version(store(), versioned_item.key_, std::nullopt);
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
                                                                        stream_id);
    if(update_info.previous_index_key_.has_value()) {
        ARCTICDB_DEBUG(log::version(), "write_versioned_metadata for stream_id: {}", stream_id);
        auto index_key = UpdateMetadataTask{store(), update_info, std::move(user_meta)}();
        write_version_and_prune_previous(prune_previous_versions, index_key, update_info.previous_index_key_);
        return VersionedItem{ std::move(index_key) };
    } else {
        auto frame = convert::py_none_to_frame();
        frame->desc.set_id(stream_id);
        frame->user_meta = std::move(user_meta);
        auto versioned_item = write_versioned_dataframe_internal(stream_id, frame, prune_previous_versions, false, false);
        if(cfg_.symbol_list())
            symbol_list().add_symbol(store_, stream_id, update_info.next_version_id_);

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
            .thenValue([this, user_meta_proto = std::move(user_meta_protos[idx]), &stream_id = stream_ids[idx]](auto&& update_info) mutable -> folly::Future<IndexKeyAndUpdateInfo> {
                auto index_key_fut = folly::Future<AtomKey>::makeEmpty();
                if (update_info.previous_index_key_.has_value()) {
                    index_key_fut = async::submit_io_task(UpdateMetadataTask{store(), update_info, std::move(user_meta_proto)});
                } else {
                    auto frame = convert::py_none_to_frame();
                    frame->desc.set_id(stream_id);
                    frame->user_meta = std::move(user_meta_proto);
                    auto version_id = 0;
                    auto write_options = get_write_options();
                    auto de_dup_map = std::make_shared<DeDupMap>();
                    index_key_fut = async_write_dataframe_impl(store(), version_id, frame, write_options, de_dup_map, false, false);
                }
                return std::move(index_key_fut)
                .thenValue([update_info=std::forward<decltype(update_info)>(update_info)](auto&& index_key) mutable -> IndexKeyAndUpdateInfo {
                    return IndexKeyAndUpdateInfo{std::move(index_key), std::move(update_info)};
                });
            })
            .thenValue([this, prune_previous_versions](auto&& index_key_and_update_info){
                auto&& [index_key, update_info] = index_key_and_update_info;
                return write_index_key_to_version_map_async(version_map(), std::move(index_key), std::move(update_info), prune_previous_versions, !update_info.previous_index_key_.has_value());
            }));
    }

    auto write_metadata_versions = collectAll(write_metadata_versions_futs).get();
    TransformBatchResultsFlags flags;
    flags.throw_on_error_ = throw_on_error;
    return transform_batch_items_or_throw(std::move(write_metadata_versions), stream_ids, flags);
}

VersionedItem LocalVersionedEngine::write_versioned_dataframe_internal(
    const StreamId& stream_id,
    const std::shared_ptr<InputTensorFrame>& frame,
    bool prune_previous_versions,
    bool allow_sparse,
    bool validate_index
    ) {
    ARCTICDB_SAMPLE(WriteVersionedDataFrame, 0)
    py::gil_scoped_release release_gil;
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write_versioned_dataframe");
    auto [maybe_prev, deleted] = ::arcticdb::get_latest_version(store(), version_map(), stream_id);
    auto version_id = get_next_version_from_key(maybe_prev);
    ARCTICDB_DEBUG(log::version(), "write_versioned_dataframe for stream_id: {} , version_id = {}", stream_id, version_id);
    auto write_options = get_write_options();
    auto de_dup_map = get_de_dup_map(stream_id, maybe_prev, write_options);

    auto versioned_item = write_dataframe_impl(
        store(),
        version_id,
        frame,
        write_options,
        de_dup_map,
        allow_sparse,
        validate_index);

    if(cfg().symbol_list())
        symbol_list().add_symbol(store(), stream_id, versioned_item.key_.version_id());

    write_version_and_prune_previous(prune_previous_versions, versioned_item.key_, deleted ? std::nullopt : maybe_prev);
    return versioned_item;
}

std::pair<VersionedItem, TimeseriesDescriptor> LocalVersionedEngine::restore_version(
    const StreamId& stream_id,
    const VersionQuery& version_query
    ) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: restore_version");
    auto version_to_restore = get_version_to_read(stream_id, version_query);
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(static_cast<bool>(version_to_restore),
                                                 "Unable to restore {}@{}: version not found", stream_id, version_query);
    auto [maybe_prev, deleted] = ::arcticdb::get_latest_version(store(), version_map(), stream_id);
    ARCTICDB_DEBUG(log::version(), "restore for stream_id: {} , version_id = {}", stream_id, version_to_restore->key_.version_id());
    return AsyncRestoreVersionTask{store(), version_map(), stream_id, version_to_restore->key_, maybe_prev}().get();
}

VersionedItem LocalVersionedEngine::write_individual_segment(
    const StreamId& stream_id,
    SegmentInMemory&& segment,
    bool prune_previous_versions
    ) {
    ARCTICDB_SAMPLE(WriteVersionedDataFrame, 0)

    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write individual segment");
    auto [maybe_prev, deleted] = ::arcticdb::get_latest_version(store(), version_map(), stream_id);
    auto version_id = get_next_version_from_key(maybe_prev);
    ARCTICDB_DEBUG(log::version(), "write individual segment for stream_id: {} , version_id = {}", stream_id, version_id);
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

    write_version_and_prune_previous(prune_previous_versions, versioned_item.key_, deleted ? std::nullopt : maybe_prev);
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

folly::Future<folly::Unit> delete_trees_responsibly(
        std::shared_ptr<Store> store,
        std::shared_ptr<VersionMap> &version_map,
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
                                                            snaps.count(*snapshot_being_deleted) : 0;
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
                auto load_strategy = load_type == LoadType::DOWNTO
                        ? LoadStrategy{load_type, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(min.second)}
                        : LoadStrategy{load_type, LoadObjective::INCLUDE_DELETED};
                const auto entry = version_map->check_reload(store, min.first, load_strategy, __FUNCTION__);
                entry_map.try_emplace(std::move(min.first), entry);
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
                    [](const AtomKey&) {},
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
    auto data_keys_to_be_deleted = recurse_index_keys(store, *keys_to_delete, read_opts);
    log::version().debug("Candidate: {} total of data keys", data_keys_to_be_deleted.size());

    read_opts.dont_warn_about_missing_key = true;
    auto data_keys_not_to_be_deleted = recurse_index_keys(store, *not_to_delete, read_opts);
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
        ARCTICDB_TRACE(log::version(), fmt::format("Column Stats keys to be deleted: {}", fmt::join(vks_column_stats, ", ")));
        ARCTICDB_TRACE(log::version(), fmt::format("Index keys to be deleted: {}", fmt::join(vks_to_delete, ", ")));
        ARCTICDB_TRACE(log::version(), fmt::format("Data keys to be deleted: {}", fmt::join(vks_data_to_delete, ", ")));

        // Delete any associated column stats keys first
        remove_keys_fut = store->remove_keys(std::move(vks_column_stats), remove_opts)
        .thenValue([store=store, vks_to_delete = std::move(vks_to_delete), remove_opts](auto&& ) mutable {
            log::version().debug("Column Stats keys deleted.");
            return store->remove_keys(std::move(vks_to_delete), remove_opts);
        })
        .thenValue([store=store, vks_data_to_delete = std::move(vks_data_to_delete), remove_opts](auto&&) mutable {
            log::version().debug("Index keys deleted.");
            return store->remove_keys(std::move(vks_data_to_delete), remove_opts);
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

void LocalVersionedEngine::remove_incompletes(
    const std::unordered_set<StreamId>& stream_ids,
    const std::string& common_prefix
) {
    remove_incomplete_segments(store_, stream_ids, common_prefix);
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

void LocalVersionedEngine::append_incomplete_frame(
    const StreamId& stream_id,
    const std::shared_ptr<InputTensorFrame>& frame,
    bool validate_index) const {
    arcticdb::append_incomplete(store_, stream_id, frame, validate_index);
}

void LocalVersionedEngine::append_incomplete_segment(
    const StreamId& stream_id,
    SegmentInMemory &&seg) {
    arcticdb::append_incomplete_segment(store_, stream_id, std::move(seg));
}

void LocalVersionedEngine::write_parallel_frame(
    const StreamId& stream_id,
    const std::shared_ptr<InputTensorFrame>& frame,
    bool validate_index,
    bool sort_on_index,
    const std::optional<std::vector<std::string>>& sort_columns) const {
    py::gil_scoped_release release_gil;
    WriteIncompleteOptions options{
        .validate_index=validate_index,
        .write_options=get_write_options(),
        .sort_on_index=sort_on_index,
        .sort_columns=sort_columns};
    write_parallel_impl(store_, stream_id, frame, options);
}

void LocalVersionedEngine::add_to_symbol_list_on_compaction(
        const StreamId& stream_id,
        const CompactIncompleteOptions& options,
        const UpdateInfo& update_info) {
    if(cfg_.symbol_list()) {
        if (!options.append_ || !update_info.previous_index_key_.has_value()) {
            symbol_list().add_symbol(store_, stream_id, update_info.next_version_id_);
        }
    }
}

VersionedItem LocalVersionedEngine::compact_incomplete_dynamic(
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    const CompactIncompleteOptions& options) {
    log::version().debug("Compacting incomplete symbol {} with options {}", stream_id, options);

    auto update_info = get_latest_undeleted_version_and_next_version_id(store(), version_map(), stream_id);
    if (update_info.previous_index_key_) {
        ARCTICDB_DEBUG(log::version(), "Found previous version for symbol {}", stream_id);
    } else {
        ARCTICDB_DEBUG(log::version(), "No previous version found for symbol {}", stream_id);
    };
    auto pipeline_context = std::make_shared<PipelineContext>();
    pipeline_context->stream_id_ = stream_id;
    pipeline_context->version_id_ = update_info.next_version_id_;
    auto delete_keys_on_failure = get_delete_keys_on_failure(pipeline_context, store(), options);

    auto versioned_item = compact_incomplete_impl(store_, stream_id, user_meta, update_info, options, get_write_options(), pipeline_context);
    ARCTICDB_DEBUG(log::version(), "Finished compact_incomplete_impl for symbol {}", stream_id);

    write_version_and_prune_previous(options.prune_previous_versions_, versioned_item.key_, update_info.previous_index_key_);
    ARCTICDB_DEBUG(log::version(), "Finished write_version_and_prune_previous for symbol {}", stream_id);

    add_to_symbol_list_on_compaction(stream_id, options, update_info);
    if (delete_keys_on_failure)
        delete_keys_on_failure->release();
    delete_incomplete_keys(*pipeline_context, *store());

    return versioned_item;
}

bool LocalVersionedEngine::is_symbol_fragmented(const StreamId& stream_id, std::optional<size_t> segment_size) {
    auto update_info = get_latest_undeleted_version_and_next_version_id(
            store(), version_map(), stream_id);
    auto options = get_write_options();
    auto pre_defragmentation_info = get_pre_defragmentation_info(
        store(), stream_id, update_info, options, segment_size.value_or(options.segment_row_size));
    return is_symbol_fragmented_impl(pre_defragmentation_info.segments_need_compaction);
}

VersionedItem LocalVersionedEngine::defragment_symbol_data(const StreamId& stream_id, std::optional<size_t> segment_size, bool prune_previous_versions) {
    log::version().info("Defragmenting data for symbol {}", stream_id);

    // Currently defragmentation only for latest version - is there a use-case to allow compaction for older data?
    auto update_info = get_latest_undeleted_version_and_next_version_id(
        store(), version_map(), stream_id);

    auto options = get_write_options();
    auto versioned_item = defragment_symbol_data_impl(
            store(), stream_id, update_info, options,
            segment_size.has_value() ? *segment_size : options.segment_row_size);

    write_version_and_prune_previous(prune_previous_versions, versioned_item.key_, update_info.previous_index_key_);

    if(cfg_.symbol_list())
        symbol_list().add_symbol(store_, stream_id, versioned_item.key_.version_id());

    return versioned_item;
}

std::vector<ReadVersionOutput> LocalVersionedEngine::batch_read_keys(const std::vector<AtomKey> &keys) {
    auto handler_data = TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::PANDAS);
    py::gil_scoped_release release_gil;
    std::vector<folly::Future<ReadVersionOutput>> res;
    res.reserve(keys.size());
    for (const auto& index_key: keys) {
        res.emplace_back(read_frame_for_version(store(), {index_key}, std::make_shared<ReadQuery>(), ReadOptions{}, handler_data));
    }
    Allocator::instance()->trim();
    return folly::collect(res).get();
}

std::vector<std::variant<ReadVersionOutput, DataError>> LocalVersionedEngine::batch_read_internal(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries,
    std::vector<std::shared_ptr<ReadQuery>>& read_queries,
    const ReadOptions& read_options,
    std::any& handler_data) {
    py::gil_scoped_release release_gil;
    // This read option should always be set when calling batch_read
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(read_options.batch_throw_on_error().has_value(),
                                                    "ReadOptions::batch_throw_on_error_ should always be set here");
    auto opt_index_key_futs = batch_get_versions_async(store(), version_map(), stream_ids, version_queries);
    std::vector<folly::Future<ReadVersionOutput>> read_versions_futs;

    const auto max_batch_size = ConfigsMap::instance()->get_int("BatchRead.MaxConcurrency", 50);
    ARCTICDB_RUNTIME_DEBUG(log::inmem(), "Running batch read with a maximum concurrency of {}", max_batch_size);
    std::vector<folly::Try<ReadVersionOutput>> all_results;
    all_results.reserve(opt_index_key_futs.size());
    size_t batch_count = 0UL;
    for (auto idx = 0UL; idx < opt_index_key_futs.size(); ++idx) {
        read_versions_futs.emplace_back(
                std::move(opt_index_key_futs[idx]).thenValue([store = store(),
                                                        idx,
                                                        &stream_ids,
                                                        &version_queries,
                                                        read_query = read_queries.empty() ? std::make_shared<ReadQuery>(): read_queries[idx],
                                                        &read_options,
                                                        &handler_data](auto&& opt_index_key) {
                    std::variant<VersionedItem, StreamId> version_info;
                    if (opt_index_key.has_value()) {
                        version_info = VersionedItem(std::move(*opt_index_key));
                    } else {
                        if (opt_false(read_options.incompletes())) {
                            log::version().warn("No index: Key not found for {}, will attempt to use incomplete segments.", stream_ids[idx]);
                            version_info = stream_ids[idx];
                        } else {
                            missing_data::raise<ErrorCode::E_NO_SUCH_VERSION>(
                                    "batch_read_internal: version matching query '{}' not found for symbol '{}'", version_queries[idx], stream_ids[idx]);
                        }
                    }
                    return read_frame_for_version(store, version_info, read_query, read_options, handler_data);
                })
        );
        if(++batch_count == static_cast<size_t>(max_batch_size)) {
            auto read_versions = folly::collectAll(read_versions_futs).get();
            all_results.insert(all_results.end(), std::make_move_iterator(read_versions.begin()), std::make_move_iterator(read_versions.end()));
            read_versions_futs.clear();
            batch_count = 0UL;
        }
    }

    if(!read_versions_futs.empty()) {
        auto read_versions = folly::collectAll(read_versions_futs).get();
        all_results.insert(all_results.end(), std::make_move_iterator(read_versions.begin()), std::make_move_iterator(read_versions.end()));
    }

    TransformBatchResultsFlags flags;
    flags.convert_no_data_found_to_key_not_found_ = true;
    flags.throw_on_error_ = *read_options.batch_throw_on_error();
    return transform_batch_items_or_throw(std::move(all_results), stream_ids, flags, version_queries);
}

void LocalVersionedEngine::write_version_and_prune_previous(
        bool prune_previous_versions,
        const AtomKey& new_version,
        const std::optional<IndexTypeKey>& previous_key) {
    if (prune_previous_versions) {
        auto pruned_indexes = version_map()->write_and_prune_previous(store(), new_version, previous_key);
        delete_unreferenced_pruned_indexes(std::move(pruned_indexes), new_version).get();
    }
    else {
        version_map()->write_version(store(), new_version, previous_key);
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
        write_version_fut = async::submit_io_task(WriteVersionTask{store(), version_map, index_key, stream_update_info.previous_index_key_});
    }

    if(add_new_symbol){
        write_version_fut = std::move(write_version_fut)
        .then([this, index_key_id = index_key.id(), reference_id = index_key.version_id()](auto &&) {
            return async::submit_io_task(WriteSymbolTask(store(), symbol_list_ptr(), index_key_id, reference_id));
        });
    }
    return std::move(write_version_fut)
    .thenValue([index_key = std::move(index_key)](auto &&) mutable {
        return VersionedItem(std::move(index_key));
    });
}

std::vector<folly::Future<AtomKey>> LocalVersionedEngine::batch_write_internal(
    const std::vector<VersionId>& version_ids,
    const std::vector<StreamId>& stream_ids,
    std::vector<std::shared_ptr<pipelines::InputTensorFrame>>&& frames,
    const std::vector<std::shared_ptr<DeDupMap>>& de_dup_maps,
    bool validate_index
) {
    ARCTICDB_SAMPLE(WriteDataFrame, 0)
    ARCTICDB_DEBUG(log::version(), "Batch writing {} dataframes", stream_ids.size());
    std::vector<folly::Future<entity::AtomKey>> results_fut;
    for (size_t idx = 0; idx < stream_ids.size(); idx++) {
        results_fut.emplace_back(async_write_dataframe_impl(
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
    std::vector<std::shared_ptr<pipelines::InputTensorFrame>>&& frames,
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
                        frame,
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
    TransformBatchResultsFlags flags;
    flags.throw_on_error_ = throw_on_error;
    return transform_batch_items_or_throw(std::move(write_versions), stream_ids, flags);
}


VersionedItem LocalVersionedEngine::append_internal(
    const StreamId& stream_id,
    const std::shared_ptr<InputTensorFrame>& frame,
    bool upsert,
    bool prune_previous_versions,
    bool validate_index) {
    py::gil_scoped_release release_gil;
    auto update_info = get_latest_undeleted_version_and_next_version_id(store(),
                                                                        version_map(),
                                                                        stream_id);

    if(update_info.previous_index_key_.has_value()) {
        if (frame->empty()) {
            ARCTICDB_DEBUG(log::version(), "Appending an empty item to existing data has no effect. \n"
                                           "No new version has been created for symbol='{}', "
                                           "and the last version is returned", stream_id);
            return VersionedItem(*update_info.previous_index_key_);
        }
        auto versioned_item = append_impl(store(),
                                          update_info,
                                          frame,
                                          get_write_options(),
                                          validate_index,
                                          cfg().write_options().empty_types());
        write_version_and_prune_previous(
            prune_previous_versions, versioned_item.key_, update_info.previous_index_key_);
        return versioned_item;
    } else {
        if(upsert) {
            auto write_options = get_write_options();
            auto versioned_item =  write_dataframe_impl(store_,
                                                        update_info.next_version_id_,
                                                        frame,
                                                        write_options,
                                                        std::make_shared<DeDupMap>(),
                                                        false,
                                                        validate_index
                                                        );

            if(cfg_.symbol_list())
                symbol_list().add_symbol(store_, stream_id, update_info.next_version_id_);

            version_map()->write_version(store(), versioned_item.key_, std::nullopt);
            return versioned_item;
        } else {
            util::raise_rte( "Cannot append to non-existent symbol {}", stream_id);
        }
    }
}

std::vector<std::variant<VersionedItem, DataError>> LocalVersionedEngine::batch_append_internal(
    const std::vector<StreamId>& stream_ids,
    std::vector<std::shared_ptr<pipelines::InputTensorFrame>>&& frames,
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
                    index_key_fut = async_append_impl(store(), update_info, frame, write_options, validate_index, cfg().write_options().empty_types());
                } else {
                    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
                    upsert,
                    "Cannot append to non-existent symbol {}", stream_id);
                    auto version_id = 0;
                    auto de_dup_map = std::make_shared<DeDupMap>();
                    index_key_fut = async_write_dataframe_impl(store(), version_id, frame, write_options, de_dup_map, false, validate_index);
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
    TransformBatchResultsFlags flags;
    flags.throw_on_error_ = throw_on_error;
    return transform_batch_items_or_throw(std::move(append_versions), stream_ids, flags);
}

std::vector<std::variant<VersionedItem, DataError>> LocalVersionedEngine::batch_update_internal(
    const std::vector<StreamId>& stream_ids,
    std::vector<std::shared_ptr<InputTensorFrame>>&& frames,
    const std::vector<UpdateQuery>& update_queries,
    bool prune_previous_versions,
    bool upsert
) {
    py::gil_scoped_release release_gil;

    auto stream_update_info_futures = batch_get_latest_undeleted_version_and_next_version_id_async(store(), version_map(),stream_ids);
    std::vector<folly::Future<VersionedItem>> update_versions_futs;
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(stream_ids.size() == stream_update_info_futures.size(), "stream_ids and stream_update_info_futures must be of the same size");
    for (const auto&& [idx, stream_update_info_fut] : enumerate(stream_update_info_futures)) {
        update_versions_futs.push_back(
            std::move(stream_update_info_fut)
            .thenValue([this, frame = std::move(frames[idx]), stream_id = stream_ids[idx], update_query = update_queries[idx], upsert](auto&& update_info) {
                auto index_key_fut = folly::Future<AtomKey>::makeEmpty();
                auto write_options = get_write_options();
                if (update_info.previous_index_key_.has_value()) {
                    const bool dynamic_schema = cfg().write_options().dynamic_schema();
                    const bool empty_types = cfg().write_options().empty_types();
                    index_key_fut = async_update_impl(
                        store(),
                        update_info,
                        update_query,
                        std::move(frame),
                        std::move(write_options),
                        dynamic_schema,
                        empty_types);
                } else {
                    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
                        upsert,
                        "Cannot update non-existent symbol {}."
                        "Using \"upsert=True\" will create create the symbol instead of throwing this exception.",
                        stream_id);
                    index_key_fut = async_write_dataframe_impl(
                        store(),
                        0,
                        std::move(frame),
                        std::move(write_options),
                        std::make_shared<DeDupMap>(),
                        false,
                        true);
                }
                return std::move(index_key_fut).thenValueInline([update_info = std::move(update_info)](auto&& index_key) mutable {
                    return IndexKeyAndUpdateInfo{std::move(index_key), std::move(update_info)};
                });
            })
            .thenValue([this, prune_previous_versions](auto&& index_key_and_update_info) {
                auto&& [index_key, update_info] = index_key_and_update_info;
                return write_index_key_to_version_map_async(version_map(), std::move(index_key), std::move(update_info), prune_previous_versions);
            })
        );
    }

    auto update_versions = collectAll(update_versions_futs).get();
    return transform_batch_items_or_throw(std::move(update_versions), stream_ids, TransformBatchResultsFlags{});
}

struct WarnVersionTypeNotHandled {
    bool warned = false;

    void warn(const StreamId& stream_id) {
        if (!warned) {
            log::version().warn("Only exact version numbers are supported when using add_to_snapshot/batch_restore_version calls."
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
    auto previous = batch_get_latest_version(store(), version_map(), stream_ids, true);
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
        symbol_write_futs.emplace_back(async::submit_io_task(WriteSymbolTask(store(), symbol_list_ptr(), item.first.key_.id(), item.first.key_.version_id())));
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
    folly::Future<std::optional<AtomKey>>&& opt_index_key_fut,
    const StreamId& stream_id,
    const VersionQuery& version_query
    ) {
    return std::move(opt_index_key_fut)
    .thenValue([this, &stream_id, &version_query](std::optional<AtomKey>&& opt_index_key){
        missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(opt_index_key.has_value(),
        "Unable to retrieve  metadata. {}@{}: version not found", stream_id, version_query);
        return get_metadata(std::move(*opt_index_key));
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
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(read_options.batch_throw_on_error().has_value(),
                                                    "ReadOptions::batch_throw_on_error_ should always be set here");
    auto opt_index_key_futs = batch_get_versions_async(store(), version_map(), stream_ids, version_queries);
    std::vector<folly::Future<std::pair<VariantKey, std::optional<google::protobuf::Any>>>> metadata_futures;
    for (auto&& [idx, opt_index_key_fut]: folly::enumerate(opt_index_key_futs)) {
        metadata_futures.emplace_back(get_metadata_async(std::move(opt_index_key_fut), stream_ids[idx], version_queries[idx]));
    }

    auto metadatas = folly::collectAll(metadata_futures).get();
    // For legacy reason read_metadata_batch is not throwing if the symbol is missing
    TransformBatchResultsFlags flags;
    flags.throw_on_missing_symbol_ = false;
    flags.throw_on_error_ = *read_options.batch_throw_on_error();
    return transform_batch_items_or_throw(std::move(metadatas), stream_ids, flags, version_queries);
}

std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>> LocalVersionedEngine::read_metadata_internal(
    const StreamId& stream_id,
    const VersionQuery& version_query
    ) {
    auto version = get_version_to_read(stream_id, version_query);
    std::optional<AtomKey> key = version.has_value() ? std::make_optional<AtomKey>(version->key_) : std::nullopt;
    return get_metadata(std::move(key)).get();
}

VersionedItem LocalVersionedEngine::sort_merge_internal(
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    const CompactIncompleteOptions& options) {
    log::version().debug("Sort merge for symbol {} with options {}", stream_id, options);

    auto update_info = get_latest_undeleted_version_and_next_version_id(store(), version_map(), stream_id);
    if (update_info.previous_index_key_) {
        ARCTICDB_DEBUG(log::version(), "Found previous version for symbol {}", stream_id);
    } else {
        ARCTICDB_DEBUG(log::version(), "No previous version found for symbol {}", stream_id);
    };

    auto pipeline_context = std::make_shared<PipelineContext>();
    pipeline_context->stream_id_ = stream_id;
    pipeline_context->version_id_ = update_info.next_version_id_;
    auto delete_keys_on_failure = get_delete_keys_on_failure(pipeline_context, store(), options);

    auto versioned_item = sort_merge_impl(store_, stream_id, user_meta, update_info, options, get_write_options(), pipeline_context);
    ARCTICDB_DEBUG(log::version(), "Finished sort_merge_impl for symbol {}", stream_id);

    write_version_and_prune_previous(options.prune_previous_versions_, versioned_item.key_, update_info.previous_index_key_);
    ARCTICDB_DEBUG(log::version(), "Finished write_version_and_prune_previous for symbol {}", stream_id);

    add_to_symbol_list_on_compaction(stream_id, options, update_info);
    if (delete_keys_on_failure)
        delete_keys_on_failure->release();
    delete_incomplete_keys(*pipeline_context, *store());

    return versioned_item;
}

StorageLockWrapper LocalVersionedEngine::get_storage_lock(const StreamId& stream_id) {
    return StorageLockWrapper{stream_id, store_};
}

void LocalVersionedEngine::delete_storage(const bool continue_on_error) {
    delete_all(store_, continue_on_error);
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
        return *latest_incomplete;

    if(auto latest_key = get_latest_version(symbol); latest_key)
        return latest_key->key_.end_time();

    return -1;
}

std::vector<storage::ObjectSizes> LocalVersionedEngine::scan_object_sizes() {
    using ObjectSizes = storage::ObjectSizes;
    std::vector<folly::Future<ObjectSizes>> sizes_futs;
    foreach_key_type([&store=store(), &sizes=sizes_futs](KeyType key_type) {
        sizes.push_back(store->get_object_sizes(key_type, ""));
    });

    return folly::collect(sizes_futs).via(&async::cpu_executor()).get();
}

std::unordered_map<StreamId, std::unordered_map<KeyType, KeySizesInfo>> LocalVersionedEngine::scan_object_sizes_by_stream() {
    std::mutex mutex;
    std::unordered_map<StreamId, std::unordered_map<KeyType, KeySizesInfo>> sizes;
    auto streams = symbol_list().get_symbols(store());

    foreach_key_type([&store=store(), &sizes, &mutex](KeyType key_type) {
        stream::StreamSource::KeySizeCalculators key_size_calculators;

        store->iterate_type(key_type, [&key_size_calculators, &mutex, &sizes, key_type](const VariantKey&& k){
            key_size_calculators.emplace_back(std::forward<const VariantKey>(k), [key_type, &sizes, &mutex] (auto&& ks) {
                auto key_seg = std::forward<decltype(ks)>(ks);
                auto variant_key = key_seg.variant_key();
                auto stream_id = variant_key_id(variant_key);
                auto compressed_size = key_seg.segment().size();
                auto desc = key_seg.segment().descriptor();
                auto uncompressed_size = desc.uncompressed_bytes();

                {
                    std::lock_guard lock{mutex};
                    auto& sizes_info = sizes[stream_id][key_type];
                    sizes_info.count++;
                    sizes_info.compressed_size += compressed_size;
                    sizes_info.uncompressed_size += uncompressed_size;
                }

                return variant_key;
            });

        });

        store->read_ignoring_key_not_found(std::move(key_size_calculators));
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
