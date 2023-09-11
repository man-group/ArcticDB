/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/version_store_api.hpp>

#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/timer.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/util/ranges_from_future.hpp>

#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/entity/descriptor_item.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/util/optional_defaults.hpp>
#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/version/version_tasks.hpp>
#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/pipeline/pipeline_utils.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>

#include <regex>

namespace arcticdb::version_store {

using namespace arcticdb::entity;
namespace as = arcticdb::stream;
using namespace arcticdb::storage;

template PythonVersionStore::PythonVersionStore(const std::shared_ptr<storage::Library>& library, const util::SysClock& ct);
template PythonVersionStore::PythonVersionStore(const std::shared_ptr<storage::Library>& library, const util::ManualClock& ct);

VersionedItem PythonVersionStore::write_dataframe_specific_version(
    const StreamId& stream_id,
    const py::tuple& item,
    const py::object& norm,
    const py::object& user_meta,
    VersionId version_id
    ) {
    ARCTICDB_SAMPLE(WriteDataFrame, 0)

    ARCTICDB_DEBUG(log::version(), "write_dataframe_specific_version stream_id: {} , version_id: {}", stream_id, version_id);
    if (auto version_key = ::arcticdb::get_specific_version(store(), version_map(), stream_id, version_id, VersionQuery{}, ReadOptions{}); version_key) {
        log::version().warn("Symbol stream_id: {} already exists with version_id: {}", stream_id, version_id);
        return {std::move(version_key.value())};
    }

    auto versioned_item = write_dataframe_impl(
        store(),
        VersionId(version_id),
        convert::py_ndf_to_frame(stream_id, item, norm, user_meta),
        get_write_options());

    version_map()->write_version(store(), versioned_item.key_);
    if(cfg().symbol_list())
        symbol_list().add_symbol(store(), stream_id);

    return versioned_item;
}

std::vector<InputTensorFrame> create_input_tensor_frames(
    const std::vector<StreamId>& stream_ids,
    const std::vector<py::tuple> &items,
    const std::vector<py::object> &norms,
    const std::vector<py::object> &user_metas) {
    std::vector<InputTensorFrame> output;
    output.reserve(stream_ids.size());
    for (size_t idx = 0; idx < stream_ids.size(); idx++) {
        output.push_back(convert::py_ndf_to_frame(stream_ids[idx], items[idx], norms[idx], user_metas[idx]));
    }
    return output;
}

std::vector<VersionedItem> PythonVersionStore::batch_write_index_keys_to_version_map(
    const std::vector<AtomKey>& index_keys,
    const std::vector<UpdateInfo>& stream_update_info_vector,
    bool prune_previous_versions) {

    folly::collect(batch_write_version_and_prune_if_needed(index_keys, stream_update_info_vector, prune_previous_versions)).get();
    std::vector<VersionedItem> output(index_keys.size());
    for(auto key : folly::enumerate(index_keys))
        output[key.index] = *key;

    std::vector<folly::Future<folly::Unit>> symbol_write_futs;
    for(const auto& item : output) {
        symbol_write_futs.emplace_back(async::submit_io_task(WriteSymbolTask(store(), symbol_list_ptr(), item.key_.id())));
    }
    folly::collect(symbol_write_futs).wait();

    return output;
}

std::vector<std::variant<VersionedItem, DataError>> PythonVersionStore::batch_write(
    const std::vector<StreamId>& stream_ids,
    const std::vector<py::tuple> &items,
    const std::vector<py::object> &norms,
    const std::vector<py::object> &user_metas,
    bool prune_previous_versions,
    bool validate_index,
    bool throw_on_error) {

    auto frames = create_input_tensor_frames(stream_ids, items, norms, user_metas);
    return batch_write_versioned_dataframe_internal(stream_ids, std::move(frames), prune_previous_versions, validate_index, throw_on_error);
}

std::vector<std::variant<VersionedItem, DataError>> PythonVersionStore::batch_append(
    const std::vector<StreamId> &stream_ids,
    const std::vector<py::tuple> &items,
    const std::vector<py::object> &norms,
    const std::vector<py::object> &user_metas,
    bool prune_previous_versions,
    bool validate_index,
    bool upsert,
    bool throw_on_error) {
    auto frames = create_input_tensor_frames(stream_ids, items, norms, user_metas);
    return batch_append_internal(stream_ids, std::move(frames), prune_previous_versions, validate_index, upsert, throw_on_error);
}

void PythonVersionStore::_clear_symbol_list_keys() {
    symbol_list().clear(store());
}

void PythonVersionStore::reload_symbol_list() {
    symbol_list().clear(store());
    symbol_list().load(store(), false);
}

// To be sorted on timestamp
using VersionResult = std::tuple<StreamId, VersionId, timestamp, std::vector<SnapshotId>, bool>;
struct VersionComp {
    bool operator() (const VersionResult& v1, const VersionResult& v2) const {
        return std::tie(std::get<0>(v1), std::get<2>(v1)) >
        std::tie(std::get<0>(v2), std::get<2>(v2));
    }
};

using SymbolVersionToSnapshotMap = std::unordered_map<std::pair<StreamId, VersionId>, std::vector<SnapshotId>>;
using SymbolVersionTimestampMap = std::unordered_map<std::pair<StreamId, VersionId>, timestamp>;

using VersionResultVector = std::vector<VersionResult>;

VersionResultVector list_versions_for_snapshot(
    const std::set<StreamId>& stream_ids,
    std::optional<SnapshotId> snap_name,
    SnapshotMap& versions_for_snapshots,
    SymbolVersionToSnapshotMap& snapshots_for_symbol) {

    VersionResultVector res;
    util::check(versions_for_snapshots.count(snap_name.value()) != 0, "Snapshot not found");
    std::unordered_map<StreamId, AtomKey> version_for_stream_in_snapshot;
    for (const auto& key_in_snap: versions_for_snapshots[snap_name.value()]) {
        util::check(version_for_stream_in_snapshot.count(key_in_snap.id()) == 0,
                    "More than 1 version found for a symbol in snap");
        version_for_stream_in_snapshot[key_in_snap.id()] = key_in_snap;
    }

    for (auto &s_id: stream_ids) {
        // Return only those versions which are in the snapshot
        const auto& version_key = version_for_stream_in_snapshot[s_id];

        res.emplace_back(
            s_id,
            version_key.version_id(),
            version_key.creation_ts(),
            snapshots_for_symbol[{s_id, version_key.version_id()}],
            false);
    }

    std::sort(res.begin(), res.end(), VersionComp());
    return res;
}

void get_snapshot_version_info(
    const std::shared_ptr<Store>& store,
    SymbolVersionToSnapshotMap& snapshots_for_symbol,
    SymbolVersionTimestampMap& creation_ts_for_version_symbol,
    std::optional<SnapshotMap>& versions_for_snapshots) {
    // We will need to construct this map even if we are getting symbols for one snapshot
    // The symbols might appear in more than 1 snapshot and "snapshots" needs to be populated
    // After SNAPSHOT_REF key introduction, this operation is no longer slow
    versions_for_snapshots = get_versions_from_snapshots(store);

    for (const auto &[snap_id, index_keys]: *versions_for_snapshots) {
        for (const auto &index_key: index_keys) {
            snapshots_for_symbol[{index_key.id(), index_key.version_id()}].push_back(snap_id);
            creation_ts_for_version_symbol[{index_key.id(), index_key.version_id()}] = index_key.creation_ts();
        }
    }

    for(auto& [sid, version_vector]  : snapshots_for_symbol)
        std::sort(std::begin(version_vector), std::end(version_vector));
}


VersionResultVector get_latest_versions_for_symbols(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<VersionMap>& version_map,
    const std::set<StreamId>& stream_ids,
    SymbolVersionToSnapshotMap& snapshots_for_symbol,
    const VersionQuery& version_query
) {
    VersionResultVector res;
    std::unordered_set<std::pair<StreamId, VersionId>> unpruned_versions;
    for (auto &s_id: stream_ids) {
            const auto& opt_version_key = get_latest_undeleted_version(store, version_map, s_id, version_query, ReadOptions{});
            if (opt_version_key) {
                res.emplace_back(
                    s_id,
                    opt_version_key->version_id(),
                    opt_version_key->creation_ts(),
                    snapshots_for_symbol[{s_id, opt_version_key->version_id()}],
                    false);
            }
    }
    std::sort(res.begin(), res.end(), VersionComp());
    return res;
}


VersionResultVector get_all_versions_for_symbols(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<VersionMap>& version_map,
    const std::set<StreamId>& stream_ids,
    SymbolVersionToSnapshotMap& snapshots_for_symbol,
    const SymbolVersionTimestampMap& creation_ts_for_version_symbol
    ) {
    VersionResultVector res;
    std::unordered_set<std::pair<StreamId, VersionId>> unpruned_versions;
    for (auto &s_id: stream_ids) {
            auto all_versions = get_all_versions(store, version_map, s_id, VersionQuery{}, ReadOptions{});
            unpruned_versions = {};
            for (const auto &entry: all_versions) {
                unpruned_versions.emplace(s_id, entry.version_id());
                res.emplace_back(
                    s_id,
                    entry.version_id(),
                    entry.creation_ts(),
                    snapshots_for_symbol[{s_id, entry.version_id()}],
                    false);
            }
            for (const auto &[sym_version, creation_ts]: creation_ts_for_version_symbol) {
                // For all symbol, version combinations in snapshots, check if they have been pruned, and if so
                // use the information from the snapshot indexes and set deleted to true.
                if (sym_version.first == s_id && unpruned_versions.find(sym_version) == std::end(unpruned_versions)) {
                    res.emplace_back(
                        sym_version.first,
                        sym_version.second,
                        creation_ts,
                        snapshots_for_symbol[sym_version],
                        true);
                }
            }
    }
    std::sort(res.begin(), res.end(), VersionComp());
    return res;
}

VersionResultVector PythonVersionStore::list_versions(
    const std::optional<StreamId> &stream_id,
    const std::optional<SnapshotId> &snap_name,
    const std::optional<bool>& latest_only,
    const std::optional<bool>& iterate_on_failure,
    const std::optional<bool>& skip_snapshots) {
    ARCTICDB_SAMPLE(ListVersions, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: list_versions");
    VersionResultVector res;
    auto stream_ids = std::set<StreamId>();

    if (stream_id) {
        stream_ids.insert(stream_id.value());
    } else {
        stream_ids = list_streams(snap_name);
    }

    const bool do_snapshots = !opt_false(skip_snapshots) || snap_name;

    SymbolVersionToSnapshotMap snapshots_for_symbol;
    SymbolVersionTimestampMap creation_ts_for_version_symbol;
    std::optional<SnapshotMap> versions_for_snapshots;
    if(do_snapshots) {
        get_snapshot_version_info(store(), snapshots_for_symbol, creation_ts_for_version_symbol, versions_for_snapshots);

        if (snap_name)
            return list_versions_for_snapshot(stream_ids, snap_name, *versions_for_snapshots, snapshots_for_symbol);
    }

    VersionQuery version_query;
    version_query.set_iterate_on_failure(iterate_on_failure);

   if(opt_false(latest_only))
       return get_latest_versions_for_symbols(store(), version_map(), stream_ids, snapshots_for_symbol, version_query);
   else
       return get_all_versions_for_symbols(store(), version_map(), stream_ids, snapshots_for_symbol, creation_ts_for_version_symbol);
}

std::vector<std::pair<SnapshotId, py::object>> PythonVersionStore::list_snapshots(const std::optional<bool> load_metadata) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: list_snapshots");
    auto snap_ids = std::vector<std::pair<SnapshotId, py::object>>();
    auto fetch_metadata = opt_false(load_metadata);
    iterate_snapshots(store(), [store=store(), &snap_ids, fetch_metadata](VariantKey &vk) {
        auto snapshot_meta_as_pyobject = fetch_metadata ? get_metadata_for_snapshot(store, vk) : py::none{};
        auto snapshot_id = fmt::format("{}", variant_key_id(vk));
        snap_ids.emplace_back(snapshot_id, snapshot_meta_as_pyobject);
    });

    return snap_ids;
}

void PythonVersionStore::add_to_snapshot(
    const SnapshotId& snap_name,
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries
    ) {
    util::check(version_queries.empty() || stream_ids.size() == version_queries.size(), "List length mismatch in add_to_snapshot: {} != {}", stream_ids.size(), version_queries.size());
    auto opt_snapshot =  get_snapshot(store(), snap_name);
    if (!opt_snapshot) {
        throw NoDataFoundException(snap_name);
    }
    auto [snap_key, snap_segment] = opt_snapshot.value();
    auto [snapshot_contents, user_meta] = get_versions_and_metadata_from_snapshot(store(), snap_key);
    auto [specific_versions_index_map, latest_versions_index_map] = get_stream_index_map(stream_ids, version_queries);
    for(const auto& latest_version : *latest_versions_index_map) {
        specific_versions_index_map->try_emplace(std::make_pair(latest_version.first, latest_version.second.version_id()), latest_version.second);
    }

    auto missing = filter_keys_on_existence(
            utils::copy_of_values_as<VariantKey>(*specific_versions_index_map), store(), false);
    util::check(missing.empty(), "Cannot snapshot version(s) that have been deleted: {}", missing);

    std::vector<AtomKey> deleted_keys;
    std::vector<AtomKey> retained_keys;
    std::unordered_set<StreamId> affected_keys;
    for(const auto& [id_version, key] : *specific_versions_index_map) {
        auto [it, inserted] = affected_keys.insert(id_version.first);
        util::check(inserted, "Multiple elements in add_to_snapshot with key {}", id_version.first);
    }

    for(auto&& key : snapshot_contents) {
        auto new_version = affected_keys.find(key.id());
        if(new_version == std::end(affected_keys)) {
            retained_keys.emplace_back(std::move(key));
        } else {
            deleted_keys.emplace_back(std::move(key));
        }
    }

    for(auto&& [id, key] : *specific_versions_index_map)
        retained_keys.emplace_back(std::move(key));

    std::sort(std::begin(retained_keys), std::end(retained_keys));
    if(variant_key_type(snap_key) == KeyType::SNAPSHOT_REF && cfg().write_options().delayed_deletes()) {
        tombstone_snapshot(store(), to_ref(snap_key), std::move(snap_segment), version_map()->log_changes());
    } else {
        delete_trees_responsibly(deleted_keys, get_master_snapshots_map(store()), snap_name).get();
        if (version_map()->log_changes()) {
            log_delete_snapshot(store(), snap_name);
        }
    }
    write_snapshot_entry(store(), retained_keys, snap_name, user_meta, version_map()->log_changes());
}

void PythonVersionStore::remove_from_snapshot(
    const SnapshotId& snap_name,
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionId>& version_ids
    ) {
    util::check(stream_ids.size() == version_ids.size(), "List length mismatch in remove_from_snapshot: {} != {}", stream_ids.size(), version_ids.size());

    auto opt_snapshot =  get_snapshot(store(), snap_name);
    if (!opt_snapshot) {
        throw NoDataFoundException(snap_name);
    }
    auto [snap_key, snap_segment] = opt_snapshot.value();
    auto [snapshot_contents, user_meta] = get_versions_and_metadata_from_snapshot(store(), snap_key);

    using SymbolVersion = std::pair<StreamId, VersionId>;
    std::unordered_set<SymbolVersion> symbol_versions;
    for(auto i = 0u; i < stream_ids.size(); ++i) {
        symbol_versions.emplace(stream_ids[i], version_ids[i]);
    }

    std::vector<AtomKey> deleted_keys;
    std::vector<AtomKey> retained_keys;
    for(auto&& key : snapshot_contents) {
        if(symbol_versions.find(SymbolVersion{key.id(), key.version_id()}) == symbol_versions.end()) {
            retained_keys.emplace_back(std::move(key));
        } else {
            deleted_keys.emplace_back(std::move(key));
        }
    }

    if(variant_key_type(snap_key) == KeyType::SNAPSHOT_REF && cfg().write_options().delayed_deletes()) {
        tombstone_snapshot(store(), to_ref(snap_key), std::move(snap_segment), version_map()->log_changes());
    } else {
        delete_trees_responsibly(deleted_keys, get_master_snapshots_map(store()), snap_name).get();
        if (version_map()->log_changes()) {
            log_delete_snapshot(store(), snap_name);
        }
    }
    write_snapshot_entry(store(), retained_keys, snap_name, user_meta, version_map()->log_changes());
}

void PythonVersionStore::snapshot(
    const SnapshotId &snap_name,
    const py::object &user_meta,
    const std::vector<StreamId> &skip_symbols,
    std::map<StreamId, VersionId> &versions
    ) {
    ARCTICDB_SAMPLE(CreateSnapshot, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: snapshot");

    util::check_arg(skip_symbols.empty() || versions.empty(), "Only one of skip_symbols and versions can be set");

    //Explicitly set logging to error prior to testing if snapshot already exists as otherwise we're guaranteed to
    //see a warning message for all new snapshots (as the key doesn't exist!)
    spdlog::logger& logger = log::storage();
    auto current_level = logger.level();
    logger.set_level(spdlog::level::err);
    auto val = get_snapshot_key(store(), snap_name).has_value();
    logger.set_level(current_level);
    
    util::check(!val, "Snapshot with name {} already exists", snap_name);

    auto index_keys = std::vector<AtomKey>();
    if (versions.empty()) {
        // User did not provide explicit versions to snapshot, get latest of all filtered symbols.
        auto skip_symbols_set = std::set<StreamId>(skip_symbols.begin(), skip_symbols.end());
        auto all_symbols = list_streams();
        std::vector<StreamId> filtered_symbols;
        std::set_difference(all_symbols.begin(), all_symbols.end(), skip_symbols_set.begin(),
                skip_symbols_set.end(), std::back_inserter(filtered_symbols));

        if (filtered_symbols.empty()) {
            log::version().warn("No valid symbols in the library, skipping creation for snapshot: {}", snap_name);
            return;
        }

        auto sym_index_map = batch_get_latest_version(store(), version_map(), filtered_symbols, false);
        index_keys = utils::values(*sym_index_map);
    } else {
        auto sym_index_map = batch_get_specific_version(store(), version_map(), versions);
        index_keys = utils::values(*sym_index_map);
        auto missing = filter_keys_on_existence(
                utils::copy_of_values_as<VariantKey>(*sym_index_map), store(), false);
        util::check(missing.empty(), "Cannot snapshot version(s) that have been deleted: {}", missing);
    }

    ARCTICDB_DEBUG(log::version(), "Total Index keys in snapshot={}", index_keys.size());
    write_snapshot_entry(store(), index_keys, snap_name, user_meta, version_map()->log_changes());
}

std::set<StreamId> PythonVersionStore::list_streams(
    const std::optional<SnapshotId>& snap_name,
    const std::optional<std::string>& regex,
    const std::optional<std::string>& prefix,
    const std::optional<bool>& opt_use_symbol_list,
    const std::optional<bool>& opt_all_symbols

    ) {
    return list_streams_internal(snap_name, regex, prefix, opt_use_symbol_list, opt_all_symbols);
}

VersionedItem PythonVersionStore::write_partitioned_dataframe(
    const StreamId& stream_id,
    const py::tuple &item,
    const py::object &norm_meta,
    const std::vector<std::string>& partition_value
    ) {
    ARCTICDB_SAMPLE(WritePartitionedDataFrame, 0)
    auto maybe_prev = ::arcticdb::get_latest_version(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
    auto version_id = get_next_version_from_key(maybe_prev);

    //    TODO: We are not actually partitioning stuff atm, just assuming a single partition is passed for now.
    std::vector<py::object> partitioned_dfs;
    partitioned_dfs.push_back(item);

    auto write_options = get_write_options();
    auto de_dup_map = std::make_shared<DeDupMap>();

    std::vector<entity::AtomKey> index_keys;
    for (size_t idx = 0; idx < partitioned_dfs.size(); idx++) {
        auto subkeyname = fmt::format("{}-{}", stream_id, partition_value[idx]);
        auto versioned_item = write_dataframe_impl(
            store(),
            version_id,
            convert::py_ndf_to_frame(subkeyname, partitioned_dfs[idx], norm_meta, py::none()),
            write_options,
            de_dup_map,
            false);
        index_keys.emplace_back(versioned_item.key_);
    }

    folly::Future<VariantKey> multi_key_fut = folly::Future<VariantKey>::makeEmpty();
    IndexAggregator <RowCountIndex> multi_index_agg(stream_id, [&stream_id, version_id, &multi_key_fut, store=store()](auto &&segment) {
        multi_key_fut = store->write(KeyType::PARTITION,
                                     version_id,  // version_id
                                     stream_id,
                                     0,  // start_index
                                     0,  // end_index
                                     std::forward<decltype(segment)>(segment)).wait();
    });

    for (const auto& index_key: index_keys) {
        multi_index_agg.add_key(index_key);
    }

    multi_index_agg.commit();
    return {to_atom(std::move(multi_key_fut).get())};
    //    TODO: now store this in the version key for this symbol
}

VersionedItem PythonVersionStore::write_versioned_composite_data(
    const StreamId& stream_id,
    const py::object &metastruct,
    const std::vector<StreamId> &sub_keys,
    const std::vector<py::tuple> &items,
    const std::vector<py::object> &norm_metas,
    const py::object &user_meta,
    bool prune_previous_versions
    ) {
    ARCTICDB_SAMPLE(WriteVersionedMultiKey, 0)

    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write_versioned_composite_data");
    auto maybe_prev = ::arcticdb::get_latest_version(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
    auto version_id = get_next_version_from_key(maybe_prev);
    ARCTICDB_DEBUG(log::version(), "write_versioned_composite_data for stream_id: {} , version_id = {}", stream_id, version_id);
    // TODO: Assuming each sub key is always going to have the same version attached to it.
    std::vector<VersionId> version_ids;
    std::vector<py::object> user_metas;

    auto write_options = get_write_options();
    auto de_dup_map = get_de_dup_map(stream_id, maybe_prev, write_options);
    std::vector<std::shared_ptr<DeDupMap>> de_dup_maps;
    for (auto i = 0u; i < sub_keys.size(); ++i) {
        version_ids.emplace_back(version_id);
        user_metas.emplace_back(py::none());
        de_dup_maps.emplace_back(de_dup_map);
    }

    auto frames = create_input_tensor_frames(sub_keys, items, norm_metas, user_metas);
    auto index_keys = folly::collect(batch_write_internal(std::move(version_ids), sub_keys, std::move(frames), std::move(de_dup_maps), false)).get();
    auto multi_key = write_multi_index_entry(store(), index_keys, stream_id, metastruct, user_meta, version_id);
    auto versioned_item = VersionedItem(to_atom(std::move(multi_key)));
    write_version_and_prune_previous_if_needed(prune_previous_versions, versioned_item.key_, maybe_prev);

    if(cfg().symbol_list())
        symbol_list().add_symbol(store(), stream_id);

    return versioned_item;
}

VersionedItem PythonVersionStore::write_versioned_dataframe(
    const StreamId& stream_id,
    const py::tuple& item,
    const py::object& norm,
    const py::object& user_meta,
    bool prune_previous_versions,
    bool sparsify_floats,
    bool validate_index) {
    ARCTICDB_SAMPLE(WriteVersionedDataframe, 0)
    auto frame = convert::py_ndf_to_frame(stream_id, item, norm, user_meta);
    auto versioned_item = write_versioned_dataframe_internal(stream_id, std::move(frame), prune_previous_versions, sparsify_floats, validate_index);

    if(cfg().symbol_list())
        symbol_list().add_symbol(store(), stream_id);

    return versioned_item;
}

VersionedItem PythonVersionStore::append(
    const StreamId& stream_id,
    const py::tuple &item,
    const py::object &norm,
    const py::object & user_meta,
    bool upsert,
    bool prune_previous_versions,
    bool validate_index) {
    return append_internal(stream_id, convert::py_ndf_to_frame(stream_id, item, norm, user_meta), upsert,
                           prune_previous_versions, validate_index);
}

VersionedItem PythonVersionStore::update(
        const StreamId &stream_id,
        const UpdateQuery &query,
        const py::tuple &item,
        const py::object &norm,
        const py::object &user_meta,
        bool upsert,
        bool dynamic_schema,
        bool prune_previous_versions) {
    return update_internal(stream_id, query,
                           convert::py_ndf_to_frame(stream_id, item, norm, user_meta), upsert,
                           dynamic_schema, prune_previous_versions);
}

VersionedItem PythonVersionStore::delete_range(
    const StreamId& stream_id,
    const UpdateQuery& query,
    bool dynamic_schema) {
    return delete_range_internal(stream_id, query, dynamic_schema);
}

void PythonVersionStore::append_incomplete(
    const StreamId& stream_id,
    const py::tuple &item,
    const py::object &norm,
    const py::object & user_meta) const {

    using namespace arcticdb::entity;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    // Turn the input into a standardised frame object
    InputTensorFrame frame = convert::py_ndf_to_frame(stream_id, item, norm, user_meta);
    append_incomplete_frame(stream_id, std::move(frame));
}

VersionedItem PythonVersionStore::write_metadata(
    const StreamId& stream_id,
    const py::object & user_meta,
    bool prune_previous_versions) {
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
    python_util::pb_from_python(user_meta, user_meta_proto);
    return write_versioned_metadata_internal(stream_id, prune_previous_versions, std::move(user_meta_proto));
}

void PythonVersionStore::create_column_stats_version(
    const StreamId& stream_id,
    ColumnStats& column_stats,
    const VersionQuery& version_query) {
    ReadOptions read_options;
    read_options.set_dynamic_schema(cfg().write_options().dynamic_schema());
    create_column_stats_version_internal(stream_id, column_stats, version_query, read_options);
}

void PythonVersionStore::drop_column_stats_version(
    const StreamId& stream_id,
    const std::optional<ColumnStats>& column_stats_to_drop,
    const VersionQuery& version_query) {
    drop_column_stats_version_internal(stream_id, column_stats_to_drop, version_query);
}

ReadResult PythonVersionStore::read_column_stats_version(
    const StreamId& stream_id,
    const VersionQuery& version_query) {
    ARCTICDB_SAMPLE(ReadColumnStats, 0)
    auto [versioned_item, frame_and_descriptor] = read_column_stats_version_internal(stream_id, version_query);
    return make_read_result_from_frame(frame_and_descriptor, versioned_item.key_);
}

ColumnStats PythonVersionStore::get_column_stats_info_version(
    const StreamId& stream_id,
    const VersionQuery& version_query) {
    ARCTICDB_SAMPLE(GetColumnStatsInfo, 0)
    return get_column_stats_info_version_internal(stream_id, version_query);
}

VersionedItem PythonVersionStore::compact_incomplete(
        const StreamId& stream_id,
        bool append,
        bool convert_int_to_float,
        bool via_iteration /*= true */,
        bool sparsify /*= false */,
        const std::optional<py::object>& user_meta /* = std::nullopt */,
        bool prune_previous_versions) {
    std::optional<arcticdb::proto::descriptors::UserDefinedMetadata> meta;
    if (user_meta && !user_meta->is_none()) {
        meta = std::make_optional<arcticdb::proto::descriptors::UserDefinedMetadata>();
        python_util::pb_from_python(*user_meta, *meta);
    }
    return compact_incomplete_dynamic(stream_id, meta, append, convert_int_to_float, via_iteration, sparsify,
        prune_previous_versions);
}

VersionedItem PythonVersionStore::sort_merge(
        const StreamId& stream_id,
        const py::object& user_meta,
        bool append,
        bool convert_int_to_float,
        bool via_iteration,
        bool sparsify
) {
    std::optional<arcticdb::proto::descriptors::UserDefinedMetadata> meta;
    if (!user_meta.is_none()) {
        meta = std::make_optional<arcticdb::proto::descriptors::UserDefinedMetadata>();
        python_util::pb_from_python(user_meta, *meta);
    }
    return sort_merge_internal(stream_id, meta, append, convert_int_to_float, via_iteration, sparsify);
}

void PythonVersionStore::write_parallel(
    const StreamId& stream_id,
    const py::tuple &item,
    const py::object &norm,
    const py::object & user_meta) const {
    using namespace arcticdb::entity;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    InputTensorFrame frame = convert::py_ndf_to_frame(stream_id, item, norm, user_meta);
    write_parallel_frame(stream_id, std::move(frame));
}

std::unordered_map<VersionId, bool> PythonVersionStore::get_all_tombstoned_versions(const StreamId &stream_id) {
    return ::arcticdb::get_all_tombstoned_versions(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
}

FrameAndDescriptor create_frame(const StreamId& target_id, SegmentInMemory seg, const ReadOptions& read_options) {
    auto context = std::make_shared<PipelineContext>(seg, in_memory_key(KeyType::TABLE_DATA, target_id, VersionId{0}));
    seg.unsparsify();
    reduce_and_fix_columns(context, seg, read_options);
    auto norm_meta = make_timeseries_norm_meta(target_id);
    const auto desc = make_timeseries_descriptor(seg.row_count(), seg.descriptor().clone(), std::move(norm_meta), std::nullopt, std::nullopt, std::nullopt, false);
    return FrameAndDescriptor{std::move(seg), desc, {}, {}};
}

ReadResult PythonVersionStore::read_dataframe_merged(
    const StreamId& target_id,
    const std::vector<StreamId> &stream_ids,
    const VersionQuery&, // TODO batch_get_specific_version
    const ReadQuery &query,
    const ReadOptions& read_options) {
    if (stream_ids.empty())
        util::raise_rte("No symbols given");

    auto stream_index_map = batch_get_latest_version(store(), version_map(), stream_ids, false);
    std::vector<AtomKey> index_keys;
    std::transform(stream_index_map->begin(), stream_index_map->end(), std::back_inserter(index_keys),
                   [](auto &stream_key) { return to_atom(stream_key.second); });

    if(stream_index_map->empty())
        throw NoDataFoundException("No data found for any symbols");

    auto [key, metadata, descriptor] = store()->read_metadata_and_descriptor(stream_index_map->begin()->second).get();
    auto index = index_type_from_descriptor(descriptor);

    FrameAndDescriptor final_frame;
    VariantColumnPolicy density_policy = read_options.allow_sparse_ ? VariantColumnPolicy{SparseColumnPolicy{}} : VariantColumnPolicy{DenseColumnPolicy{}};

    merge_frames_for_keys(
        target_id,
        std::move(index),
        NeverSegmentPolicy{},
        density_policy,
        index_keys,
        query,
        store(),
        [&final_frame, &target_id, &read_options](auto &&seg) {
            final_frame = create_frame(target_id, std::forward<decltype(seg)>(seg), read_options);
        });

    const auto version = VersionedItem{to_atom((*stream_index_map)[stream_ids[0]])};
    return create_python_read_result(version, std::move(final_frame));
}

std::vector<std::variant<ReadResult, DataError>> PythonVersionStore::batch_read(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries,
    std::vector<ReadQuery>& read_queries,
    const ReadOptions& read_options) {

    auto read_versions_or_errors = batch_read_internal(stream_ids, version_queries, read_queries, read_options);
    std::vector<std::variant<ReadResult, DataError>> res;
    for (auto&& [idx, read_version_or_error]: folly::enumerate(read_versions_or_errors)) {
        util::variant_match(
                read_version_or_error,
                [&res] (ReadVersionOutput& read_version) {
                    res.emplace_back(create_python_read_result(read_version.versioned_item_,
                                                               std::move(read_version.frame_and_descriptor_)));
                },
                [&res] (DataError& data_error) {
                    res.emplace_back(std::move(data_error));
                }
                );
    }
    return res;
}

ReadResult PythonVersionStore::read_dataframe_version(
    const StreamId &stream_id,
    const VersionQuery& version_query,
    ReadQuery& read_query,
    const ReadOptions& read_options) {
    auto opt_version_and_frame = read_dataframe_version_internal(stream_id, version_query, read_query, read_options);
    return create_python_read_result(opt_version_and_frame.versioned_item_, std::move(opt_version_and_frame.frame_and_descriptor_));
}

void PythonVersionStore::delete_snapshot(const SnapshotId& snap_name) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: delete_snapshot");
    auto opt_snapshot =  get_snapshot(store(), snap_name);
    if (!opt_snapshot) {
        throw NoDataFoundException(snap_name);
    }
    auto [snap_key, snap_segment] = opt_snapshot.value();

    if (variant_key_type(snap_key) == KeyType::SNAPSHOT_REF && cfg().write_options().delayed_deletes()) {
        ARCTICDB_DEBUG(log::version(), "Delaying deletion of Snapshot {}", snap_name);
        tombstone_snapshot(store(), to_ref(snap_key), std::move(snap_segment), version_map()->log_changes());
    } else {
        delete_snapshot_sync(snap_name, snap_key);
        if (version_map()->log_changes()) {
            log_delete_snapshot(store(), snap_name);
        }
    }
}

void PythonVersionStore::delete_snapshot_sync(const SnapshotId& snap_name, const VariantKey& snap_key) {
    ARCTICDB_DEBUG(log::version(), "Deleting data of Snapshot {}", snap_name);

    std::vector<AtomKey> index_keys_in_current_snapshot;
    auto snap_map = get_master_snapshots_map(
        store(),
        std::tie(snap_key, index_keys_in_current_snapshot));

    ARCTICDB_DEBUG(log::version(), "Deleting Snapshot {}", snap_name);
    store()->remove_key(snap_key).get();

    try {
        delete_trees_responsibly(
            index_keys_in_current_snapshot,
            snap_map,
            snap_name).get();
        ARCTICDB_DEBUG(log::version(), "Deleted orphaned index keys in snapshot {}", snap_name);
    } catch(const std::exception &ex) {
        log::version().warn("Garbage collection of unreachable deleted index keys failed due to: {}", ex.what());
    }
}

namespace {

std::vector<SnapshotVariantKey> ARCTICDB_UNUSED iterate_snapshot_tombstones (
    const std::string& limit_stream_id,
    std::set<IndexTypeKey>& candidates,
    const std::shared_ptr<Store>& store) {

    std::vector<SnapshotVariantKey> snap_tomb_keys;
    if (limit_stream_id.empty()) {
        store->iterate_type(KeyType::SNAPSHOT_TOMBSTONE, [&store, &candidates, &snap_tomb_keys](VariantKey&& snap_tomb_key) {
            ARCTICDB_DEBUG(log::version(), "Processing {}", snap_tomb_key);
            std::vector<IndexTypeKey> indexes{};
            auto snap_seg = store->read_sync(snap_tomb_key).second;
            auto before = candidates.size();

            for (size_t idx = 0; idx < snap_seg.row_count(); idx++) {
                auto key = read_key_row(snap_seg, static_cast<ssize_t>(idx));
                if (candidates.count(key) == 0) { // Snapshots often hold the same keys, so worthwhile optimisation
                    indexes.emplace_back(std::move(key));
                }
            }

            if (!indexes.empty()) {
                filter_keys_on_existence(indexes, store, true);
                candidates.insert(std::move_iterator(indexes.begin()), std::move_iterator(indexes.end()));
                indexes.clear();
            }

            log::version().info("Processed {} keys from snapshot {}. {} are unique.",
                                snap_seg.row_count(), variant_key_id(snap_tomb_key), candidates.size() - before);
            snap_tomb_keys.emplace_back(std::move(snap_tomb_key));
        });
    }
    return snap_tomb_keys;
}

} // namespace

void PythonVersionStore::delete_version(
    const StreamId& stream_id,
    const VersionId& version_id
    ) {

    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: delete_version");
    auto result = ::arcticdb::tombstone_version(store(), version_map(), stream_id, version_id, VersionQuery{}, ReadOptions{});

    if (!result.keys_to_delete.empty() && !cfg().write_options().delayed_deletes()) {
        delete_tree(result.keys_to_delete, result);
    }

    if(result.no_undeleted_left && cfg().symbol_list()) {
        symbol_list().remove_symbol(store(), stream_id);
    }
}

void PythonVersionStore::fix_symbol_trees(const std::vector<StreamId>& symbols) {
    auto snaps = get_master_snapshots_map(store());
    for (const auto& sym : symbols) {
        auto index_keys_from_symbol_tree = get_all_versions(store(), version_map(), sym, VersionQuery{}, ReadOptions{});
        for(const auto& [key, map] : snaps[sym]) {
            index_keys_from_symbol_tree.push_back(key);
        }
        std::sort(std::begin(index_keys_from_symbol_tree), std::end(index_keys_from_symbol_tree),
                  [&](const auto& k1, const auto& k2){return k1.version_id() > k2.version_id();});
        auto last = std::unique(std::begin(index_keys_from_symbol_tree), std::end(index_keys_from_symbol_tree),
                                [&](const auto& k1, const auto& k2){return k1.version_id() == k2.version_id();});
        index_keys_from_symbol_tree.erase(last, index_keys_from_symbol_tree.end());
        version_map()->overwrite_symbol_tree(store(), sym, index_keys_from_symbol_tree);
    }
}

void PythonVersionStore::prune_previous_versions(const StreamId& stream_id) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: prune_previous_versions stream_id={}", stream_id);
    const std::shared_ptr<VersionMapEntry>& entry = version_map()->check_reload(
            store(),
            stream_id,
            LoadParameter{LoadType::LOAD_UNDELETED},
            __FUNCTION__);
    storage::check<ErrorCode::E_SYMBOL_NOT_FOUND>(!entry->empty(), "Symbol {} is not found", stream_id);
    auto latest = entry->get_first_index(false);

    auto prev_id = get_prev_version_in_entry(entry, latest->version_id());
    if (!prev_id) {
        ARCTICDB_DEBUG(log::version(), "No previous versions to prune for stream_id={}", stream_id);
        return;
    }

    auto previous = ::arcticdb::get_specific_version(store(), version_map(), stream_id, prev_id.value(), VersionQuery{}, ReadOptions{});
    auto pruned_indexes = version_map()->tombstone_from_key_or_all(store(), stream_id, previous);
    delete_unreferenced_pruned_indexes(pruned_indexes, latest.value()).get();
}

void PythonVersionStore::delete_all_versions(const StreamId& stream_id) {
    ARCTICDB_SAMPLE(DeleteAllVersions, 0)

    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: delete_all_versions");
    if (!has_stream(stream_id)) {
        log::version().warn("Symbol: {} does not exist.", stream_id);
        return;
    }
    auto all_index_keys = version_map()->delete_all_versions(store(), stream_id);
    ARCTICDB_DEBUG(log::version(), "Version heads deleted for symbol {}. Proceeding with index keys total of {}", stream_id, all_index_keys.size());
    if (!cfg().write_options().delayed_deletes()) {
        delete_tree({all_index_keys.begin(), all_index_keys.end()});
    } else {
        ARCTICDB_DEBUG(log::version(), "Not deleting data for {}", stream_id);
    }

    if(cfg().symbol_list())
        symbol_list().remove_symbol(store(), stream_id);
    ARCTICDB_DEBUG(log::version(), "Delete of Symbol {} successful", stream_id);
}

std::vector<timestamp> PythonVersionStore::get_update_times(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries) {
    return batch_get_update_times(stream_ids, version_queries);
}

timestamp PythonVersionStore::get_update_time(
        const StreamId& stream_id,
        const VersionQuery& version_query) {
    return get_update_time_internal(stream_id, version_query);
}

namespace {
py::object metadata_protobuf_to_pyobject(std::optional<google::protobuf::Any> metadata_proto) {
    py::object pyobj;

    if (metadata_proto) {
        arcticdb::proto::descriptors::TimeSeriesDescriptor tsd;
        metadata_proto.value().UnpackTo(&tsd);
        pyobj = python_util::pb_to_python(tsd.user_meta());
    }
    else {
        pyobj = pybind11::none();
    }
    return pyobj;
}
}

std::pair<VersionedItem, py::object> PythonVersionStore::read_metadata(
    const StreamId& stream_id,
    const VersionQuery& version_query,
    const ReadOptions& read_options
    ) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: read_metadata");
    ARCTICDB_SAMPLE(ReadMetadata, 0)

    auto metadata = read_metadata_internal(stream_id, version_query, read_options);
    if(!metadata.first.has_value())
        throw NoDataFoundException(fmt::format("read_metadata: version not found for symbol", stream_id));

    auto metadata_proto = metadata.second;
    py::object pyobj = metadata_protobuf_to_pyobject(metadata_proto);
    VersionedItem version{std::move(to_atom(*metadata.first))};
    return std::pair{version, pyobj};
}

std::vector<std::variant<VersionedItem, DataError>> PythonVersionStore::batch_write_metadata(
    const std::vector<StreamId>& stream_ids,
    const std::vector<py::object>& user_meta,
    bool prune_previous_versions,
    bool throw_on_error) {
    std::vector<arcticdb::proto::descriptors::UserDefinedMetadata> user_meta_protos;
    user_meta_protos.reserve(user_meta.size());
    for(const auto& user_meta_item : user_meta) {
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        python_util::pb_from_python(user_meta_item, user_meta_proto);
        user_meta_protos.emplace_back(std::move(user_meta_proto));
    }
    return batch_write_versioned_metadata_internal(stream_ids, prune_previous_versions, throw_on_error, std::move(user_meta_protos));
}

std::vector<std::pair<VersionedItem, TimeseriesDescriptor>> PythonVersionStore::batch_restore_version(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries) {
    return batch_restore_version_internal(stream_ids, version_queries);
}

std::vector<std::variant<std::pair<VersionedItem, py::object>, DataError>> PythonVersionStore::batch_read_metadata(
    const std::vector<StreamId>& stream_ids,
    const std::vector<VersionQuery>& version_queries,
    const ReadOptions& read_options) {
    ARCTICDB_SAMPLE(BatchReadMetadata, 0)
    auto metadatas_or_errors = batch_read_metadata_internal(stream_ids, version_queries, read_options);

    std::vector<std::variant<std::pair<VersionedItem, py::object>, DataError>> results;
    for (auto&& metadata_or_error: metadatas_or_errors) {
        if (std::holds_alternative<std::pair<VariantKey, std::optional<google::protobuf::Any>>>(metadata_or_error)) {
            auto&& [key, meta_proto] = std::get<std::pair<VariantKey, std::optional<google::protobuf::Any>>>(metadata_or_error);
            VersionedItem version{std::move(to_atom(key))};
            if(meta_proto.has_value()) {
                auto res = std::make_pair(std::move(version), metadata_protobuf_to_pyobject(std::move(meta_proto)));
                results.push_back(std::move(res));
            }else{
                auto res = std::make_pair(std::move(version), py::none());
                results.push_back(std::move(res));   
            }
        } else {
            results.push_back(std::get<DataError>(std::move(metadata_or_error)));   
        }
    }
    return results;
}

DescriptorItem PythonVersionStore::read_descriptor(
    const StreamId& stream_id,
    const VersionQuery& version_query,
    const ReadOptions& read_options
    ) {
    return read_descriptor_internal(stream_id, version_query, read_options);
}

std::vector<std::variant<DescriptorItem, DataError>> PythonVersionStore::batch_read_descriptor(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries,
        const ReadOptions& read_options){
    
    return batch_read_descriptor_internal(stream_ids, version_queries, read_options);
}

ReadResult PythonVersionStore::read_index(
    const StreamId& stream_id,
    const VersionQuery& version_query
    ) {
    ARCTICDB_SAMPLE(ReadDescriptor, 0)

    py::object pyobj;
    auto version = get_version_to_read(stream_id, version_query, ReadOptions{});
    if(!version)
        throw NoDataFoundException(fmt::format("read_index: version not found for symbol '{}'", stream_id));

    auto res = read_index_impl(store(), version.value());
    return make_read_result_from_frame(res, version->key_);
}

std::vector<AtomKey> PythonVersionStore::get_version_history(const StreamId& stream_id) {
    return get_index_and_tombstone_keys(store(), version_map(), stream_id, VersionQuery{}, ReadOptions{});
}

void PythonVersionStore::_compact_version_map(const StreamId& id) {
    version_map()->compact(store(), id);
}

void PythonVersionStore::compact_library(size_t batch_size) {
    version_map()->compact_if_necessary_stand_alone(store(), batch_size);
}

std::vector<SliceAndKey> PythonVersionStore::list_incompletes(const StreamId& stream_id) {
    return get_incomplete(store(), stream_id, unspecified_range(), 0u, true, false);
}

void PythonVersionStore::clear() {
    if (store()->fast_delete()) {
        // Most storage backends have a fast deletion method for a db/collection equivalent, eg. drop() for mongo and
        // lmdb and iterating each key is always going to be suboptimal.
        ARCTICDB_DEBUG(log::version(), "fast deleting library as supported by storage");
        return;
    }

    delete_all(store(), true);
}

bool PythonVersionStore::empty() {
    // No good way to break out of these iterations, so use exception for flow control
    try {
        foreach_key_type([store=store()](KeyType key_type) {
            // Skip symbol list keys, as these can be generated by list_symbols calls even if the lib is empty
            if (key_type != KeyType::SYMBOL_LIST) {
                store->iterate_type(key_type, [](VariantKey&&) {
                    throw std::exception();
                });
            }
        });
    } catch (...) {
        return false;
    }
    return true;
}

void PythonVersionStore::force_delete_symbol(const StreamId& stream_id) {
    version_map()->delete_all_versions(store(), stream_id);
    delete_all_for_stream(store(), stream_id, true);
}

} //namespace arcticdb::version_store
