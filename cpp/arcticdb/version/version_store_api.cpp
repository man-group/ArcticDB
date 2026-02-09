/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/util/ranges_from_future.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/entity/descriptor_item.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/util/optional_defaults.hpp>
#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/pipeline/pipeline_utils.hpp>
#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/storage/file/file_store.hpp>
#include <arcticdb/version/version_functions.hpp>

namespace arcticdb::version_store {

using namespace arcticdb::entity;
namespace as = arcticdb::stream;
using namespace arcticdb::storage;

template PythonVersionStore::PythonVersionStore(
        const std::shared_ptr<storage::Library>& library, const util::SysClock& ct
);
template PythonVersionStore::PythonVersionStore(
        const std::shared_ptr<storage::Library>& library, const util::ManualClock& ct
);

VersionedItem PythonVersionStore::write_dataframe_specific_version(
        const StreamId& stream_id, const py::tuple& item, const py::object& norm, const py::object& user_meta,
        VersionId version_id
) {
    ARCTICDB_SAMPLE(WriteDataFrame, 0)

    ARCTICDB_DEBUG(
            log::version(), "write_dataframe_specific_version stream_id: {} , version_id: {}", stream_id, version_id
    );
    if (auto version_key = ::arcticdb::get_specific_version(store(), version_map(), stream_id, version_id);
        version_key) {
        log::version().warn("Symbol stream_id: {} already exists with version_id: {}", stream_id, version_id);
        return {std::move(*version_key)};
    }

    auto versioned_item = write_dataframe_impl(
            store(),
            VersionId(version_id),
            convert::py_ndf_to_frame(stream_id, item, norm, user_meta, cfg().write_options().empty_types()),
            get_write_options()
    );

    version_map()->write_version(store(), versioned_item.key_, std::nullopt);
    if (cfg().symbol_list())
        symbol_list().add_symbol(store(), stream_id, version_id);

    return versioned_item;
}

std::vector<std::shared_ptr<InputFrame>> create_input_tensor_frames(
        const std::vector<StreamId>& stream_ids, const std::vector<convert::InputItem>& items,
        const std::vector<py::object>& norms, const std::vector<py::object>& user_metas, bool empty_types
) {
    std::vector<std::shared_ptr<InputFrame>> output;
    output.reserve(stream_ids.size());
    for (size_t idx = 0; idx < stream_ids.size(); idx++) {
        output.emplace_back(
                convert::py_ndf_to_frame(stream_ids[idx], items[idx], norms[idx], user_metas[idx], empty_types)
        );
    }
    return output;
}

std::vector<std::variant<VersionedItem, DataError>> PythonVersionStore::batch_write(
        const std::vector<StreamId>& stream_ids, const std::vector<convert::InputItem>& items,
        const std::vector<py::object>& norms, const std::vector<py::object>& user_metas, bool prune_previous_versions,
        bool validate_index, bool throw_on_error
) {

    auto frames = create_input_tensor_frames(stream_ids, items, norms, user_metas, cfg().write_options().empty_types());
    return batch_write_versioned_dataframe_internal(
            stream_ids, std::move(frames), prune_previous_versions, validate_index, throw_on_error
    );
}

std::vector<std::variant<VersionedItem, DataError>> PythonVersionStore::batch_append(
        const std::vector<StreamId>& stream_ids, const std::vector<convert::InputItem>& items,
        const std::vector<py::object>& norms, const std::vector<py::object>& user_metas, bool prune_previous_versions,
        bool validate_index, bool upsert, bool throw_on_error
) {
    auto frames = create_input_tensor_frames(stream_ids, items, norms, user_metas, cfg().write_options().empty_types());
    return batch_append_internal(
            stream_ids, std::move(frames), prune_previous_versions, validate_index, upsert, throw_on_error
    );
}

void PythonVersionStore::_clear_symbol_list_keys() { symbol_list().clear(store()); }

void PythonVersionStore::reload_symbol_list() {
    symbol_list().clear(store());
    symbol_list().load<std::set<StreamId>>(version_map(), store(), false);
}

// To be sorted on timestamp
using VersionResult = std::tuple<StreamId, VersionId, timestamp, std::vector<SnapshotId>, bool>;
struct VersionComp {
    bool operator()(const VersionResult& v1, const VersionResult& v2) const {
        return std::tie(std::get<0>(v1), std::get<2>(v1)) > std::tie(std::get<0>(v2), std::get<2>(v2));
    }
};

struct SnapshotInfo {
    std::vector<SnapshotId> snapshots;
    timestamp index_creation_ts;
};

using SymbolVersionToSnapshotInfoMap = std::unordered_map<std::pair<StreamId, VersionId>, SnapshotInfo>;

using VersionResultVector = std::vector<VersionResult>;

VersionResultVector list_versions_for_snapshot_without_snapshot_list(
        const std::shared_ptr<Store>& store, const std::optional<StreamId>& stream_id, const SnapshotId& snap_name
) {
    const auto snap = get_snapshot(store, snap_name);
    if (!snap) {
        throw NoDataFoundException(fmt::format("Specified snapshot {} not found", snap_name));
    }
    const auto& seg = snap->second;
    VersionResultVector res;
    std::vector<SnapshotId> empty_snapshots;
    for (size_t idx = 0; idx < seg.row_count(); idx++) {
        auto stream_index = read_key_row(seg, static_cast<ssize_t>(idx));
        if (stream_id && stream_index.id() != *stream_id) {
            continue;
        }
        res.emplace_back(
                stream_index.id(), stream_index.version_id(), stream_index.creation_ts(), empty_snapshots, false
        );
    }

    std::sort(res.begin(), res.end(), VersionComp());
    return res;
}

VersionResultVector list_versions_for_snapshot_with_snapshot_list(
        const std::unordered_set<StreamId>& stream_ids, std::optional<SnapshotId> snap_name,
        SnapshotMap&& versions_for_snapshots, SymbolVersionToSnapshotInfoMap&& snapshots_for_symbol
) {

    VersionResultVector res;
    if (!versions_for_snapshots.contains(snap_name.value())) {
        throw NoDataFoundException(fmt::format("Snapshot {} not found", *snap_name));
    }

    std::unordered_map<StreamId, AtomKey> version_for_stream_in_snapshot;
    for (const auto& key_in_snap : versions_for_snapshots[snap_name.value()]) {
        util::check(
                version_for_stream_in_snapshot.count(key_in_snap.id()) == 0,
                "More than 1 version found for a symbol in snap"
        );
        version_for_stream_in_snapshot[key_in_snap.id()] = key_in_snap;
    }

    for (auto& s_id : stream_ids) {
        // Return only those versions which are in the snapshot
        const auto& version_key = version_for_stream_in_snapshot[s_id];

        res.emplace_back(
                s_id,
                version_key.version_id(),
                version_key.creation_ts(),
                std::move(snapshots_for_symbol[{s_id, version_key.version_id()}].snapshots),
                false
        );
    }

    std::sort(res.begin(), res.end(), VersionComp());
    return res;
}

void get_snapshot_version_info(
        const std::shared_ptr<Store>& store, SymbolVersionToSnapshotInfoMap& snapshots_for_symbol,
        std::optional<SnapshotMap>& versions_for_snapshots, const std::optional<StreamId>& stream_id
) {
    // We will need to construct this map even if we are getting symbols for one snapshot
    // The symbols might appear in more than 1 snapshot and "snapshots" needs to be populated
    // After SNAPSHOT_REF key introduction, this operation is no longer slow
    versions_for_snapshots = get_versions_from_snapshots(store, stream_id);

    for (const auto& [snap_id, index_keys] : *versions_for_snapshots) {
        for (const auto& index_key : index_keys) {
            auto& snapshot_info = snapshots_for_symbol[{index_key.id(), index_key.version_id()}];
            snapshot_info.snapshots.push_back(snap_id);
            snapshot_info.index_creation_ts = index_key.creation_ts();
        }
    }

    for (auto& [sid, snapshot_info] : snapshots_for_symbol)
        std::sort(std::begin(snapshot_info.snapshots), std::end(snapshot_info.snapshots));
}

VersionResultVector get_latest_versions_for_symbols(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        std::vector<StreamId>&& symbols, SymbolVersionToSnapshotInfoMap&& snapshots_for_symbol
) {
    const auto window_size = async::TaskScheduler::instance()->io_thread_count();
    // We are going to use folly::collect to short-circuit on any network errors, which means we need to keep everything
    // alive even after this function exits to avoid segfaults in workers that are still going after one worker raises
    auto snapshots_for_symbol_ptr = std::make_shared<SymbolVersionToSnapshotInfoMap>(std::move(snapshots_for_symbol));
    auto opt_version_result_futures = folly::window(
            std::move(symbols),
            [store, version_map, snapshots_for_symbol_ptr](StreamId&& stream_id) {
                return async::submit_io_task(CheckReloadTask{
                                                     store,
                                                     version_map,
                                                     stream_id,
                                                     LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY}
                                             }
                ).thenValueInline([stream_id, snapshots_for_symbol_ptr](auto&& version_map_entry) {
                    const auto& opt_version_key = version_map_entry->get_first_index(false).first;
                    if (opt_version_key) {
                        auto snapshots_it =
                                snapshots_for_symbol_ptr->find(std::make_pair(stream_id, opt_version_key->version_id())
                                );
                        auto snapshots = snapshots_it == snapshots_for_symbol_ptr->end()
                                                 ? std::vector<SnapshotId>()
                                                 : std::move(snapshots_it->second.snapshots);
                        return std::make_optional<VersionResult>(
                                stream_id,
                                opt_version_key->version_id(),
                                opt_version_key->creation_ts(),
                                std::move(snapshots),
                                false
                        );
                    } else {
                        return std::optional<VersionResult>();
                    }
                });
            },
            window_size
    );
    auto opt_version_results = folly::collect(std::move(opt_version_result_futures)).get();
    VersionResultVector res;
    res.reserve(opt_version_results.size());
    // opt_version_results are already in alphabetical order of symbols, so no sorting required
    for (auto&& opt_version_result : opt_version_results) {
        if (opt_version_result.has_value()) {
            res.emplace_back(std::move(opt_version_result.value()));
        }
    }
    return res;
}

VersionResultVector get_all_versions_for_symbols(
        const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMap>& version_map,
        std::vector<StreamId>&& symbols, SymbolVersionToSnapshotInfoMap&& snapshots_for_symbol
) {
    const auto window_size = async::TaskScheduler::instance()->io_thread_count();
    // We are going to use folly::collect to short-circuit on any network errors, which means we need to keep everything
    // alive even after this function exits to avoid segfaults in workers that are still going after one worker raises
    auto snapshots_for_symbol_ptr = std::make_shared<SymbolVersionToSnapshotInfoMap>(std::move(snapshots_for_symbol));
    auto symbol_version_vector_futures = folly::window(
            std::move(symbols),
            [store, version_map, snapshots_for_symbol_ptr](StreamId&& stream_id) {
                return async::submit_io_task(CheckReloadTask{
                                                     store,
                                                     version_map,
                                                     stream_id,
                                                     LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY}
                                             })
                        .via(&async::cpu_executor())
                        .thenValue([stream_id, snapshots_for_symbol_ptr](auto&& version_map_entry) {
                            auto all_versions = version_map_entry->get_indexes(false);
                            std::unordered_set<std::pair<StreamId, VersionId>> unpruned_versions;
                            VersionResultVector res;
                            for (const auto& entry : all_versions) {
                                unpruned_versions.emplace(stream_id, entry.version_id());
                                auto snapshots_it =
                                        snapshots_for_symbol_ptr->find(std::make_pair(stream_id, entry.version_id()));
                                auto snapshots = snapshots_it == snapshots_for_symbol_ptr->end()
                                                         ? std::vector<SnapshotId>()
                                                         : std::move(snapshots_it->second.snapshots);
                                res.emplace_back(
                                        stream_id, entry.version_id(), entry.creation_ts(), std::move(snapshots), false
                                );
                            }
                            for (auto& [sym_version, snapshot_info] : *snapshots_for_symbol_ptr) {
                                // For all symbol, version combinations in snapshots, check if they have been pruned,
                                // and if so use the information from the snapshot indexes and set deleted to true.
                                if (sym_version.first == stream_id && !unpruned_versions.contains(sym_version)) {
                                    res.emplace_back(
                                            sym_version.first,
                                            sym_version.second,
                                            snapshot_info.index_creation_ts,
                                            std::move(snapshot_info.snapshots),
                                            true
                                    );
                                }
                            }
                            // All symbols will be the same so compare only on date field
                            std::sort(res.begin(), res.end(), [](const VersionResult& v1, const VersionResult& v2) {
                                return std::get<2>(v1) > std::get<2>(v2);
                            });
                            return res;
                        });
            },
            window_size
    );
    auto symbol_version_vectors = folly::collect(std::move(symbol_version_vector_futures)).get();
    VersionResultVector res;
    res.reserve(std::accumulate(
            symbol_version_vectors.cbegin(),
            symbol_version_vectors.cend(),
            size_t(0),
            [](const size_t& accum, const auto& symbol_version_vector) { return accum + symbol_version_vector.size(); }
    ));
    // symbol_version_vectors are already in alphabetical order of symbols, so no sorting required
    for (auto&& symbol_version_vector : symbol_version_vectors) {
        res.insert(
                res.end(),
                std::make_move_iterator(symbol_version_vector.begin()),
                std::make_move_iterator(symbol_version_vector.end())
        );
    }
    return res;
}

VersionResultVector PythonVersionStore::list_versions(
        const std::optional<StreamId>& stream_id, const std::optional<SnapshotId>& snap_name, bool latest_only,
        bool skip_snapshots
) {
    ARCTICDB_SAMPLE(ListVersions, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: list_versions");

    // If the snapshot name is specified, and we are skipping snapshots (so the user does not want a list of every
    // snapshot that each version is in), then just load that snapshot and transform its contents.
    if (snap_name && skip_snapshots) {
        return list_versions_for_snapshot_without_snapshot_list(store(), stream_id, *snap_name);
    }

    auto stream_ids = std::unordered_set<StreamId>{};

    if (stream_id) {
        stream_ids.insert(*stream_id);
    } else {
        stream_ids = list_streams_unordered(snap_name);
    }

    const bool do_snapshots = !skip_snapshots || snap_name;

    SymbolVersionToSnapshotInfoMap snapshots_for_symbol;
    if (do_snapshots) {
        std::optional<SnapshotMap> versions_for_snapshots;
        get_snapshot_version_info(store(), snapshots_for_symbol, versions_for_snapshots, stream_id);

        if (snap_name) {
            return list_versions_for_snapshot_with_snapshot_list(
                    stream_ids, snap_name, std::move(*versions_for_snapshots), std::move(snapshots_for_symbol)
            );
        }

        // The step below needs to consider streams that have been deleted but are kept alive by a snapshot
        if (!stream_id && versions_for_snapshots) {
            for (const auto& keys : *versions_for_snapshots | ranges::views::values) {
                for (const AtomKey& key : keys) {
                    stream_ids.insert(key.id());
                }
            }
        }
    }

    std::vector<StreamId> ordered_stream_ids(
            std::make_move_iterator(stream_ids.begin()), std::make_move_iterator(stream_ids.end())
    );
    ranges::sort(ordered_stream_ids, std::greater<StreamId>());

    if (latest_only) {
        return get_latest_versions_for_symbols(
                store(), version_map(), std::move(ordered_stream_ids), std::move(snapshots_for_symbol)
        );
    } else {
        return get_all_versions_for_symbols(
                store(), version_map(), std::move(ordered_stream_ids), std::move(snapshots_for_symbol)
        );
    }
}

namespace {

py::object get_metadata_from_segment(const SegmentInMemory& segment) {
    py::object pyobj;
    if (segment.has_user_metadata()) {
        // Between v4.5.0 and v5.2.1 we saved this metadata here (commit 516d16968f0)
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        return python_util::pb_to_python(segment.user_metadata());
    } else if (segment.metadata()) {
        // Before v4.5.0 and after v5.2.1 we saved this metadata here
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        if (segment.metadata()->UnpackTo(&user_meta_proto)) {
            return python_util::pb_to_python(user_meta_proto);
        }
    }
    return pybind11::none();
}

py::object get_metadata_for_snapshot(const std::shared_ptr<Store>& store, const VariantKey& snap_key) {
    auto seg = store->read_sync(snap_key).second;
    return get_metadata_from_segment(seg);
}

std::pair<std::vector<AtomKey>, py::object> get_versions_and_metadata_from_snapshot(
        const std::shared_ptr<Store>& store, const VariantKey& vk
) {
    auto snapshot_segment = store->read_sync(vk).second;
    return {get_versions_from_segment(snapshot_segment), get_metadata_from_segment(snapshot_segment)};
}

} // namespace

std::vector<std::pair<SnapshotId, py::object>> PythonVersionStore::list_snapshots(std::optional<bool> load_metadata) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: list_snapshots");
    auto snap_ids = std::vector<std::pair<SnapshotId, py::object>>();
    auto fetch_metadata = opt_false(load_metadata);
    iterate_snapshots(store(), [store = store(), &snap_ids, fetch_metadata](const VariantKey& vk) {
        auto snapshot_meta_as_pyobject = fetch_metadata ? get_metadata_for_snapshot(store, vk) : py::none{};
        auto snapshot_id = fmt::format("{}", variant_key_id(vk));
        snap_ids.emplace_back(std::move(snapshot_id), std::move(snapshot_meta_as_pyobject));
    });

    return snap_ids;
}

void PythonVersionStore::add_to_snapshot(
        const SnapshotId& snap_name, const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries
) {
    util::check(
            version_queries.empty() || stream_ids.size() == version_queries.size(),
            "List length mismatch in add_to_snapshot: {} != {}",
            stream_ids.size(),
            version_queries.size()
    );
    auto opt_snapshot = get_snapshot(store(), snap_name);
    if (!opt_snapshot) {
        throw NoDataFoundException(snap_name);
    }
    auto [snap_key, snap_segment] = std::move(*opt_snapshot);
    auto [snapshot_contents, user_meta] = get_versions_and_metadata_from_snapshot(store(), snap_key);
    auto [specific_versions_index_map, latest_versions_index_map] = get_stream_index_map(stream_ids, version_queries);
    for (const auto& latest_version : *latest_versions_index_map) {
        specific_versions_index_map->try_emplace(
                std::make_pair(latest_version.first, latest_version.second.version_id()), latest_version.second
        );
    }

    auto missing = filter_keys_on_existence(
            utils::copy_of_values_as<VariantKey>(*specific_versions_index_map), store(), false
    );
    util::check(missing.empty(), "Cannot snapshot version(s) that have been deleted: {}", missing);

    std::vector<AtomKey> deleted_keys;
    std::vector<AtomKey> retained_keys;
    std::unordered_set<StreamId> affected_keys;
    for (const auto& [id_version, key] : *specific_versions_index_map) {
        auto [it, inserted] = affected_keys.insert(id_version.first);
        util::check(inserted, "Multiple elements in add_to_snapshot with key {}", id_version.first);
    }

    bool is_delete_keys_immediately =
            variant_key_type(snap_key) != KeyType::SNAPSHOT_REF || !cfg().write_options().delayed_deletes();
    for (auto&& key : snapshot_contents) {
        auto new_version = affected_keys.find(key.id());
        if (new_version == std::end(affected_keys)) {
            retained_keys.emplace_back(std::move(key));
        } else {
            if (is_delete_keys_immediately) {
                deleted_keys.emplace_back(std::move(key));
            }
        }
    }

    for (auto&& [id, key] : *specific_versions_index_map)
        retained_keys.emplace_back(std::move(key));

    std::sort(std::begin(retained_keys), std::end(retained_keys));
    if (is_delete_keys_immediately) {
        delete_trees_responsibly(store(), version_map(), deleted_keys, get_master_snapshots_map(store()), snap_name)
                .get();
        if (version_map()->log_changes()) {
            log_delete_snapshot(store(), snap_name);
        }
    }
    write_snapshot_entry(store(), retained_keys, snap_name, user_meta, version_map()->log_changes());
}

void PythonVersionStore::remove_from_snapshot(
        const SnapshotId& snap_name, const std::vector<StreamId>& stream_ids, const std::vector<VersionId>& version_ids
) {
    util::check(
            stream_ids.size() == version_ids.size(),
            "List length mismatch in remove_from_snapshot: {} != {}",
            stream_ids.size(),
            version_ids.size()
    );

    auto opt_snapshot = get_snapshot(store(), snap_name);
    if (!opt_snapshot) {
        throw NoDataFoundException(snap_name);
    }
    auto [snap_key, snap_segment] = std::move(*opt_snapshot);
    auto [snapshot_contents, user_meta] = get_versions_and_metadata_from_snapshot(store(), snap_key);

    using SymbolVersion = std::pair<StreamId, VersionId>;
    std::unordered_set<SymbolVersion> symbol_versions;
    for (auto i = 0u; i < stream_ids.size(); ++i) {
        symbol_versions.emplace(stream_ids[i], version_ids[i]);
    }

    bool is_delete_keys_immediately =
            variant_key_type(snap_key) != KeyType::SNAPSHOT_REF || !cfg().write_options().delayed_deletes();
    std::vector<AtomKey> deleted_keys;
    std::vector<AtomKey> retained_keys;
    for (auto&& key : snapshot_contents) {
        if (symbol_versions.find(SymbolVersion{key.id(), key.version_id()}) == symbol_versions.end()) {
            retained_keys.emplace_back(std::move(key));
        } else {
            if (is_delete_keys_immediately) {
                deleted_keys.emplace_back(std::move(key));
            }
        }
    }

    if (is_delete_keys_immediately) {
        delete_trees_responsibly(store(), version_map(), deleted_keys, get_master_snapshots_map(store()), snap_name)
                .get();
        if (version_map()->log_changes()) {
            log_delete_snapshot(store(), snap_name);
        }
    }
    write_snapshot_entry(store(), retained_keys, snap_name, user_meta, version_map()->log_changes());
}

void PythonVersionStore::verify_snapshot(const SnapshotId& snap_name) {
    if (CheckOutcome check_outcome = verify_snapshot_id(snap_name); std::holds_alternative<Error>(check_outcome)) {
        std::get<Error>(check_outcome).throw_error();
    }
}

void PythonVersionStore::snapshot(
        const SnapshotId& snap_name, const py::object& user_meta, const std::vector<StreamId>& skip_symbols,
        std::map<StreamId, VersionId>& versions, bool allow_partial_snapshot
) {
    ARCTICDB_SAMPLE(CreateSnapshot, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: snapshot");

    util::check_arg(skip_symbols.empty() || versions.empty(), "Only one of skip_symbols and versions can be set");

    // Explicitly set logging to error prior to testing if snapshot already exists as otherwise we're guaranteed to
    // see a warning message for all new snapshots (as the key doesn't exist!)
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
        std::set_difference(
                all_symbols.begin(),
                all_symbols.end(),
                skip_symbols_set.begin(),
                skip_symbols_set.end(),
                std::back_inserter(filtered_symbols)
        );

        missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
                !filtered_symbols.empty(),
                "No valid symbols in the library, skipping creation for snapshot: {}",
                snap_name
        );

        auto sym_index_map = batch_get_latest_version(store(), version_map(), filtered_symbols, false);
        index_keys = utils::values(*sym_index_map);
    } else {
        auto sym_index_map = batch_get_specific_version(
                store(), version_map(), versions, BatchGetVersionOption::LIVE_AND_TOMBSTONED_VER_REF_IN_OTHER_SNAPSHOT
        );
        if (allow_partial_snapshot) {
            missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
                    !sym_index_map->empty(),
                    "None of the symbol-version pairs specified in versions exist, skipping creation for snapshot: {}",
                    snap_name
            );
        } else {
            if (sym_index_map->size() != versions.size()) {
                std::string error_msg = fmt::format(
                        "Snapshot {} will not be created. Specified symbol-version pairs do not exist in the library: ",
                        snap_name
                );
                for (const auto& kv : versions) {
                    if (!sym_index_map->count(kv.first)) {
                        error_msg += fmt::format("{}:{} ", kv.first, kv.second);
                    }
                }
                missing_data::raise<ErrorCode::E_NO_SUCH_VERSION>(error_msg);
            }
        }
        index_keys = utils::values(*sym_index_map);
        auto missing = filter_keys_on_existence(utils::copy_of_values_as<VariantKey>(*sym_index_map), store(), false);
        util::check(missing.empty(), "Cannot snapshot version(s) that have been deleted: {}", missing);
    }

    ARCTICDB_DEBUG(log::version(), "Total Index keys in snapshot={}", index_keys.size());
    write_snapshot_entry(store(), index_keys, snap_name, user_meta, version_map()->log_changes());
}

std::set<StreamId> PythonVersionStore::list_streams(
        const std::optional<SnapshotId>& snap_name, const std::optional<std::string>& regex,
        const std::optional<std::string>& prefix, const std::optional<bool>& opt_use_symbol_list,
        const std::optional<bool>& opt_all_symbols

) {
    auto res = list_streams_unordered_internal(snap_name, regex, prefix, opt_use_symbol_list, opt_all_symbols);
    return {std::make_move_iterator(res.begin()), std::make_move_iterator(res.end())};
}

std::unordered_set<StreamId> PythonVersionStore::list_streams_unordered(
        const std::optional<SnapshotId>& snap_name, const std::optional<std::string>& regex,
        const std::optional<std::string>& prefix, const std::optional<bool>& opt_use_symbol_list,
        const std::optional<bool>& opt_all_symbols

) {
    return list_streams_unordered_internal(snap_name, regex, prefix, opt_use_symbol_list, opt_all_symbols);
}

size_t PythonVersionStore::compact_symbol_list() { return compact_symbol_list_internal(); }

VersionedItem PythonVersionStore::write_partitioned_dataframe(
        const StreamId& stream_id, const py::tuple& item, const py::object& norm_meta,
        const std::vector<std::string>& partition_value
) {
    ARCTICDB_SAMPLE(WritePartitionedDataFrame, 0)
    auto [maybe_prev, deleted] = ::arcticdb::get_latest_version(store(), version_map(), stream_id);
    auto version_id = get_next_version_from_key(maybe_prev);

    //    TODO: We are not actually partitioning stuff atm, just assuming a single partition is passed for now.
    std::array<py::object, 1> partitioned_dfs{item};

    auto write_options = get_write_options();
    auto de_dup_map = std::make_shared<DeDupMap>();

    std::vector<entity::AtomKey> index_keys;
    for (size_t idx = 0; idx < partitioned_dfs.size(); idx++) {
        auto subkeyname = fmt::format("{}-{}", stream_id, partition_value[idx]);
        auto versioned_item = write_dataframe_impl(
                store(),
                version_id,
                convert::py_ndf_to_frame(
                        subkeyname, partitioned_dfs[idx], norm_meta, py::none(), cfg().write_options().empty_types()
                ),
                write_options,
                de_dup_map,
                false
        );
        index_keys.emplace_back(versioned_item.key_);
    }

    folly::Future<VariantKey> multi_key_fut = folly::Future<VariantKey>::makeEmpty();
    IndexAggregator<RowCountIndex> multi_index_agg(
            stream_id,
            [&stream_id, version_id, &multi_key_fut, store = store()](auto&& segment) {
                multi_key_fut = store->write(KeyType::PARTITION,
                                             version_id, // version_id
                                             stream_id,
                                             NumericIndex{0}, // start_index
                                             NumericIndex{0}, // end_index
                                             std::forward<decltype(segment)>(segment))
                                        .wait();
            }
    );

    for (const auto& index_key : index_keys) {
        multi_index_agg.add_key(index_key);
    }

    multi_index_agg.commit();
    return {to_atom(std::move(multi_key_fut).get())};
    //    TODO: now store this in the version key for this symbol
}

VersionedItem PythonVersionStore::write_versioned_composite_data(
        const StreamId& stream_id, const py::object& metastruct, const std::vector<StreamId>& sub_keys,
        const std::vector<convert::InputItem>& items, const std::vector<py::object>& norm_metas,
        const py::object& user_meta, bool prune_previous_versions
) {
    ARCTICDB_SAMPLE(WriteVersionedMultiKey, 0)
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write_versioned_composite_data");

    auto [maybe_prev, deleted] = ::arcticdb::get_latest_version(store(), version_map(), stream_id);
    auto version_id = get_next_version_from_key(maybe_prev);
    ARCTICDB_DEBUG(
            log::version(), "write_versioned_composite_data for stream_id: {} , version_id = {}", stream_id, version_id
    );
    // TODO: Assuming each sub key is always going to have the same version attached to it.
    std::vector<VersionId> version_ids;
    version_ids.reserve(sub_keys.size());

    std::vector<py::object> user_metas;
    user_metas.reserve(sub_keys.size());

    std::vector<std::shared_ptr<DeDupMap>> de_dup_maps;
    de_dup_maps.reserve(sub_keys.size());

    auto write_options = get_write_options();
    auto de_dup_map = get_de_dup_map(stream_id, maybe_prev, write_options);
    for (auto i = 0u; i < sub_keys.size(); ++i) {
        version_ids.emplace_back(version_id);
        user_metas.emplace_back(py::none());
        de_dup_maps.emplace_back(de_dup_map);
    }

    auto frames =
            create_input_tensor_frames(sub_keys, items, norm_metas, user_metas, cfg().write_options().empty_types());
    // Need to hold the GIL up to this point as we will call pb_from_python
    auto release_gil = std::make_unique<py::gil_scoped_release>();
    auto index_keys =
            batch_write_internal(std::move(version_ids), sub_keys, std::move(frames), std::move(de_dup_maps), false)
                    .get();
    release_gil.reset();
    auto multi_key = write_multi_index_entry(store(), index_keys, stream_id, metastruct, user_meta, version_id);
    auto versioned_item = VersionedItem(to_atom(std::move(multi_key)));
    write_version_and_prune_previous(prune_previous_versions, versioned_item.key_, maybe_prev);

    if (cfg().symbol_list())
        symbol_list().add_symbol(store(), stream_id, version_id);

    return versioned_item;
}

VersionedItem PythonVersionStore::write_versioned_dataframe(
        const StreamId& stream_id, const convert::InputItem& item, const py::object& norm, const py::object& user_meta,
        bool prune_previous_versions, bool sparsify_floats, bool validate_index
) {
    ARCTICDB_SAMPLE(WriteVersionedDataframe, 0)
    auto frame = convert::py_ndf_to_frame(stream_id, item, norm, user_meta, cfg().write_options().empty_types());
    auto versioned_item = write_versioned_dataframe_internal(
            stream_id, frame, prune_previous_versions, sparsify_floats, validate_index
    );

    return versioned_item;
}

VersionedItem PythonVersionStore::test_write_versioned_segment(
        const StreamId& stream_id,
        SegmentInMemory& segment, // we use lvalue reference because pybind does not allow rvalue reference
        bool prune_previous_versions, Slicing slicing
) {
    ARCTICDB_SAMPLE(WriteVersionedSegment, 0)
    auto versioned_item = write_segment(stream_id, std::move(segment), prune_previous_versions, slicing);
    return versioned_item;
}

VersionedItem PythonVersionStore::append(
        const StreamId& stream_id, const convert::InputItem& item, const py::object& norm, const py::object& user_meta,
        bool upsert, bool prune_previous_versions, bool validate_index
) {
    return append_internal(
            stream_id,
            convert::py_ndf_to_frame(stream_id, item, norm, user_meta, cfg().write_options().empty_types()),
            upsert,
            prune_previous_versions,
            validate_index
    );
}

VersionedItem PythonVersionStore::update(
        const StreamId& stream_id, const UpdateQuery& query, const convert::InputItem& item, const py::object& norm,
        const py::object& user_meta, bool upsert, bool dynamic_schema, bool prune_previous_versions
) {
    return update_internal(
            stream_id,
            query,
            convert::py_ndf_to_frame(stream_id, item, norm, user_meta, cfg().write_options().empty_types()),
            upsert,
            dynamic_schema,
            prune_previous_versions
    );
}

VersionedItem PythonVersionStore::delete_range(
        const StreamId& stream_id, const UpdateQuery& query, bool dynamic_schema, bool prune_previous_versions
) {
    return delete_range_internal(stream_id, query, DeleteRangeOptions{dynamic_schema, prune_previous_versions});
}

void PythonVersionStore::append_incomplete(
        const StreamId& stream_id, const py::tuple& item, const py::object& norm, const py::object& user_meta,
        bool validate_index
) const {

    using namespace arcticdb::entity;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    // Turn the input into a standardised frame object
    auto frame = convert::py_ndf_to_frame(stream_id, item, norm, user_meta, cfg().write_options().empty_types());
    append_incomplete_frame(stream_id, frame, validate_index);
}

VersionedItem PythonVersionStore::write_metadata(
        const StreamId& stream_id, const py::object& user_meta, bool prune_previous_versions
) {
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
    python_util::pb_from_python(user_meta, user_meta_proto);
    return write_versioned_metadata_internal(stream_id, prune_previous_versions, std::move(user_meta_proto));
}

void PythonVersionStore::create_column_stats_version(
        const StreamId& stream_id, ColumnStats& column_stats, const VersionQuery& version_query
) {
    ReadOptions read_options;
    read_options.set_dynamic_schema(cfg().write_options().dynamic_schema());
    create_column_stats_version_internal(stream_id, column_stats, version_query, read_options);
}

void PythonVersionStore::drop_column_stats_version(
        const StreamId& stream_id, const std::optional<ColumnStats>& column_stats_to_drop,
        const VersionQuery& version_query
) {
    drop_column_stats_version_internal(stream_id, column_stats_to_drop, version_query);
}

ReadResult PythonVersionStore::read_column_stats_version(
        const StreamId& stream_id, const VersionQuery& version_query, std::any& handler_data
) {
    ARCTICDB_SAMPLE(ReadColumnStats, 0)
    auto [versioned_item, frame_and_descriptor] = read_column_stats_version_internal(stream_id, version_query);
    return read_result_from_single_frame(frame_and_descriptor, versioned_item.key_, handler_data, OutputFormat::PANDAS);
}

ColumnStats PythonVersionStore::get_column_stats_info_version(
        const StreamId& stream_id, const VersionQuery& version_query
) {
    ARCTICDB_SAMPLE(GetColumnStatsInfo, 0)
    return get_column_stats_info_version_internal(stream_id, version_query);
}

static void validate_stage_results(
        const std::optional<std::vector<StageResult>>& stage_results, const StreamId& stream_id
) {
    if (!stage_results) {
        return;
    }

    for (const auto& stage_result : *stage_results) {
        for (const auto& staged_segment : stage_result.staged_segments) {
            user_input::check<ErrorCode::E_STAGE_RESULT_WITH_INCORRECT_SYMBOL>(
                    staged_segment.id() == stream_id,
                    fmt::format(
                            "Expected all stage_result objects submitted for compaction to have "
                            "the specified symbol {} but found one with symbol {}",
                            stream_id,
                            staged_segment.id()
                    )
            );
        }
    }
}

std::variant<VersionedItem, CompactionError> PythonVersionStore::compact_incomplete(
        const StreamId& stream_id, bool append, bool convert_int_to_float, bool via_iteration /*= true */,
        bool sparsify /*= false */, const std::optional<py::object>& user_meta /* = std::nullopt */,
        bool prune_previous_versions, bool validate_index, bool delete_staged_data_on_failure,
        const std::optional<std::vector<StageResult>>& stage_results
) {
    std::optional<arcticdb::proto::descriptors::UserDefinedMetadata> meta;
    if (user_meta && !user_meta->is_none()) {
        meta = std::make_optional<arcticdb::proto::descriptors::UserDefinedMetadata>();
        python_util::pb_from_python(*user_meta, *meta);
    }

    validate_stage_results(stage_results, stream_id);

    CompactIncompleteParameters params{
            .prune_previous_versions_ = prune_previous_versions,
            .append_ = append,
            .convert_int_to_float_ = convert_int_to_float,
            .via_iteration_ = via_iteration,
            .sparsify_ = sparsify,
            .validate_index_ = validate_index,
            .delete_staged_data_on_failure_ = delete_staged_data_on_failure,
            .stage_results = stage_results
    };

    return compact_incomplete_dynamic(stream_id, meta, params);
}

std::variant<VersionedItem, CompactionError> PythonVersionStore::sort_merge(
        const StreamId& stream_id, const py::object& user_meta, bool append, bool convert_int_to_float,
        bool via_iteration, bool sparsify, bool prune_previous_versions, bool delete_staged_data_on_failure,
        const std::optional<std::vector<StageResult>>& stage_results
) {
    std::optional<arcticdb::proto::descriptors::UserDefinedMetadata> meta;
    if (!user_meta.is_none()) {
        meta = std::make_optional<arcticdb::proto::descriptors::UserDefinedMetadata>();
        python_util::pb_from_python(user_meta, *meta);
    }

    validate_stage_results(stage_results, stream_id);

    CompactIncompleteParameters params{
            .prune_previous_versions_ = prune_previous_versions,
            .append_ = append,
            .convert_int_to_float_ = convert_int_to_float,
            .via_iteration_ = via_iteration,
            .sparsify_ = sparsify,
            .delete_staged_data_on_failure_ = delete_staged_data_on_failure,
            .stage_results = stage_results
    };

    return sort_merge_internal(stream_id, meta, params);
}

StageResult PythonVersionStore::write_parallel(
        const StreamId& stream_id, const convert::InputItem& item, const py::object& norm, bool validate_index,
        bool sort_on_index, std::optional<std::vector<std::string>> sort_columns
) const {
    auto frame = convert::py_ndf_to_frame(stream_id, item, norm, py::none(), cfg().write_options().empty_types());
    return write_parallel_frame(stream_id, frame, validate_index, sort_on_index, sort_columns);
}

std::unordered_map<VersionId, bool> PythonVersionStore::get_all_tombstoned_versions(const StreamId& stream_id) {
    return ::arcticdb::get_all_tombstoned_versions(store(), version_map(), stream_id);
}

std::vector<std::variant<ReadResult, DataError>> PythonVersionStore::batch_read(
        const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries,
        std::vector<std::shared_ptr<ReadQuery>>& read_queries, const BatchReadOptions& batch_read_options,
        std::any& handler_data
) {

    auto read_versions_or_errors =
            batch_read_internal(stream_ids, version_queries, read_queries, batch_read_options, handler_data);
    std::vector<std::variant<ReadResult, DataError>> res;
    for (auto&& [idx, read_version_or_error] : folly::enumerate(read_versions_or_errors)) {
        util::variant_match(
                read_version_or_error,
                [&res, &batch_read_options](ReadVersionWithNodesOutput& read_version) {
                    res.emplace_back(create_python_read_result(
                            read_version.root_.versioned_item_,
                            batch_read_options.output_format(),
                            std::move(read_version.root_.frame_and_descriptor_),
                            std::nullopt,
                            std::move(read_version.nodes_)
                    ));
                },
                [&res](DataError& data_error) { res.emplace_back(std::move(data_error)); }
        );
    }
    return res;
}

std::vector<std::variant<VersionedItem, DataError>> PythonVersionStore::batch_update(
        const std::vector<StreamId>& stream_ids, const std::vector<convert::InputItem>& items,
        const std::vector<py::object>& norms, const std::vector<py::object>& user_metas,
        const std::vector<UpdateQuery>& update_qeries, bool prune_previous_versions, bool upsert
) {
    auto frames = create_input_tensor_frames(stream_ids, items, norms, user_metas, cfg().write_options().empty_types());
    return batch_update_internal(stream_ids, std::move(frames), update_qeries, prune_previous_versions, upsert);
}

ReadResult PythonVersionStore::batch_read_and_join(
        std::shared_ptr<std::vector<StreamId>> stream_ids, std::shared_ptr<std::vector<VersionQuery>> version_queries,
        std::vector<std::shared_ptr<ReadQuery>>& read_queries, const ReadOptions& read_options,
        std::vector<std::shared_ptr<Clause>>&& clauses, std::any& handler_data
) {
    auto versions_and_frame = batch_read_and_join_internal(
            std::move(stream_ids),
            std::move(version_queries),
            read_queries,
            read_options,
            std::move(clauses),
            handler_data
    );
    return create_python_read_result(
            versions_and_frame.versioned_items_,
            read_options.output_format(),
            std::move(versions_and_frame.frame_and_descriptor_),
            std::move(versions_and_frame.metadatas_)
    );
}

void PythonVersionStore::delete_snapshot(const SnapshotId& snap_name) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: delete_snapshot");
    auto opt_snapshot = get_snapshot(store(), snap_name);
    if (!opt_snapshot) {
        throw NoDataFoundException(snap_name);
    }
    auto [snap_key, snap_segment] = std::move(*opt_snapshot);

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
    auto snap_map = get_master_snapshots_map(store(), std::tie(snap_key, index_keys_in_current_snapshot));

    ARCTICDB_DEBUG(log::version(), "Deleting Snapshot {}", snap_name);
    store()->remove_key(snap_key).get();

    try {
        delete_trees_responsibly(store(), version_map(), index_keys_in_current_snapshot, snap_map, snap_name).get();
        ARCTICDB_DEBUG(log::version(), "Deleted orphaned index keys in snapshot {}", snap_name);
    } catch (const std::exception& ex) {
        log::version().warn("Garbage collection of unreachable deleted index keys failed due to: {}", ex.what());
    }
}

ReadResult PythonVersionStore::read_dataframe_version(
        const StreamId& stream_id, const VersionQuery& version_query, const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options, std::any& handler_data
) {

    auto opt_version_and_frame =
            read_dataframe_version_internal(stream_id, version_query, read_query, read_options, handler_data);
    return create_python_read_result(
            opt_version_and_frame.root_.versioned_item_,
            read_options.output_format(),
            std::move(opt_version_and_frame.root_.frame_and_descriptor_),
            std::nullopt,
            std::move(opt_version_and_frame.nodes_)
    );
}

std::shared_ptr<LazyRecordBatchIterator> PythonVersionStore::create_lazy_record_batch_iterator(
        const StreamId& stream_id, const VersionQuery& version_query, const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options, std::shared_ptr<FilterClause> filter_clause, size_t prefetch_size
) {
    py::gil_scoped_release release_gil;

    // Resolve version
    auto version = get_version_to_read(stream_id, version_query);
    std::variant<VersionedItem, StreamId> version_info;
    if (version) {
        version_info = *version;
    } else if (opt_false(read_options.incompletes())) {
        version_info = stream_id;
    } else {
        missing_data::raise<ErrorCode::E_NO_SUCH_VERSION>(
                "create_lazy_record_batch_iterator: version matching query '{}' not found for symbol '{}'",
                version_query,
                stream_id
        );
    }

    // Read only the index — populates slice_and_keys_ (cheap metadata I/O, no segment data)
    auto pipeline_context = version_store::setup_pipeline_context(store(), version_info, *read_query, read_options);

    util::check(
            !pipeline_context->multi_key_,
            "Lazy record batch iterator does not support recursive/composite data (multi_key)"
    );

    // Build columns_to_decode from the pipeline context's column bitset.
    // If a FilterClause is provided, also include its required input columns
    // so that segments contain the columns needed for expression evaluation.
    std::shared_ptr<std::unordered_set<std::string>> cols_to_decode;
    if (pipeline_context->overall_column_bitset_) {
        cols_to_decode = std::make_shared<std::unordered_set<std::string>>();
        auto en = pipeline_context->overall_column_bitset_->first();
        auto en_end = pipeline_context->overall_column_bitset_->end();
        while (en < en_end) {
            cols_to_decode->insert(std::string(pipeline_context->desc_->field(*en++).name()));
        }
        // Ensure filter clause input columns are decoded even if not in the user's column selection
        if (filter_clause && filter_clause->clause_info().input_columns_) {
            for (const auto& col : *filter_clause->clause_info().input_columns_) {
                cols_to_decode->insert(col);
            }
        }
    }

    // Extract filter expression context and root node name from the FilterClause
    std::shared_ptr<ExpressionContext> expression_context;
    std::string filter_root_node_name;
    if (filter_clause) {
        expression_context = filter_clause->expression_context_;
        expression_context->dynamic_schema_ = opt_false(read_options.dynamic_schema());
        filter_root_node_name = filter_clause->root_node_name_.value;
    }

    return std::make_shared<LazyRecordBatchIterator>(
            std::move(pipeline_context->slice_and_keys_),
            pipeline_context->descriptor(),
            store(),
            std::move(cols_to_decode),
            read_query->row_filter,
            std::move(expression_context),
            std::move(filter_root_node_name),
            prefetch_size
    );
}

VersionedItem PythonVersionStore::read_modify_write(
        const StreamId& source_stream, const StreamId& target_stream, const py::object& user_meta,
        const VersionQuery& version_query, const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options, const bool prune_previous_versions
) {
    return read_modify_write_internal(
            source_stream,
            target_stream,
            version_query,
            read_query,
            read_options,
            prune_previous_versions,
            python_util::maybe_pb_from_python<proto::descriptors::UserDefinedMetadata>(user_meta)
    );
}

namespace {

std::vector<SnapshotVariantKey> ARCTICDB_UNUSED iterate_snapshot_tombstones(
        const std::string& limit_stream_id, std::set<IndexTypeKey>& candidates, const std::shared_ptr<Store>& store
) {

    std::vector<SnapshotVariantKey> snap_tomb_keys;
    if (limit_stream_id.empty()) {
        store->iterate_type(
                KeyType::SNAPSHOT_TOMBSTONE,
                [&store, &candidates, &snap_tomb_keys](VariantKey&& snap_tomb_key) {
                    ARCTICDB_DEBUG(log::version(), "Processing {}", snap_tomb_key);
                    std::vector<IndexTypeKey> indexes{};
                    auto snap_seg = store->read_sync(snap_tomb_key).second;
                    auto before ARCTICDB_UNUSED = candidates.size();

                    for (size_t idx = 0; idx < snap_seg.row_count(); idx++) {
                        auto key = read_key_row(snap_seg, static_cast<ssize_t>(idx));
                        if (candidates.count(key) ==
                            0) { // Snapshots often hold the same keys, so worthwhile optimisation
                            indexes.emplace_back(std::move(key));
                        }
                    }

                    if (!indexes.empty()) {
                        filter_keys_on_existence(indexes, store, true);
                        candidates.insert(std::move_iterator(indexes.begin()), std::move_iterator(indexes.end()));
                        indexes.clear();
                    }

                    ARCTICDB_DEBUG(
                            log::version(),
                            "Processed {} keys from snapshot {}. {} are unique.",
                            snap_seg.row_count(),
                            variant_key_id(snap_tomb_key),
                            candidates.size() - before
                    );
                    snap_tomb_keys.emplace_back(std::move(snap_tomb_key));
                }
        );
    }
    return snap_tomb_keys;
}

} // namespace

// Kept for backwards compatibility
void PythonVersionStore::delete_version(const StreamId& stream_id, VersionId version_id) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: delete_version");
    delete_versions(stream_id, {version_id});
}

void PythonVersionStore::delete_versions(const StreamId& stream_id, const std::vector<VersionId>& version_ids) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: delete_versions");
    if (version_ids.empty()) {
        log::version().info("No version ids passed for delete_versions for stream {}, skipping", stream_id);
        return;
    }

    std::unordered_set<VersionId> version_ids_set(version_ids.begin(), version_ids.end());
    auto result = ::arcticdb::tombstone_versions(store(), version_map(), stream_id, version_ids_set);
    if (!result.keys_to_delete.empty() && !cfg().write_options().delayed_deletes()) {
        delete_tree(result.keys_to_delete, result);
    }

    if (result.no_undeleted_left && cfg().symbol_list()) {
        symbol_list().remove_symbol(store(), stream_id, result.latest_version_);
    }
}

std::vector<std::optional<DataError>> PythonVersionStore::batch_delete(
        const std::vector<StreamId>& stream_ids, const std::vector<std::vector<VersionId>>& version_ids
) {
    // This error can only be triggered when the function is called from batch_delete_versions
    // The other code paths make checks that prevents us getting to this point
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            stream_ids.size() == version_ids.size(),
            "when calling batch_delete_versions, stream_ids and version_ids must have the same size"
    );

    auto results = batch_delete_internal(stream_ids, version_ids);

    std::vector<std::optional<DataError>> return_results;

    std::vector<IndexTypeKey> keys_to_delete;
    std::vector<std::pair<StreamId, VersionId>> symbols_to_delete;

    for (const auto& result : results) {
        util::variant_match(
                result,
                [&](const version_store::TombstoneVersionResult& tombstone_result) {
                    return_results.emplace_back(std::nullopt);

                    if (tombstone_result.keys_to_delete.empty()) {
                        log::version().warn("Nothing to delete for symbol '{}'", tombstone_result.symbol);
                        return;
                    }

                    if (!cfg().write_options().delayed_deletes()) {
                        keys_to_delete.insert(
                                keys_to_delete.end(),
                                tombstone_result.keys_to_delete.begin(),
                                tombstone_result.keys_to_delete.end()
                        );
                    }

                    if (tombstone_result.no_undeleted_left && cfg().symbol_list() &&
                        !tombstone_result.keys_to_delete.empty()) {
                        symbols_to_delete.emplace_back(tombstone_result.symbol, tombstone_result.latest_version_);
                    }
                },
                [&](const DataError& data_error) {
                    return_results.emplace_back(std::make_optional(std::move(data_error)));
                }
        );
    }

    // Make sure to call delete_tree and thus get_master_snapshots_map only once for all symbols
    if (!keys_to_delete.empty()) {
        delete_tree(keys_to_delete, TombstoneVersionResult{true});
    }

    auto sym_delete_results = batch_delete_symbols_internal(symbols_to_delete);

    for (size_t i = 0; i < symbols_to_delete.size(); ++i) {
        const auto& result = sym_delete_results[i];
        if (std::holds_alternative<DataError>(result)) {
            return_results[i] = std::make_optional(std::get<DataError>(result));
        }
    }

    return return_results;
}

void PythonVersionStore::fix_symbol_trees(const std::vector<StreamId>& symbols) {
    auto snaps = get_master_snapshots_map(store());
    for (const auto& sym : symbols) {
        auto index_keys_from_symbol_tree = get_all_versions(store(), version_map(), sym);
        for (const auto& [key, map] : snaps[sym]) {
            index_keys_from_symbol_tree.push_back(key);
        }
        std::sort(
                std::begin(index_keys_from_symbol_tree),
                std::end(index_keys_from_symbol_tree),
                [&](const auto& k1, const auto& k2) { return k1.version_id() > k2.version_id(); }
        );
        auto last = std::unique(
                std::begin(index_keys_from_symbol_tree),
                std::end(index_keys_from_symbol_tree),
                [&](const auto& k1, const auto& k2) { return k1.version_id() == k2.version_id(); }
        );
        index_keys_from_symbol_tree.erase(last, index_keys_from_symbol_tree.end());
        version_map()->overwrite_symbol_tree(store(), sym, index_keys_from_symbol_tree);
    }
}

void PythonVersionStore::prune_previous_versions(const StreamId& stream_id) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: prune_previous_versions stream_id={}", stream_id);
    const std::shared_ptr<VersionMapEntry>& entry = version_map()->check_reload(
            store(), stream_id, LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY}, __FUNCTION__
    );
    storage::check<ErrorCode::E_SYMBOL_NOT_FOUND>(!entry->empty(), "Symbol {} is not found", stream_id);
    auto [latest, deleted] = entry->get_first_index(false);
    util::check(static_cast<bool>(latest), "Failed to find latest index");
    auto prev_id = get_prev_version_in_entry(entry, latest->version_id());
    if (!prev_id) {
        ARCTICDB_DEBUG(log::version(), "No previous versions to prune for stream_id={}", stream_id);
        return;
    }

    auto previous = ::arcticdb::get_specific_version(store(), version_map(), stream_id, *prev_id);
    auto [_, pruned_indexes] = version_map()->tombstone_from_key_or_all(store(), stream_id, previous);
    delete_unreferenced_pruned_indexes(std::move(pruned_indexes), *latest).get();
}

void PythonVersionStore::delete_all_versions(const StreamId& stream_id) {
    ARCTICDB_SAMPLE(DeleteAllVersions, 0)

    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: delete_all_versions");
    try {
        auto res = tombstone_all_async(store(), version_map(), stream_id).get();
        auto version_id = res.latest_version_;
        auto all_index_keys = res.keys_to_delete;

        if (all_index_keys.empty()) {
            log::version().warn("Nothing to delete for symbol '{}'", stream_id);
            return;
        }
        if (cfg().symbol_list())
            symbol_list().remove_symbol(store(), stream_id, version_id);

        ARCTICDB_DEBUG(
                log::version(),
                "Version heads deleted for symbol {}. Proceeding with index keys total of {}",
                stream_id,
                all_index_keys.size()
        );
        if (!cfg().write_options().delayed_deletes()) {
            delete_tree({all_index_keys.begin(), all_index_keys.end()});
        } else {
            ARCTICDB_DEBUG(log::version(), "Not deleting data for {}", stream_id);
        }

        ARCTICDB_DEBUG(log::version(), "Delete of Symbol {} successful", stream_id);
    } catch (const StorageException& ex) {
        log::version().error("Got storage exception in delete - possible parallel deletion?: {}", ex.what());
    } catch (const CodecException& ex) {
        log::version().error("Got codec exception in delete - possible parallel deletion?: {}", ex.what());
    }
}

std::vector<timestamp> PythonVersionStore::get_update_times(
        const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries
) {
    return batch_get_update_times(stream_ids, version_queries);
}

timestamp PythonVersionStore::get_update_time(const StreamId& stream_id, const VersionQuery& version_query) {
    return get_update_time_internal(stream_id, version_query);
}

namespace {
py::object metadata_protobuf_to_pyobject(const std::optional<google::protobuf::Any>& metadata_proto) {
    py::object pyobj;
    if (metadata_proto) {
        if (metadata_proto->Is<arcticdb::proto::descriptors::TimeSeriesDescriptor>()) {
            arcticdb::proto::descriptors::TimeSeriesDescriptor tsd;
            metadata_proto->UnpackTo(&tsd);
            pyobj = python_util::pb_to_python(tsd.user_meta());
        } else {
            arcticdb::proto::descriptors::FrameMetadata meta;
            metadata_proto->UnpackTo(&meta);
            pyobj = python_util::pb_to_python(meta.user_meta());
        }
    } else {
        pyobj = pybind11::none();
    }
    return pyobj;
}
} // namespace

std::pair<VersionedItem, py::object> PythonVersionStore::read_metadata(
        const StreamId& stream_id, const VersionQuery& version_query
) {
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: read_metadata");
    ARCTICDB_SAMPLE(ReadMetadata, 0)

    auto metadata = read_metadata_internal(stream_id, version_query);
    if (!metadata.first.has_value())
        throw NoDataFoundException(fmt::format("read_metadata: version not found for symbol", stream_id));

    auto metadata_proto = metadata.second;
    py::object pyobj = metadata_protobuf_to_pyobject(metadata_proto);
    VersionedItem version{std::move(to_atom(*metadata.first))};
    return std::pair{version, pyobj};
}

std::vector<std::variant<VersionedItem, DataError>> PythonVersionStore::batch_write_metadata(
        const std::vector<StreamId>& stream_ids, const std::vector<py::object>& user_meta, bool prune_previous_versions,
        bool throw_on_error
) {
    std::vector<arcticdb::proto::descriptors::UserDefinedMetadata> user_meta_protos;
    user_meta_protos.reserve(user_meta.size());
    for (const auto& user_meta_item : user_meta) {
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        python_util::pb_from_python(user_meta_item, user_meta_proto);
        user_meta_protos.emplace_back(std::move(user_meta_proto));
    }
    return batch_write_versioned_metadata_internal(
            stream_ids, prune_previous_versions, throw_on_error, std::move(user_meta_protos)
    );
}

std::vector<std::pair<VersionedItem, TimeseriesDescriptor>> PythonVersionStore::batch_restore_version(
        const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries
) {
    return batch_restore_version_internal(stream_ids, version_queries);
}

std::vector<std::variant<std::pair<VersionedItem, py::object>, DataError>> PythonVersionStore::batch_read_metadata(
        const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries,
        const BatchReadOptions& batch_read_options
) {
    ARCTICDB_SAMPLE(BatchReadMetadata, 0)
    auto metadatas_or_errors = batch_read_metadata_internal(stream_ids, version_queries, batch_read_options);

    std::vector<std::variant<std::pair<VersionedItem, py::object>, DataError>> results;
    for (auto& metadata_or_error : metadatas_or_errors) {
        if (std::holds_alternative<std::pair<VariantKey, std::optional<google::protobuf::Any>>>(metadata_or_error)) {
            auto& [key, meta_proto] =
                    std::get<std::pair<VariantKey, std::optional<google::protobuf::Any>>>(metadata_or_error);
            VersionedItem version{to_atom(std::move(key))};
            if (meta_proto.has_value()) {
                results.emplace_back(std::pair{std::move(version), metadata_protobuf_to_pyobject(meta_proto)});
            } else {
                results.emplace_back(std::pair{std::move(version), py::none()});
            }
        } else {
            results.emplace_back(std::get<DataError>(std::move(metadata_or_error)));
        }
    }
    return results;
}

DescriptorItem PythonVersionStore::read_descriptor(const StreamId& stream_id, const VersionQuery& version_query) {
    return read_descriptor_internal(stream_id, version_query);
}

std::vector<std::variant<DescriptorItem, DataError>> PythonVersionStore::batch_read_descriptor(
        const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries,
        const BatchReadOptions& batch_read_options
) {

    return batch_read_descriptor_internal(stream_ids, version_queries, batch_read_options);
}

ReadResult PythonVersionStore::read_index(
        const StreamId& stream_id, const VersionQuery& version_query, OutputFormat output_format, std::any& handler_data
) {
    ARCTICDB_SAMPLE(ReadIndex, 0)

    auto version = get_version_to_read(stream_id, version_query);
    if (!version)
        throw NoDataFoundException(fmt::format("read_index: version not found for symbol '{}'", stream_id));

    auto res = read_index_impl(store(), *version);
    return read_result_from_single_frame(res, version->key_, handler_data, output_format);
}

std::vector<AtomKey> PythonVersionStore::get_version_history(const StreamId& stream_id) {
    return get_index_and_tombstone_keys(store(), version_map(), stream_id);
}

void PythonVersionStore::_compact_version_map(const StreamId& id) { version_map()->compact(store(), id); }

void PythonVersionStore::compact_library(size_t batch_size) {
    version_map()->compact_if_necessary_stand_alone(store(), batch_size);
}

std::vector<SliceAndKey> PythonVersionStore::list_incompletes(const StreamId& stream_id) {
    return get_incomplete(store(), stream_id, unspecified_range(), 0u, true, false);
}

void PythonVersionStore::clear(const bool continue_on_error) {
    version_map()->flush();
    if (store()->fast_delete()) {
        // Most storage backends have a fast deletion method for a db/collection equivalent, eg. drop() for mongo and
        // lmdb and iterating each key is always going to be suboptimal.
        ARCTICDB_DEBUG(log::version(), "Fast deleting library as supported by storage");
        return;
    }

    delete_all(store(), continue_on_error);
}

bool PythonVersionStore::empty() { return is_empty_excluding_key_types({KeyType::SYMBOL_LIST}); }

bool PythonVersionStore::is_empty_excluding_key_types(const std::vector<KeyType>& excluded_key_types) {
    // No good way to break out of these iterations, so use exception for flow control
    try {
        foreach_key_type([&excluded_key_types, store = store()](KeyType key_type) {
            if (std::find(excluded_key_types.begin(), excluded_key_types.end(), key_type) == excluded_key_types.end()) {
                store->iterate_type(key_type, [](VariantKey&&) { throw std::exception(); });
            }
        });
    } catch (...) {
        return false;
    }
    return true;
}

void write_dataframe_to_file(
        const StreamId& stream_id, const std::string& path, const py::tuple& item, const py::object& norm,
        const py::object& user_meta
) {
    ARCTICDB_SAMPLE(WriteDataframeToFile, 0)
    auto frame = convert::py_ndf_to_frame(stream_id, item, norm, user_meta, false);
    write_dataframe_to_file_internal(
            stream_id, frame, path, WriteOptions{}, codec::default_lz4_codec(), EncodingVersion::V2
    );
}

ReadResult read_dataframe_from_file(
        const StreamId& stream_id, const std::string& path, const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options, std::any& handler_data
) {

    auto release_gil = std::make_unique<py::gil_scoped_release>();
    auto opt_version_and_frame = read_dataframe_from_file_internal(
            stream_id, path, read_query, read_options, codec::default_lz4_codec(), handler_data
    );

    return create_python_read_result(
            opt_version_and_frame.versioned_item_,
            read_options.output_format(),
            std::move(opt_version_and_frame.frame_and_descriptor_)
    );
}

void PythonVersionStore::force_delete_symbol(const StreamId& stream_id) {
    version_map()->delete_all_versions(store(), stream_id);
    delete_all_for_stream(store(), stream_id, true);
    version_map()->flush();
}

VersionedItem PythonVersionStore::merge(
        const StreamId& stream_id, const py::tuple& source, const py::object& norm, const py::object& user_meta,
        const bool prune_previous_versions, const bool upsert, const py::tuple& py_strategy, std::vector<std::string> on
) {
    const MergeStrategy strategy{
            .matched = py_strategy[0].cast<MergeAction>(), .not_matched_by_target = py_strategy[1].cast<MergeAction>()
    };
    return merge_internal(
            stream_id,
            convert::py_ndf_to_frame(stream_id, source, norm, user_meta, cfg().write_options().empty_types()),
            prune_previous_versions,
            upsert,
            strategy,
            std::move(on)
    );
}

} // namespace arcticdb::version_store
