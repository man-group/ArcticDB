/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/s3/s3_storage_tool.hpp>
#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/util/pybind_mutex.hpp>
#include <arcticdb/util/storage_lock.hpp>
#include <arcticdb/util/reliable_storage_lock.hpp>
#include <arcticdb/toolbox/library_tool.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/toolbox/storage_mover.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb::toolbox::apy {

void register_bindings(py::module& m, py::exception<arcticdb::ArcticException>& base_exception) {
    auto tools = m.def_submodule("tools", "Library management tool hooks");
    using namespace arcticdb::toolbox::apy;
    using namespace arcticdb::storage;

    tools.def("print_mem_usage", &util::print_total_mem_usage);
#ifdef WIN32
    // Manipulates the _environ in our statically linked msvcrt
    tools.def("putenv_s", &::_putenv_s);
#endif

    py::class_<LibraryTool, std::shared_ptr<LibraryTool>>(tools, "LibraryTool")
            .def(py::init<>([](std::shared_ptr<Library> lib) { return std::make_shared<LibraryTool>(lib); }))
            .def("read_to_segment", &LibraryTool::read_to_segment)
            .def("read_metadata", &LibraryTool::read_metadata)
            .def("key_exists", &LibraryTool::key_exists)
            .def("read_descriptor", &LibraryTool::read_descriptor, R"pbdoc(
                Gives the <StreamDescriptor> for a Variant key. The Stream Descriptor contains the <FieldRef>s for all fields in
                the value written for that key.

                E.g. an Index key will have fields like 'start_index', 'end_index', 'creation_ts', etc.
            )pbdoc")
            .def("read_timeseries_descriptor", &LibraryTool::read_timeseries_descriptor, R"pbdoc(
                Gives the <TimeseriesDescriptor> for a Variant key. The Timeseries Descriptor contains the <FieldRef>s for all
                fields in the dataframe written for the corresponding symbol.

                E.g. an Index key for a symbol which has columns "index" and "col" will have <FieldRef>s for those columns.
            )pbdoc")
            .def("write", &LibraryTool::write)
            .def("overwrite_segment_in_memory", &LibraryTool::overwrite_segment_in_memory)
            .def("item_to_segment_in_memory", &LibraryTool::item_to_segment_in_memory)
            .def("overwrite_append_data", &LibraryTool::overwrite_append_data)
            .def("remove", &LibraryTool::remove)
            .def("find_keys", &LibraryTool::find_keys)
            .def("count_keys", &LibraryTool::count_keys)
            .def("get_key_path", &LibraryTool::get_key_path)
            .def("find_keys_for_id", &LibraryTool::find_keys_for_id)
            .def("list_storage_locks",
                 [](LibraryTool& self) {
                     py::list result;
                     for (auto& info : self.list_storage_locks()) {
                         py::dict d;
                         d["name"] = info.name;
                         d["timestamp"] = info.acquire_time;
                         d["active"] = info.active;
                         d["metadata"] = python_util::any_metadata_to_py(info.metadata);
                         result.append(std::move(d));
                     }
                     return result;
                 })
            .def("clear_ref_keys", &LibraryTool::clear_ref_keys)
            .def("batch_key_exists", &LibraryTool::batch_key_exists, py::call_guard<SingleThreadMutexHolder>())
            .def("inspect_env_variable", &LibraryTool::inspect_env_variable)
            .def_static("read_unaltered_lib_cfg", &LibraryTool::read_unaltered_lib_cfg)
            .def("segment_in_memory_to_read_result", &LibraryTool::segment_in_memory_to_read_result);

    // Reliable storage lock exposed for integration testing. It is intended for use in C++
    using namespace arcticdb::lock;

    py::register_local_exception<LostReliableLock>(tools, "LostReliableLock", base_exception.ptr());

    py::class_<ReliableStorageLock<>>(tools, "ReliableStorageLock")
            .def(py::init<>([](std::string base_name, std::shared_ptr<Library> lib, timestamp timeout) {
                auto store = version_store::LocalVersionedEngine(lib)._test_get_store();
                return ReliableStorageLock<>(base_name, store, timeout);
            }));

    py::class_<ReliableStorageLockManager>(tools, "ReliableStorageLockManager")
            .def(py::init<>([]() { return ReliableStorageLockManager(); }))
            .def("take_lock_guard", &ReliableStorageLockManager::take_lock_guard)
            .def("free_lock_guard", &ReliableStorageLockManager::free_lock_guard);

    py::class_<StorageMover>(tools, "StorageMover")
            .def(py::init<std::shared_ptr<storage::Library>, std::shared_ptr<storage::Library>>())
            .def("go", &StorageMover::go, "start the storage mover copy", py::arg("batch_size") = 100)
            .def("get_keys_in_source_only", &StorageMover::get_keys_in_source_only)
            .def("get_all_source_keys", &StorageMover::get_all_source_keys, "get_all_source_keys")
            .def("incremental_copy", &StorageMover::incremental_copy, "incrementally copy keys")
            .def("write_keys_from_source_to_target",
                 &StorageMover::write_keys_from_source_to_target,
                 "write_keys_from_source_to_target")
            .def("write_symbol_trees_from_source_to_target",
                 &StorageMover::write_symbol_trees_from_source_to_target,
                 "write_symbol_trees_from_source_to_target")
            .def("clone_all_keys_for_symbol",
                 &StorageMover::clone_all_keys_for_symbol,
                 "Clone all the keys that have this symbol as id to the dest library.")
            .def("clone_all_keys_for_symbol_for_type",
                 &StorageMover::clone_all_keys_for_symbol_for_type,
                 "Clone all the keys that have this symbol and type to the dest library.");

    // S3 Storage tool
    using namespace arcticdb::storage::s3;
    py::class_<S3StorageTool, std::shared_ptr<S3StorageTool>>(tools, "S3Tool")
            .def(py::init<>(
                    [](const std::string& bucket_name,
                       const std::string& credential_name,
                       const std::string& credential_key,
                       const std::string& endpoint) -> std::shared_ptr<S3StorageTool> {
                        arcticc::pb2::s3_storage_pb2::Config cfg;
                        cfg.set_bucket_name(bucket_name);
                        cfg.set_credential_name(credential_name);
                        cfg.set_credential_key(credential_key);
                        cfg.set_endpoint(endpoint);
                        return std::make_shared<S3StorageTool>(cfg);
                    }
            ))
            .def("list_bucket", &S3StorageTool::list_bucket)
            .def("delete_bucket", &S3StorageTool::delete_bucket)
            .def("write_object", &S3StorageTool::set_object)
            .def("get_object", &S3StorageTool::get_object)
            .def("get_prefix_info", &S3StorageTool::get_prefix_info)
            .def("get_object_size", &S3StorageTool::get_file_size)
            .def("delete_object", &S3StorageTool::delete_object);

    // For tests:
    tools.add_object("CompactionId", py::str(arcticdb::CompactionId));
    tools.add_object("CompactionLockName", py::str(arcticdb::CompactionLockName));

    py::class_<StorageLockWrapper>(tools, "StorageLock")
            .def(
                    "lock",
                    [](StorageLockWrapper& self, const py::object& metadata) {
                        self.lock(python_util::py_metadata_to_any(metadata));
                    },
                    py::arg("metadata") = py::none()
            )
            .def("unlock", &StorageLockWrapper::unlock)
            .def(
                    "lock_timeout",
                    [](StorageLockWrapper& self, size_t timeout_ms, const py::object& metadata) {
                        self.lock_timeout(timeout_ms, python_util::py_metadata_to_any(metadata));
                    },
                    py::arg("timeout_ms"),
                    py::arg("metadata") = py::none()
            )
            .def(
                    "try_lock",
                    [](StorageLockWrapper& self, const py::object& metadata) {
                        return self.try_lock(python_util::py_metadata_to_any(metadata));
                    },
                    py::arg("metadata") = py::none()
            )
            .def("read_metadata",
                 [](StorageLockWrapper& self) { return python_util::any_metadata_to_py(self.read_metadata()); })
            .def("_test_release_local_lock", &StorageLockWrapper::_test_release_local_lock);

    using namespace arcticdb::query_stats;
    auto query_stats_module = tools.def_submodule("query_stats", "Query stats functionality");

    auto latency_to_py = [](const LatencyHistogram& h) {
        py::dict d;
        d["count"] = h.count;
        d["total_ms"] = h.total_ms();
        d["mean_ms"] = h.mean_ms();
        d["min_ms"] = h.min_ms();
        d["p50_ms"] = h.p50_ms();
        d["p95_ms"] = h.p95_ms();
        d["p99_ms"] = h.p99_ms();
        d["max_ms"] = h.max_ms();
        return d;
    };

    auto storage_map_to_py = [&latency_to_py](const StorageStatsMap& smap) {
        py::dict backends;
        for (const auto& [backend, ops] : smap) {
            py::dict ops_dict;
            for (const auto& [op, key_types] : ops) {
                py::dict kt_dict;
                for (const auto& [kt, st] : key_types) {
                    py::dict s;
                    s["ok"] = st.ok;
                    s["not_found"] = st.not_found;
                    s["conflict"] = st.conflict;
                    s["unsupported"] = st.unsupported;
                    s["error"] = st.error;
                    s["count"] = st.total_count();
                    s["bytes"] = st.bytes;
                    s["items"] = st.items;
                    s["pages"] = st.pages;
                    s["partial_delete_retries"] = st.partial_delete_retries;
                    s["latency"] = latency_to_py(st.latency);
                    s["error_latency"] = latency_to_py(st.error_latency);
                    kt_dict[py::str(kt)] = std::move(s);
                }
                ops_dict[py::str(to_string(op))] = std::move(kt_dict);
            }
            backends[py::str(to_string(backend))] = std::move(ops_dict);
        }
        return backends;
    };

    auto http_map_to_py = [&latency_to_py](const HttpStatsMap& hmap) {
        py::dict services;
        for (const auto& [svc, ops] : hmap) {
            py::dict ops_dict;
            for (const auto& [op, st] : ops) {
                py::dict s;
                s["requests"] = st.requests;
                s["attempts"] = st.attempts;
                s["retries"] = st.retries;
                s["status_2xx"] = st.status_2xx;
                s["status_3xx"] = st.status_3xx;
                s["status_4xx"] = st.status_4xx;
                s["status_5xx"] = st.status_5xx;
                s["throttled"] = st.throttled;
                s["network_errors"] = st.network_errors;
                s["checksum_mismatch_retries"] = st.checksum_mismatch_retries;
                s["new_connections"] = st.new_connections;
                s["backoff_time_ms"] = static_cast<double>(st.backoff_time_ns) / 1e6;
                s["request_latency"] = latency_to_py(st.request_latency);
                s["attempt_latency"] = latency_to_py(st.attempt_latency);
                s["dns_latency"] = latency_to_py(st.dns_latency);
                s["tcp_connect_latency"] = latency_to_py(st.tcp_connect_latency);
                s["tls_handshake_latency"] = latency_to_py(st.tls_handshake_latency);
                s["ttfb_latency"] = latency_to_py(st.ttfb_latency);
                s["transfer_latency"] = latency_to_py(st.transfer_latency);
                ops_dict[py::str(op)] = std::move(s);
            }
            services[py::str(svc)] = std::move(ops_dict);
        }
        return services;
    };

    auto api_map_to_py = [&latency_to_py](const ApiStatsMap& amap) {
        py::dict out;
        for (const auto& [op, st] : amap) {
            py::dict s;
            s["ok"] = st.ok;
            s["error"] = st.error;
            s["count"] = st.total_count();
            s["latency"] = latency_to_py(st.latency);
            s["error_latency"] = latency_to_py(st.error_latency);
            out[py::str(to_string(op))] = std::move(s);
        }
        return out;
    };

    auto symbol_map_to_py = [&latency_to_py](const SymbolStatsMap& sym_map) {
        py::dict out;
        for (const auto& [sym, st] : sym_map) {
            py::dict s;
            s["reads"] = st.reads;
            s["writes"] = st.writes;
            s["deletes"] = st.deletes;
            s["exists"] = st.exists;
            s["lists"] = st.lists;
            s["errors"] = st.errors;
            s["total_ops"] = st.total_ops();
            s["bytes_read"] = st.bytes_read;
            s["bytes_written"] = st.bytes_written;
            s["http_requests"] = st.http_requests;
            s["http_retries"] = st.http_retries;
            s["storage_latency"] = latency_to_py(st.storage_latency);
            out[py::str(sym)] = std::move(s);
        }
        return out;
    };

    auto thread_pool_to_py = [&latency_to_py](const ThreadPoolStats& tp) {
        py::dict d;
        d["tasks"] = tp.tasks;
        d["expired"] = tp.expired;
        d["wait_latency"] = latency_to_py(tp.wait_latency);
        d["run_latency"] = latency_to_py(tp.run_latency);
        return d;
    };

    auto snapshot_to_py = [storage_map_to_py,
                           http_map_to_py,
                           api_map_to_py,
                           symbol_map_to_py,
                           latency_to_py,
                           thread_pool_to_py](const QueryStatsSnapshot& snap) {
        py::dict out;
        out["wall_time_ms"] = snap.wall_time_ms;
        out["storage"] = storage_map_to_py(snap.storage);
        out["http"] = http_map_to_py(snap.http);
        out["api"] = api_map_to_py(snap.api);

        py::dict by_lib;
        for (const auto& [lib, lb] : snap.by_library) {
            py::dict ldict;
            ldict["storage"] = storage_map_to_py(lb.storage);
            ldict["http"] = http_map_to_py(lb.http);
            ldict["api"] = api_map_to_py(lb.api);
            ldict["by_symbol"] = symbol_map_to_py(lb.by_symbol);
            ldict["dropped_symbols"] = lb.dropped_symbols;
            by_lib[py::str(lib)] = std::move(ldict);
        }
        out["by_library"] = std::move(by_lib);
        out["by_symbol"] = symbol_map_to_py(snap.by_symbol);
        out["dropped_symbols"] = snap.dropped_symbols;

        py::dict tp_dict;
        tp_dict["cpu"] = thread_pool_to_py(snap.thread_pools.cpu);
        tp_dict["io"] = thread_pool_to_py(snap.thread_pools.io);
        tp_dict["semaphore_waits"] = snap.thread_pools.semaphore_waits;
        tp_dict["semaphore_wait_latency"] = latency_to_py(snap.thread_pools.semaphore_wait_latency);
        out["thread_pools"] = std::move(tp_dict);

        py::dict vm_dict;
        vm_dict["hits"] = snap.caches.version_map.hits;
        vm_dict["misses"] = snap.caches.version_map.misses;
        vm_dict["reloads"] = snap.caches.version_map.reloads;
        vm_dict["ref_retries"] = snap.caches.version_map.ref_retries;
        vm_dict["chain_hops"] = snap.caches.version_map.chain_hops;

        py::dict dd_dict;
        dd_dict["hits"] = snap.caches.dedup_map.hits;
        dd_dict["misses"] = snap.caches.dedup_map.misses;
        dd_dict["bytes_saved"] = snap.caches.dedup_map.bytes_saved;

        py::dict cc_dict;
        cc_dict["hits"] = snap.caches.config_cache.hits;
        cc_dict["misses"] = snap.caches.config_cache.misses;

        py::dict caches_dict;
        caches_dict["version_map"] = std::move(vm_dict);
        caches_dict["dedup_map"] = std::move(dd_dict);
        caches_dict["config_cache"] = std::move(cc_dict);
        out["caches"] = std::move(caches_dict);

        py::dict codec_dict;
        codec_dict["encodes"] = snap.codec.encodes;
        codec_dict["decodes"] = snap.codec.decodes;
        codec_dict["encode_uncompressed_bytes"] = snap.codec.encode_uncompressed_bytes;
        codec_dict["encode_compressed_bytes"] = snap.codec.encode_compressed_bytes;
        codec_dict["decode_compressed_bytes"] = snap.codec.decode_compressed_bytes;
        codec_dict["decode_uncompressed_bytes"] = snap.codec.decode_uncompressed_bytes;
        codec_dict["encode_compression_ratio"] = snap.codec.encode_compression_ratio();
        codec_dict["decode_compression_ratio"] = snap.codec.decode_compression_ratio();
        codec_dict["encode_latency"] = latency_to_py(snap.codec.encode_latency);
        codec_dict["decode_latency"] = latency_to_py(snap.codec.decode_latency);
        out["codec"] = std::move(codec_dict);

        py::dict query_dict;
        query_dict["queries"] = snap.query.queries;
        query_dict["segments_total"] = snap.query.segments_total;
        query_dict["segments_pruned_index"] = snap.query.segments_pruned_index;
        query_dict["segments_pruned_column_stats"] = snap.query.segments_pruned_column_stats;
        query_dict["segments_read"] = snap.query.segments_read;
        query_dict["columns_total"] = snap.query.columns_total;
        query_dict["columns_projected"] = snap.query.columns_projected;
        query_dict["rows_total"] = snap.query.rows_total;
        query_dict["rows_selected"] = snap.query.rows_selected;
        out["query"] = std::move(query_dict);

        py::dict locks_dict;
        for (const auto& [kind, lk] : snap.locks) {
            py::dict ld;
            ld["acquires"] = lk.acquires;
            ld["conflicts"] = lk.conflicts;
            ld["retries"] = lk.retries;
            ld["timeouts"] = lk.timeouts;
            ld["wait_latency"] = latency_to_py(lk.wait_latency);
            locks_dict[py::str(to_string(kind))] = std::move(ld);
        }
        out["locks"] = std::move(locks_dict);

        py::dict boundary_dict;
        for (const auto& [phase, st] : snap.boundary) {
            py::dict bd;
            bd["count"] = st.count;
            bd["latency"] = latency_to_py(st.latency);
            boundary_dict[py::str(to_string(phase))] = std::move(bd);
        }
        out["boundary"] = std::move(boundary_dict);

        py::dict clauses_dict;
        for (const auto& [kind, st] : snap.processing.clauses) {
            py::dict cd;
            cd["executions"] = st.executions;
            cd["rows_in"] = st.rows_in;
            cd["rows_out"] = st.rows_out;
            cd["latency"] = latency_to_py(st.latency);
            clauses_dict[py::str(to_string(kind))] = std::move(cd);
        }
        py::dict adm_dict;
        adm_dict["units_admitted"] = snap.processing.admission.units_admitted;
        adm_dict["residency_waits"] = snap.processing.admission.residency_waits;
        adm_dict["segments_dispatched"] = snap.processing.admission.segments_dispatched;
        adm_dict["read_window_waits"] = snap.processing.admission.read_window_waits;
        adm_dict["residency_wait_latency"] = latency_to_py(snap.processing.admission.residency_wait_latency);
        adm_dict["read_window_wait_latency"] = latency_to_py(snap.processing.admission.read_window_wait_latency);

        py::dict proc_dict;
        proc_dict["clauses"] = std::move(clauses_dict);
        proc_dict["admission"] = std::move(adm_dict);
        out["processing"] = std::move(proc_dict);

        py::dict rps_dict;
        rps_dict["count"] = snap.stream.rows_per_segment.count;
        rps_dict["sum"] = snap.stream.rows_per_segment.sum;
        rps_dict["mean"] = snap.stream.rows_per_segment.mean();
        rps_dict["min"] = snap.stream.rows_per_segment.min_value();
        rps_dict["p50"] = snap.stream.rows_per_segment.p50();
        rps_dict["p95"] = snap.stream.rows_per_segment.p95();
        rps_dict["p99"] = snap.stream.rows_per_segment.p99();
        rps_dict["max"] = snap.stream.rows_per_segment.max;

        py::dict comp_dict;
        for (const auto& [kind, st] : snap.stream.compactions) {
            py::dict cd;
            cd["count"] = st.count;
            cd["input_segments"] = st.input_segments;
            cd["output_segments"] = st.output_segments;
            cd["latency"] = latency_to_py(st.latency);
            comp_dict[py::str(to_string(kind))] = std::move(cd);
        }

        py::dict conf_dict;
        for (const auto& [kind, cnt] : snap.stream.conflicts) {
            conf_dict[py::str(to_string(kind))] = cnt;
        }

        py::dict stream_dict;
        stream_dict["data_segments_written"] = snap.stream.data_segments_written;
        stream_dict["staged_segments_written"] = snap.stream.staged_segments_written;
        stream_dict["small_segments_written"] = snap.stream.small_segments_written;
        stream_dict["rows_per_segment"] = std::move(rps_dict);
        stream_dict["incomplete_chain_loads"] = snap.stream.incomplete_chain_loads;
        stream_dict["incomplete_chain_hops"] = snap.stream.incomplete_chain_hops;
        stream_dict["incomplete_iteration_loads"] = snap.stream.incomplete_iteration_loads;
        stream_dict["incomplete_iteration_segments"] = snap.stream.incomplete_iteration_segments;
        stream_dict["compactions"] = std::move(comp_dict);
        stream_dict["conflicts"] = std::move(conf_dict);
        out["stream"] = std::move(stream_dict);

        auto pool_gauge_to_py = [](const PoolGaugeSnapshot& pg) {
            py::dict d;
            d["pending_tasks"] = pg.pending_tasks;
            d["active_threads"] = pg.active_threads;
            d["total_threads"] = pg.total_threads;
            d["max_threads"] = pg.max_threads;
            return d;
        };
        py::dict lmdb_dict;
        lmdb_dict["open_envs"] = snap.resources.lmdb.open_envs;
        lmdb_dict["map_size_bytes"] = snap.resources.lmdb.map_size_bytes;
        lmdb_dict["used_bytes"] = snap.resources.lmdb.used_bytes;
        lmdb_dict["active_readers"] = snap.resources.lmdb.active_readers;
        lmdb_dict["map_full_errors"] = snap.resources.lmdb.map_full_errors;

        py::dict res_dict;
        res_dict["cpu_pool"] = pool_gauge_to_py(snap.resources.cpu_pool);
        res_dict["io_pool"] = pool_gauge_to_py(snap.resources.io_pool);
        res_dict["write_semaphore_in_flight"] = snap.resources.write_semaphore_in_flight;
        res_dict["write_semaphore_limit"] = snap.resources.write_semaphore_limit;
        res_dict["version_map_entries"] = snap.resources.version_map_entries;
        res_dict["config_cache_libraries"] = snap.resources.config_cache_libraries;
        res_dict["config_cache_storages"] = snap.resources.config_cache_storages;
        res_dict["lmdb"] = std::move(lmdb_dict);
        out["resources"] = std::move(res_dict);

        return out;
    };

    py::class_<CollectorOptions>(query_stats_module, "CollectorOptions")
            .def(py::init<>())
            .def_readwrite("by_library", &CollectorOptions::by_library)
            .def_readwrite("by_symbol", &CollectorOptions::by_symbol)
            .def_readwrite("max_symbols", &CollectorOptions::max_symbols)
            .def_readwrite("isolate", &CollectorOptions::isolate);

    py::class_<CollectorSession, std::shared_ptr<CollectorSession>>(query_stats_module, "CollectorSession")
            .def(py::init<>([](bool by_library, bool by_symbol, size_t max_symbols, bool isolate) {
                     CollectorOptions opts;
                     opts.by_library = by_library;
                     opts.by_symbol = by_symbol;
                     opts.max_symbols = max_symbols;
                     opts.isolate = isolate;
                     return std::make_shared<CollectorSession>(opts, /*auto_enter=*/false);
                 }),
                 py::arg("by_library") = true,
                 py::arg("by_symbol") = false,
                 py::arg("max_symbols") = 1000,
                 py::arg("isolate") = false)
            .def("enter", &CollectorSession::enter)
            .def("exit", &CollectorSession::exit)
            .def("is_entered", &CollectorSession::is_entered)
            .def("reset", [](CollectorSession& self) { self.collector()->reset(); })
            .def("snapshot",
                 [snapshot_to_py](const CollectorSession& self) { return snapshot_to_py(self.snapshot()); });

    query_stats_module.def("reset_stats", []() { QueryStats::instance()->reset_stats(); });
    query_stats_module.def("enable", []() { QueryStats::instance()->enable(); });
    query_stats_module.def("disable", []() { QueryStats::instance()->disable(); });
    query_stats_module.def("is_enabled", []() { return QueryStats::instance()->is_enabled(); });
    query_stats_module.def("is_active", []() { return arcticdb::query_stats::is_active(); });
    query_stats_module.def("record_boundary_normalize", [](uint64_t duration_ns) {
        arcticdb::query_stats::record_boundary_op(BoundaryPhase::Normalize, duration_ns);
    });
    query_stats_module.def("record_boundary_denormalize", [](uint64_t duration_ns) {
        arcticdb::query_stats::record_boundary_op(BoundaryPhase::Denormalize, duration_ns);
    });
    query_stats_module.def("get_stats", []() { return QueryStats::instance()->get_stats(); });
    query_stats_module.def("get_structured_stats", [snapshot_to_py]() {
        return snapshot_to_py(QueryStats::instance()->get_structured_snapshot());
    });
    query_stats_module.def(
            "configure_prometheus",
            [](const std::string& host,
               const std::string& port,
               const std::string& job_name,
               const std::string& instance,
               const std::string& prometheus_env,
               const std::string& model) {
                arcticdb::MetricsConfig::Model m = arcticdb::MetricsConfig::Model::PULL;
                if (model == "pull" || model == "PULL") {
                    m = arcticdb::MetricsConfig::Model::PULL;
                } else if (model == "push" || model == "PUSH") {
                    m = arcticdb::MetricsConfig::Model::PUSH;
                } else {
                    user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                            "Unsupported Prometheus model '{}'. Expected 'pull' or 'push'.", model
                    );
                }
                arcticdb::MetricsConfig cfg{host, port, job_name, instance, prometheus_env, m};
                arcticdb::PrometheusInstance::instance()->configure(cfg);
            },
            py::arg("host") = "127.0.0.1",
            py::arg("port") = "9108",
            py::arg("job_name") = "arcticdb",
            py::arg("instance") = "local",
            py::arg("prometheus_env") = "local",
            py::arg("model") = "pull"
    );
    query_stats_module.def("push_prometheus_metrics", []() {
        return arcticdb::PrometheusInstance::instance()->push();
    });
    query_stats_module.def(
            "enable_prometheus_sink",
            [](bool include_library) { arcticdb::query_stats::enable_prometheus_sink(include_library); },
            py::arg("include_library") = false
    );
    query_stats_module.def("disable_prometheus_sink", []() { arcticdb::query_stats::disable_prometheus_sink(); });
}
} // namespace arcticdb::toolbox::apy
