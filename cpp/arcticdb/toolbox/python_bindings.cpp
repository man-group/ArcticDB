/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <pybind11/functional.h>

#include <arcticdb/python/adapt_read_dataframe.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/s3/s3_storage_tool.hpp>
#include <arcticdb/toolbox/library_tool.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/util/pybind_mutex.hpp>
#include <arcticdb/util/storage_lock.hpp>
#include <arcticdb/util/reliable_storage_lock.hpp>
#include <arcticdb/toolbox/storage_mover.hpp>

namespace arcticdb::toolbox::apy {

void register_bindings(py::module &m, py::exception<arcticdb::ArcticException>& base_exception) {
    auto tools = m.def_submodule("tools", "Library management tool hooks");
    using namespace arcticdb::toolbox::apy;
    using namespace arcticdb::storage;

    tools.def("print_mem_usage", &util::print_total_mem_usage);
#ifdef WIN32
    // Manipulates the _environ in our statically linked msvcrt
    tools.def("putenv_s", &::_putenv_s);
#endif


    py::class_<LibraryTool, std::shared_ptr<LibraryTool>>(tools, "LibraryTool")
            .def(py::init<>([](std::shared_ptr<Library> lib) {
                return std::make_shared<LibraryTool>(lib);
            }))
            .def("read_to_segment", 
                [](LibraryTool& lt, const VariantKey& key) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "read_to_segment")
                    return lt.read_to_segment(key);
                })
            .def("read_metadata", 
                [](LibraryTool& lt, const VariantKey& key) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "read_metadata")
                    return lt.read_metadata(key);
                })
            .def("key_exists", 
                [](LibraryTool& lt, const VariantKey& key) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "key_exists")
                    return lt.key_exists(key);
                })
            .def("read_descriptor", 
                [](LibraryTool& lt, const VariantKey& key) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "read_descriptor")
                    return lt.read_descriptor(key);
                }, R"pbdoc(
                Gives the <StreamDescriptor> for a Variant key. The Stream Descriptor contains the <FieldRef>s for all fields in
                the value written for that key.

                E.g. an Index key will have fields like 'start_index', 'end_index', 'creation_ts', etc.
            )pbdoc")
            .def("read_timeseries_descriptor", 
                [](LibraryTool& lt, const VariantKey& key) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "read_timeseries_descriptor")
                    return lt.read_timeseries_descriptor(key);
                }, R"pbdoc(
                Gives the <TimeseriesDescriptor> for a Variant key. The Timeseries Descriptor contains the <FieldRef>s for all
                fields in the dataframe written for the corresponding symbol.

                E.g. an Index key for a symbol which has columns "index" and "col" will have <FieldRef>s for those columns.
            )pbdoc")
            .def("write", 
                [](LibraryTool& lt, const VariantKey& key, Segment& segment) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "write")
                    lt.write(key, segment);
                })
            .def("overwrite_segment_in_memory", 
                [](LibraryTool& lt, const VariantKey& key, SegmentInMemory& segment) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "overwrite_segment_in_memory")
                    lt.overwrite_segment_in_memory(key, segment);
                })
            .def("overwrite_append_data", 
                [](LibraryTool& lt, const VariantKey& key, const py::tuple& item, const py::object& norm, const py::object& user_meta) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "overwrite_append_data")
                    return lt.overwrite_append_data(key, item, norm, user_meta);
                })
            .def("remove", 
                [](LibraryTool& lt, const VariantKey& key) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "remove")
                    lt.remove(key);
                })
            .def("find_keys", 
                [](LibraryTool& lt, KeyType key_type) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "find_keys")
                    return lt.find_keys(key_type);
                })
            .def("count_keys", 
                [](LibraryTool& lt, KeyType key_type) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "count_keys")
                    return lt.count_keys(key_type);
                })
            .def("get_key_path", 
                [](LibraryTool& lt, const VariantKey& key) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "get_key_path")
                    return lt.get_key_path(key);
                })
            .def("find_keys_for_id", 
                [](LibraryTool& lt, entity::KeyType kt, const StreamId& stream_id) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "find_keys_for_id")
                    return lt.find_keys_for_id(kt, stream_id);
                })
            .def("clear_ref_keys", 
                [](LibraryTool& lt) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "clear_ref_keys")
                    lt.clear_ref_keys();
                })
            .def("batch_key_exists", 
                [](LibraryTool& lt, const std::vector<VariantKey>& keys) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "batch_key_exists")
                    return lt.batch_key_exists(keys);
                }, py::call_guard<SingleThreadMutexHolder>())
            .def("read_to_read_result",
                [&](LibraryTool& lt, const VariantKey& key){
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "read_to_read_result")
                    return adapt_read_df(lt.read(key));
                },
                "Read the most recent dataframe from the store")
            .def("inspect_env_variable", 
                [](LibraryTool& lt, const std::string& name) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "inspect_env_variable")
                    return lt.inspect_env_variable(name);
                })
            .def_static("read_unaltered_lib_cfg", 
                [](const storage::LibraryManager& lib_manager, const std::string& lib_name) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "read_unaltered_lib_cfg")
                    return LibraryTool::read_unaltered_lib_cfg(lib_manager, lib_name);
                });

    // Reliable storage lock exposed for integration testing. It is intended for use in C++
    using namespace arcticdb::lock;

    py::register_exception<LostReliableLock>(tools, "LostReliableLock", base_exception.ptr());

    py::class_<ReliableStorageLock<>>(tools, "ReliableStorageLock")
            .def(py::init<>([](std::string base_name, std::shared_ptr<Library> lib, timestamp timeout){
                auto store = version_store::LocalVersionedEngine(lib)._test_get_store();
                return ReliableStorageLock<>(base_name, store, timeout);
            }));

    py::class_<ReliableStorageLockManager>(tools, "ReliableStorageLockManager")
            .def(py::init<>([](){
                return ReliableStorageLockManager();
            }))
            // Fix for take_lock_guard - expected ReliableStorageLock not string
            .def("take_lock_guard", 
                [](ReliableStorageLockManager& manager, const ReliableStorageLock<>& lock) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "take_lock_guard")
                    manager.take_lock_guard(lock);
                })
            // Fix for free_lock_guard - takes no arguments
            .def("free_lock_guard", 
                [](ReliableStorageLockManager& manager) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "free_lock_guard")
                    manager.free_lock_guard();
                });


    py::class_<StorageMover>(tools, "StorageMover")
    .def(py::init<std::shared_ptr<storage::Library>, std::shared_ptr<storage::Library>>())
    .def("go",
        [](StorageMover& mover, size_t batch_size) {
            QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "go")
            return mover.go(batch_size);
        },
        "start the storage mover copy",
        py::arg("batch_size") = 100)
    .def("get_keys_in_source_only",
        [](StorageMover& mover) {
            QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "get_keys_in_source_only")
            return mover.get_keys_in_source_only();
        })
    .def("get_all_source_keys",
        [](StorageMover& mover) {
            QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "get_all_source_keys")
            return mover.get_all_source_keys();
        },
        "get_all_source_keys")
    .def("incremental_copy",
        [](StorageMover& mover, size_t batch_size, size_t thread_count, bool delete_keys, bool perform_checks) {
            QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "incremental_copy")
            return mover.incremental_copy(batch_size, thread_count, delete_keys, perform_checks);
        },
        "incrementally copy keys",
        py::arg("batch_size") = 1000,
        py::arg("thread_count") = 32,
        py::arg("delete_keys") = false,
        py::arg("perform_checks") = true)
    .def("write_keys_from_source_to_target",
        [](StorageMover& mover, const std::vector<py::object>& py_keys, size_t batch_size) {
            QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "write_keys_from_source_to_target")
            return mover.write_keys_from_source_to_target(py_keys, batch_size);
        },
        "write_keys_from_source_to_target",
        py::arg("py_keys"),
        py::arg("batch_size") = 100)
    .def("write_symbol_trees_from_source_to_target",
        [](StorageMover& mover, const std::vector<py::object>& py_partial_keys, bool append_versions) {
            QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "write_symbol_trees_from_source_to_target")
            return mover.write_symbol_trees_from_source_to_target(py_partial_keys, append_versions);
        },
        "write_symbol_trees_from_source_to_target")
    .def("clone_all_keys_for_symbol",
        [](StorageMover& mover, const StreamId& stream_id, size_t batch_size) {
            QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "clone_all_keys_for_symbol")
            return mover.clone_all_keys_for_symbol(stream_id, batch_size);
        },
        "Clone all the keys that have this symbol as id to the dest library.")
    .def("clone_all_keys_for_symbol_for_type",
        [](StorageMover& mover, const StreamId& stream_id, size_t batch_size, KeyType key_type) {
            QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "clone_all_keys_for_symbol_for_type")
            return mover.clone_all_keys_for_symbol_for_type(stream_id, batch_size, key_type);
        },
        "Clone all the keys that have this symbol and type to the dest library.");

    // S3 Storage tool
    using namespace arcticdb::storage::s3;
    py::class_<S3StorageTool, std::shared_ptr<S3StorageTool>>(tools, "S3Tool")
            .def(py::init<>([](
                    const std::string &bucket_name,
                    const std::string &credential_name,
                    const std::string &credential_key,
                    const std::string &endpoint) -> std::shared_ptr<S3StorageTool> {
                arcticc::pb2::s3_storage_pb2::Config cfg;
                cfg.set_bucket_name(bucket_name);
                cfg.set_credential_name(credential_name);
                cfg.set_credential_key(credential_key);
                cfg.set_endpoint(endpoint);
                return std::make_shared<S3StorageTool>(cfg);
            }))
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
            .def("lock", 
                [](StorageLockWrapper& lock) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "lock")
                    lock.lock();
                })
            .def("unlock", 
                [](StorageLockWrapper& lock) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "unlock")
                    lock.unlock();
                })
            .def("lock_timeout", 
                [](StorageLockWrapper& lock, size_t timeout_ms) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "lock_timeout")
                    return lock.lock_timeout(timeout_ms);
                })
            .def("try_lock", 
                [](StorageLockWrapper& lock) {
                    QUERY_STATS_ADD_GROUP_WITH_TIME(arcticdb_call, "try_lock")
                    return lock.try_lock();
                })
            ;


    using namespace arcticdb::util::query_stats;
    auto query_stats_module = tools.def_submodule("query_stats", "Query stats functionality");    
    py::enum_<GroupName>(query_stats_module, "GroupName")
        .value("arcticdb_call", GroupName::arcticdb_call)
        .value("key_type", GroupName::key_type)
        .value("encode_key_type", GroupName::encode_key_type)
        .value("decode_key_type", GroupName::decode_key_type)
        .value("storage_ops", GroupName::storage_ops)
        .export_values();
    
    py::enum_<StatsName>(query_stats_module, "StatsName")
        .value("result_count", StatsName::result_count)
        .value("total_time_ms", StatsName::total_time_ms)
        .value("count", StatsName::count)
        .value("encode_compressed_size_bytes", StatsName::encode_compressed_size_bytes)
        .value("encode_uncompressed_size_bytes", StatsName::encode_uncompressed_size_bytes)
        .value("decode_compressed_size_bytes", StatsName::decode_compressed_size_bytes)
        .value("decode_uncompressed_size_bytes", StatsName::decode_uncompressed_size_bytes)
        .export_values();
    
    py::class_<GroupingLevel, std::shared_ptr<GroupingLevel>>(query_stats_module, "GroupingLevel")
        .def(py::init<>())
        .def_readonly("stats", &GroupingLevel::stats_)
        .def_readonly("next_level_maps", &GroupingLevel::next_level_maps_);
        
    query_stats_module.def("root_levels", []() { 
        return QueryStats::instance().root_levels(async::TaskScheduler::instance()); 
    });
    query_stats_module.def("reset_stats", []() { 
        QueryStats::instance().reset_stats(); 
    }); 
    query_stats_module.def("enable", []() { 
        QueryStats::instance().enable(); 
    });
    query_stats_module.def("disable", []() { 
        QueryStats::instance().disable(); 
    });
    query_stats_module.def("is_enabled", []() { 
        return QueryStats::instance().is_enabled(); 
    });
}
} // namespace arcticdb::toolbox::apy
