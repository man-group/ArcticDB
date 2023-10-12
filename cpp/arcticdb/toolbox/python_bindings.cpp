/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <pybind11/functional.h>

#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/s3/s3_storage_tool.hpp>
#include <arcticdb/toolbox/library_tool.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/version/symbol_list.hpp>

namespace arcticdb::toolbox::apy {

void register_bindings(py::module &m) {
    auto tools = m.def_submodule("tools", "Library management tool hooks");
    using namespace arcticdb::toolbox::apy;
    using namespace arcticdb::storage;

    tools.def("print_mem_usage", &util::print_total_mem_usage);

    py::class_<LibraryTool, std::shared_ptr<LibraryTool>>(tools, "LibraryTool")
            .def(py::init<>([](std::shared_ptr<Library> lib) {
                return std::make_shared<LibraryTool>(lib);
            }))
            .def("read_to_segment", &LibraryTool::read_to_segment)
            .def("write", &LibraryTool::write)
            .def("remove", &LibraryTool::remove)
            .def("find_keys", &LibraryTool::find_keys)
            .def("count_keys", &LibraryTool::count_keys)
            .def("get_key_path", &LibraryTool::get_key_path)
            .def("find_keys_for_id", &LibraryTool::find_keys_for_id)
            .def("clear_ref_keys", &LibraryTool::clear_ref_keys)
            .def("batch_key_exists", &LibraryTool::batch_key_exists);

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
            .def("lock", &StorageLockWrapper::lock)
            .def("unlock", &StorageLockWrapper::unlock)
            .def("lock_timeout", &StorageLockWrapper::lock_timeout)
            .def("try_lock", &StorageLockWrapper::try_lock)
            ;
}

} // namespace arcticdb::toolbox::apy
