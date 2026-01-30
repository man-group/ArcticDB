/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/python_bindings.hpp>
#include <arcticdb/python/python_utils.hpp>

#include <arcticdb/util/variant.hpp>
#include <arcticdb/util/pybind_mutex.hpp>

#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_manager.hpp>
#include <arcticdb/storage/library_index.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/constants.hpp>
#include <arcticdb/storage/s3/s3_settings.hpp>
#include <arcticdb/storage/python_bindings_common.hpp>

namespace py = pybind11;

namespace arcticdb::storage::apy {

using namespace python_util;

std::shared_ptr<LibraryIndex> create_library_index(const std::string& environment_name, const py::object& py_envs) {
    arcticdb::proto::storage::EnvironmentConfigsMap envs;
    pb_from_python(py_envs, envs);
    auto env_by_id = convert_environment_config(envs);
    auto mem_resolver = create_in_memory_resolver(env_by_id);
    return std::make_shared<LibraryIndex>(EnvironmentName{environment_name}, mem_resolver);
}

void register_bindings(py::module& storage, py::exception<arcticdb::ArcticException>& base_exception) {
    storage.attr("CONFIG_LIBRARY_NAME") = py::str(arcticdb::storage::CONFIG_LIBRARY_NAME);

    py::enum_<ModifiableLibraryOption>(storage, "ModifiableLibraryOption", R"pbdoc(
         Library options that can be modified after library creation.
 
         See also `ModifiableEnterpriseLibraryOption` for enterprise options that can be modified.
 
         See `LibraryOptions` for a description of each option.
     )pbdoc")
            .value("DEDUP", ModifiableLibraryOption::DEDUP)
            .value("ROWS_PER_SEGMENT", ModifiableLibraryOption::ROWS_PER_SEGMENT)
            .value("COLUMNS_PER_SEGMENT", ModifiableLibraryOption::COLUMNS_PER_SEGMENT)
            .value("RECURSIVE_NORMALIZERS", ModifiableLibraryOption::RECURSIVE_NORMALIZERS);

    py::enum_<ModifiableEnterpriseLibraryOption>(storage, "ModifiableEnterpriseLibraryOption", R"pbdoc(
         Enterprise library options that can be modified after library creation.
 
         See also `ModifiableLibraryOption` for non-enterprise options that can be modified.
 
         See `EnterpriseLibraryOptions` for a description of each option.
     )pbdoc")
            .value("REPLICATION", ModifiableEnterpriseLibraryOption::REPLICATION)
            .value("BACKGROUND_DELETION", ModifiableEnterpriseLibraryOption::BACKGROUND_DELETION);

    py::register_exception<UnknownLibraryOption>(storage, "UnknownLibraryOption", base_exception.ptr());
    py::register_exception<UnsupportedLibraryOptionValue>(
            storage, "UnsupportedLibraryOptionValue", base_exception.ptr()
    );

    storage.def("create_library_index", &create_library_index);

    register_common_storage_bindings(storage, BindingScope::GLOBAL);

    py::class_<S3Override>(storage, "S3Override")
            .def(py::init<>())
            .def_property("credential_name", &S3Override::credential_name, &S3Override::set_credential_name)
            .def_property("credential_key", &S3Override::credential_key, &S3Override::set_credential_key)
            .def_property("endpoint", &S3Override::endpoint, &S3Override::set_endpoint)
            .def_property("bucket_name", &S3Override::bucket_name, &S3Override::set_bucket_name)
            .def_property("region", &S3Override::region, &S3Override::set_region)
            .def_property(
                    "use_virtual_addressing",
                    &S3Override::use_virtual_addressing,
                    &S3Override::set_use_virtual_addressing
            )
            .def_property("ca_cert_path", &S3Override::ca_cert_path, &S3Override::set_ca_cert_path)
            .def_property("ca_cert_dir", &S3Override::ca_cert_dir, &S3Override::set_ca_cert_dir)
            .def_property("https", &S3Override::https, &S3Override::set_https)
            .def_property("ssl", &S3Override::ssl, &S3Override::set_ssl);

    py::class_<GCPXMLOverride>(storage, "GCPXMLOverride").def(py::init<>());

    py::class_<AzureOverride>(storage, "AzureOverride")
            .def(py::init<>())
            .def_property("container_name", &AzureOverride::container_name, &AzureOverride::set_container_name)
            .def_property("endpoint", &AzureOverride::endpoint, &AzureOverride::set_endpoint)
            .def_property("ca_cert_path", &AzureOverride::ca_cert_path, &AzureOverride::set_ca_cert_path)
            .def_property("ca_cert_dir", &AzureOverride::ca_cert_dir, &AzureOverride::set_ca_cert_dir);

    py::class_<LmdbOverride>(storage, "LmdbOverride")
            .def(py::init<>())
            .def_property("path", &LmdbOverride::path, &LmdbOverride::set_path)
            .def_property("map_size", &LmdbOverride::map_size, &LmdbOverride::set_map_size);

    py::class_<StorageOverride>(storage, "StorageOverride")
            .def(py::init<>())
            .def("set_s3_override", &StorageOverride::set_s3_override)
            .def("set_azure_override", &StorageOverride::set_azure_override)
            .def("set_lmdb_override", &StorageOverride::set_lmdb_override)
            .def("set_gcpxml_override", &StorageOverride::set_gcpxml_override);

    py::class_<LibraryManager::LibraryWithConfig>(storage, "LibraryWithConfig")
            .def_property_readonly(
                    "config",
                    [](const LibraryManager::LibraryWithConfig& library_with_config) {
                        return pb_to_python(library_with_config.config);
                    }
            )
            .def_property_readonly("library", [](const LibraryManager::LibraryWithConfig& library_with_config) {
                return library_with_config.library;
            });

    py::class_<LibraryManager, std::shared_ptr<LibraryManager>>(storage, "LibraryManager")
            .def(py::init<std::shared_ptr<storage::Library>>())
            .def(
                    "write_library_config",
                    [](const LibraryManager& library_manager,
                       py::object& lib_cfg,
                       std::string_view library_path,
                       const StorageOverride& storage_override,
                       const bool validate) {
                        LibraryPath lib_path{library_path, '.'};
                        return library_manager.write_library_config(lib_cfg, lib_path, storage_override, validate);
                    },
                    py::arg("lib_cfg"),
                    py::arg("library_path"),
                    py::arg("override") = StorageOverride{},
                    py::arg("test_only_validation_toggle") = false
            )
            .def(
                    "modify_library_option",
                    [](const LibraryManager& library_manager,
                       std::string_view library_path,
                       std::variant<ModifiableLibraryOption, ModifiableEnterpriseLibraryOption>
                               option,
                       std::variant<bool, int64_t>
                               new_value) {
                        LibraryPath lib_path{library_path, '.'};
                        return library_manager.modify_library_option(lib_path, option, new_value);
                    },
                    py::arg("library_path"),
                    py::arg("option"),
                    py::arg("new_value")
            )
            .def(
                    "get_library_config",
                    [](const LibraryManager& library_manager,
                       std::string_view library_path,
                       const StorageOverride& storage_override) {
                        return library_manager.get_library_config(LibraryPath{library_path, '.'}, storage_override);
                    },
                    py::arg("library_path"),
                    py::arg("override") = StorageOverride{}
            )
            .def(
                    "is_library_config_ok",
                    [](const LibraryManager& library_manager, std::string_view library_path, bool throw_on_failure) {
                        return library_manager.is_library_config_ok(LibraryPath{library_path, '.'}, throw_on_failure);
                    },
                    py::arg("library_path"),
                    py::arg("throw_on_failure") = true
            )
            .def(
                    "remove_library_config",
                    [](const LibraryManager& library_manager, std::string_view library_path) {
                        return library_manager.remove_library_config(LibraryPath{library_path, '.'});
                    },
                    py::call_guard<SingleThreadMutexHolder>()
            )
            .def(
                    "get_library",
                    [](LibraryManager& library_manager,
                       std::string_view library_path,
                       const StorageOverride& storage_override,
                       const bool ignore_cache,
                       const NativeVariantStorage& native_storage_config) {
                        return library_manager.get_library(
                                LibraryPath{library_path, '.'}, storage_override, ignore_cache, native_storage_config
                        );
                    },
                    py::arg("library_path"),
                    py::arg("storage_override") = StorageOverride{},
                    py::arg("ignore_cache") = false,
                    py::arg("native_storage_config") = std::nullopt
            )
            .def("cleanup_library_if_open",
                 [](LibraryManager& library_manager, std::string_view library_path) {
                     return library_manager.cleanup_library_if_open(LibraryPath{library_path, '.'});
                 })
            .def("has_library",
                 [](const LibraryManager& library_manager, std::string_view library_path) {
                     return library_manager.has_library(LibraryPath{library_path, '.'});
                 })
            .def("list_libraries", [](const LibraryManager& library_manager) {
                std::vector<std::string> res;
                for (auto& lp : library_manager.get_library_paths()) {
                    res.emplace_back(lp.to_delim_path());
                }
                return res;
            });
}

} // namespace arcticdb::storage::apy
