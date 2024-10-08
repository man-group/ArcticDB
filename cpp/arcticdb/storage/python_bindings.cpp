/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/python_bindings.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <pybind11/stl.h>

#include <arcticdb/util/variant.hpp>
#include <arcticdb/util/pybind_mutex.hpp>

#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_manager.hpp>
#include <arcticdb/storage/library_index.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/constants.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>

namespace py = pybind11;

namespace arcticdb::storage::apy {

using namespace python_util;

std::shared_ptr<LibraryIndex> create_library_index(const std::string &environment_name, const py::object &py_envs) {
    arcticdb::proto::storage::EnvironmentConfigsMap envs;
    pb_from_python(py_envs, envs);
    auto env_by_id = convert_environment_config(envs);
    auto mem_resolver = create_in_memory_resolver(env_by_id);
    return std::make_shared<LibraryIndex>(EnvironmentName{environment_name}, mem_resolver);
}

void register_bindings(py::module& storage) {
    storage.attr("CONFIG_LIBRARY_NAME") = py::str(arcticdb::storage::CONFIG_LIBRARY_NAME);

    py::enum_<KeyType>(storage, "KeyType")
        .value("STREAM_GROUP", KeyType::STREAM_GROUP)
        .value("VERSION", KeyType::VERSION)
        .value("VERSION_JOURNAL", KeyType::VERSION_JOURNAL)
        .value("GENERATION", KeyType::GENERATION)
        .value("TABLE_DATA", KeyType::TABLE_DATA)
        .value("TABLE_INDEX", KeyType::TABLE_INDEX)
        .value("METRICS", KeyType::METRICS)
        .value("SNAPSHOT", KeyType::SNAPSHOT)
        .value("SYMBOL_LIST", KeyType::SYMBOL_LIST)
        .value("VERSION_REF", KeyType::VERSION_REF)
        .value("STORAGE_INFO", KeyType::STORAGE_INFO)
        .value("APPEND_REF", KeyType::APPEND_REF)
        .value("LOCK", KeyType::LOCK)
        .value("SNAPSHOT_REF", KeyType::SNAPSHOT_REF)
        .value("TOMBSTONE", KeyType::TOMBSTONE)
        .value("APPEND_DATA", KeyType::APPEND_DATA)
        .value("MULTI_KEY", KeyType::MULTI_KEY)
        .value("LOG", KeyType::LOG)
        .value("PARTITION", KeyType::PARTITION)
        .value("OFFSET", KeyType::OFFSET)
        .value("BACKUP_SNAPSHOT_REF", KeyType::BACKUP_SNAPSHOT_REF)
        .value("TOMBSTONE_ALL", KeyType::TOMBSTONE_ALL)
        .value("SNAPSHOT_TOMBSTONE", KeyType::SNAPSHOT_TOMBSTONE)
        .value("LOG_COMPACTED", KeyType::LOG_COMPACTED)
        .value("COLUMN_STATS", KeyType::COLUMN_STATS)
        ;

    py::enum_<OpenMode>(storage, "OpenMode")
        .value("READ", OpenMode::READ)
        .value("WRITE", OpenMode::WRITE)
        .value("DELETE", OpenMode::DELETE);

    storage.def("create_library_index", &create_library_index);

    storage.def("create_mem_config_resolver", [](const py::object & env_config_map_py) -> std::shared_ptr<ConfigResolver> {
        arcticdb::proto::storage::EnvironmentConfigsMap ecm;
        pb_from_python(env_config_map_py, ecm);
        auto resolver = std::make_shared<storage::details::InMemoryConfigResolver>();
        for(auto &[env, cfg] :ecm.env_by_id()){
            EnvironmentName env_name{env};
            for(auto &[id, variant_storage]: cfg.storage_by_id()){
                resolver->add_storage(env_name, StorageName{id}, variant_storage);
            }
            for(auto &[id, lib_desc]: cfg.lib_by_path()){
                resolver->add_library(env_name, lib_desc);
            }
        }
        return resolver;
    });

    py::class_<ConfigResolver, std::shared_ptr<ConfigResolver>>(storage, "ConfigResolver");

    py::class_<Library, std::shared_ptr<Library>>(storage, "Library")
        .def_property_readonly("library_path", [](const Library &library){ return library.library_path().to_delim_path(); })
        .def_property_readonly("open_mode", [](const Library &library){ return library.open_mode(); })
        .def_property_readonly("config", [](const Library & library) {
            return util::variant_match(library.config(),
                                       [](const arcticdb::proto::storage::VersionStoreConfig & cfg){
                                           return pb_to_python(cfg);
                                       },
                                       [](const std::monostate & ){
                                            auto none = py::none{};
                                            none.inc_ref();
                                            return none.cast<py::object>();
                                       });
        })
        ;

    py::class_<S3Override>(storage, "S3Override")
        .def(py::init<>())
        .def_property("credential_name", &S3Override::credential_name, &S3Override::set_credential_name)
        .def_property("credential_key", &S3Override::credential_key, &S3Override::set_credential_key)
        .def_property("endpoint", &S3Override::endpoint, &S3Override::set_endpoint)
        .def_property("bucket_name", &S3Override::bucket_name, &S3Override::set_bucket_name)
        .def_property("region", &S3Override::region, &S3Override::set_region)
        .def_property(
                "use_virtual_addressing", &S3Override::use_virtual_addressing, &S3Override::set_use_virtual_addressing)
        .def_property("ca_cert_path", &S3Override::ca_cert_path, &S3Override::set_ca_cert_path)
        .def_property("ca_cert_dir", &S3Override::ca_cert_dir, &S3Override::set_ca_cert_dir)
        .def_property("https", &S3Override::https, &S3Override::set_https)
        .def_property("ssl", &S3Override::ssl, &S3Override::set_ssl)
        .def_property("aws_auth",
            // pybind cannot smartly convert AWSAuthMethod defined in proto from C++ layer to Python layer
            // AWSAuthMethod in python is just a integer but in C++ it is an enum class
            // so we need to convert it manually
            [](S3Override &s3_override) {
                return static_cast<size_t>(s3_override.aws_auth());
            }, 
            [](S3Override &s3_override, size_t aws_auth) {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    aws_auth <= static_cast<std::underlying_type<arcticdb::proto::storage::AWSAuthMethod>::type>(arcticdb::proto::storage::AWSAuthMethod::STS_PROFILE_CREDENTIALS_PROVIDER),
                    "Invalid AWSAuthMethod"
                );
                s3_override.set_aws_auth(static_cast<arcticdb::proto::storage::AWSAuthMethod>(aws_auth));
            })
        .def_property("aws_profile", &S3Override::aws_profile, &S3Override::set_aws_profile);

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
        .def("set_lmdb_override", &StorageOverride::set_lmdb_override);

    py::class_<LibraryManager, std::shared_ptr<LibraryManager>>(storage, "LibraryManager")
        .def(py::init<std::shared_ptr<storage::Library>>())
        .def("write_library_config", [](const LibraryManager& library_manager, py::object& lib_cfg,
                                        std::string_view library_path, const StorageOverride& storage_override, const bool validate) {
            LibraryPath lib_path{library_path, '.'};
            return library_manager.write_library_config(lib_cfg, lib_path, storage_override, validate);
        },
            py::arg("lib_cfg"),
            py::arg("library_path"),
            py::arg("override") = StorageOverride{},
            py::arg("test_only_validation_toggle") = false)
        .def("get_library_config", [](const LibraryManager& library_manager, std::string_view library_path, const StorageOverride& storage_override){
            return library_manager.get_library_config(LibraryPath{library_path, '.'}, storage_override);
        }, py::arg("library_path"), py::arg("override") = StorageOverride{})
        .def("is_library_config_ok", [](const LibraryManager& library_manager, std::string_view library_path, bool throw_on_failure) {
            return library_manager.is_library_config_ok(LibraryPath{library_path, '.'}, throw_on_failure);
        }, py::arg("library_path"), py::arg("throw_on_failure") = true)
        .def("remove_library_config", [](const LibraryManager& library_manager, std::string_view library_path){
            return library_manager.remove_library_config(LibraryPath{library_path, '.'});
        }, py::call_guard<SingleThreadMutexHolder>())
        .def("get_library", [](
                LibraryManager& library_manager, std::string_view library_path,
                const StorageOverride& storage_override,
                const bool ignore_cache) {
            return library_manager.get_library(LibraryPath{library_path, '.'}, storage_override, ignore_cache);
        },
             py::arg("library_path"),
             py::arg("storage_override") = StorageOverride{},
             py::arg("ignore_cache") = false
        )
        .def("cleanup_library_if_open", [](LibraryManager& library_manager, std::string_view library_path) {
            return library_manager.cleanup_library_if_open(LibraryPath{library_path, '.'});
        })
        .def("has_library", [](const LibraryManager& library_manager, std::string_view library_path){
            return library_manager.has_library(LibraryPath{library_path, '.'});
        })
        .def("list_libraries", [](const LibraryManager& library_manager){
            std::vector<std::string> res;
            for(auto & lp:library_manager.get_library_paths()){
                res.emplace_back(lp.to_delim_path());
            }
            return res;
        });

    py::class_<LibraryIndex, std::shared_ptr<LibraryIndex>>(storage, "LibraryIndex")
        .def(py::init<>([](const std::string &environment_name) {
                 auto resolver = std::make_shared<details::InMemoryConfigResolver>();
                 return std::make_unique<LibraryIndex>(EnvironmentName{environment_name}, resolver);
             })
        )
        .def_static("create_from_resolver", [](const std::string &environment_name, std::shared_ptr<ConfigResolver> resolver){
            return std::make_shared<LibraryIndex>(EnvironmentName{environment_name}, resolver);
        })
        .def("list_libraries", [](LibraryIndex &library_index, std::string_view prefix = ""){
            std::vector<std::string> res;
            for(const auto& lp:library_index.list_libraries(prefix)){
                res.emplace_back(lp.to_delim_path());
            }
            return res;
        } )
        .def("get_library", [](LibraryIndex &library_index, const std::string &library_path, OpenMode open_mode = OpenMode::DELETE) {
            LibraryPath path = LibraryPath::from_delim_path(library_path);
            return library_index.get_library(path, open_mode, UserAuth{});
        })
        ;
}

} // namespace arcticdb::storage::apy
