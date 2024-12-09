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
#include <arcticdb/storage/s3/s3_settings.hpp>

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

void register_bindings(py::module& storage, py::exception<arcticdb::ArcticException>& base_exception) {
    storage.attr("CONFIG_LIBRARY_NAME") = py::str(arcticdb::storage::CONFIG_LIBRARY_NAME);

    py::enum_<KeyType>(storage, "KeyType")
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
        .value("SLOW_LOCK", KeyType::ATOMIC_LOCK)
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

    py::enum_<ModifiableLibraryOption>(storage, "ModifiableLibraryOption", R"pbdoc(
        Library options that can be modified after library creation.

        See also `ModifiableEnterpriseLibraryOption` for enterprise options that can be modified.

        See `LibraryOptions` for a description of each option.
    )pbdoc")
            .value("DEDUP", ModifiableLibraryOption::DEDUP)
            .value("ROWS_PER_SEGMENT", ModifiableLibraryOption::ROWS_PER_SEGMENT)
            .value("COLUMNS_PER_SEGMENT", ModifiableLibraryOption::COLUMNS_PER_SEGMENT);

    py::enum_<ModifiableEnterpriseLibraryOption>(storage, "ModifiableEnterpriseLibraryOption", R"pbdoc(
        Enterprise library options that can be modified after library creation.

        See also `ModifiableLibraryOption` for non-enterprise options that can be modified.

        See `EnterpriseLibraryOptions` for a description of each option.
    )pbdoc")
            .value("REPLICATION", ModifiableEnterpriseLibraryOption::REPLICATION)
            .value("BACKGROUND_DELETION", ModifiableEnterpriseLibraryOption::BACKGROUND_DELETION);

    py::register_exception<UnknownLibraryOption>(storage, "UnknownLibraryOption", base_exception.ptr());
    py::register_exception<UnsupportedLibraryOptionValue>(storage, "UnsupportedLibraryOptionValue", base_exception.ptr());

    storage.def("create_library_index", &create_library_index);

    
    py::enum_<s3::AWSAuthMethod>(storage, "AWSAuthMethod")
        .value("DISABLED", s3::AWSAuthMethod::DISABLED)
        .value("DEFAULT_CREDENTIALS_PROVIDER_CHAIN", s3::AWSAuthMethod::DEFAULT_CREDENTIALS_PROVIDER_CHAIN)
        .value("STS_PROFILE_CREDENTIALS_PROVIDER", s3::AWSAuthMethod::STS_PROFILE_CREDENTIALS_PROVIDER);

    enum class S3SettingsPickleOrder : uint32_t {
        AWS_AUTH = 0,
        AWS_PROFILE = 1
    };

    py::class_<s3::S3Settings>(storage, "S3Settings")
        .def(py::init<s3::AWSAuthMethod, const std::string&>())
        .def(py::pickle(
            [](const s3::S3Settings &settings) {
                return py::make_tuple(settings.aws_auth(), settings.aws_profile());
            },
            [](py::tuple t) {
                util::check(t.size() == 2, "Invalid S3Settings pickle objects");
                s3::S3Settings settings(t[static_cast<uint32_t>(S3SettingsPickleOrder::AWS_AUTH)].cast<s3::AWSAuthMethod>(), t[static_cast<uint32_t>(S3SettingsPickleOrder::AWS_PROFILE)].cast<std::string>());
                return settings;
            }
        ))
        .def_property_readonly("aws_profile", [](const s3::S3Settings &settings) { return settings.aws_profile(); })
        .def_property_readonly("aws_auth", [](const s3::S3Settings &settings) { return settings.aws_auth(); });

    py::class_<NativeVariantStorage>(storage, "NativeVariantStorage")
        .def(py::init<>())
        .def(py::init<NativeVariantStorage::VariantStorageConfig>())
        .def(py::pickle(
            [](const NativeVariantStorage &settings) {
                return util::variant_match(settings.variant(),
                    [] (const s3::S3Settings& settings) {
                        return py::make_tuple(settings.aws_auth(), settings.aws_profile());
                    },
                    [](const auto &) {
                        util::raise_rte("Invalid native storage setting type");
                        return py::make_tuple();
                    }
                );
            },
            [](py::tuple t) {
                util::check(t.size() == 2, "Invalid S3Settings pickle objects");
                s3::S3Settings settings(t[static_cast<uint32_t>(S3SettingsPickleOrder::AWS_AUTH)].cast<s3::AWSAuthMethod>(), t[static_cast<uint32_t>(S3SettingsPickleOrder::AWS_PROFILE)].cast<std::string>());
                return NativeVariantStorage(std::move(settings));
            }
        ))
        .def("update", &NativeVariantStorage::update);
    py::implicitly_convertible<NativeVariantStorage::VariantStorageConfig, NativeVariantStorage>();

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
        .def_property("ssl", &S3Override::ssl, &S3Override::set_ssl);

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
        .def("modify_library_option", [](const LibraryManager& library_manager, std::string_view library_path,
                                         std::variant<ModifiableLibraryOption, ModifiableEnterpriseLibraryOption> option,
                                         std::variant<bool, int64_t> new_value) {
            LibraryPath lib_path{library_path, '.'};
            return library_manager.modify_library_option(lib_path, option, new_value);
        },
             py::arg("library_path"),
             py::arg("option"),
             py::arg("new_value"))
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
                const bool ignore_cache,
                const NativeVariantStorage& native_storage_config) {
            return library_manager.get_library(LibraryPath{library_path, '.'}, storage_override, ignore_cache, native_storage_config);
        },
             py::arg("library_path"),
             py::arg("storage_override") = StorageOverride{},
             py::arg("ignore_cache") = false,
             py::arg("native_storage_map") = std::nullopt
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
        })
        .def("get_library", [](LibraryIndex &library_index, const std::string &library_path, OpenMode open_mode = OpenMode::DELETE, const NativeVariantStorage& native_storage_config = NativeVariantStorage()) {
            LibraryPath path = LibraryPath::from_delim_path(library_path);
            return library_index.get_library(path, open_mode, UserAuth{}, native_storage_config);
        })
        ;
}

} // namespace arcticdb::storage::apy
