#include <arcticdb/storage/python_bindings_common.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/library_index.hpp>
#include <arcticdb/storage/s3/s3_settings.hpp>

namespace py = pybind11;

namespace arcticdb::storage::apy {
using namespace python_util;

s3::GCPXMLSettings gcp_settings(const py::tuple& t) {
    static size_t py_object_size = 11;
    util::check(
            t.size() >= py_object_size,
            "Invalid GCPXMLSettings pickle objects, expected at least {} attributes but was {}",
            py_object_size,
            t.size()
    );
    util::warn(
            t.size() > py_object_size,
            "GCPXMLSettings py tuple expects {} attributes but has {}. Will continue by ignoring extra attributes.",
            py_object_size,
            t.size()
    );
    return s3::GCPXMLSettings{
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::AWS_AUTH)].cast<s3::AWSAuthMethod>(),
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::CA_CERT_PATH)].cast<std::string>(),
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::CA_CERT_DIR)].cast<std::string>(),
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::SSL)].cast<bool>(),
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::HTTPS)].cast<bool>(),
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::PREFIX)].cast<std::string>(),
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::ENDPOINT)].cast<std::string>(),
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::SECRET)].cast<std::string>(),
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::ACCESS)].cast<std::string>(),
            t[static_cast<uint32_t>(GCPXMLSettingsPickleOrder::BUCKET)].cast<std::string>()
    };
}

s3::S3Settings s3_settings(const py::tuple& t) {
    static size_t py_object_size = 4;
    util::check(t.size() >= py_object_size, "Invalid S3Settings pickle objects");
    util::warn(
            t.size() > py_object_size,
            "S3Settings py tuple expects {} attributes but has {}. Will continue by ignoring extra attributes.",
            py_object_size,
            t.size()
    );
    return s3::S3Settings{
            t[static_cast<uint32_t>(S3SettingsPickleOrder::AWS_AUTH)].cast<s3::AWSAuthMethod>(),
            t[static_cast<uint32_t>(S3SettingsPickleOrder::AWS_PROFILE)].cast<std::string>(),
            t[static_cast<uint32_t>(S3SettingsPickleOrder::USE_INTERNAL_CLIENT_WRAPPER_FOR_TESTING)].cast<bool>()
    };
}

py::tuple to_tuple(const s3::GCPXMLSettings& settings) {
    return py::make_tuple(
            s3::NativeSettingsType::GCPXML,
            settings.aws_auth(),
            settings.ca_cert_path(),
            settings.ca_cert_dir(),
            settings.ssl(),
            settings.https(),
            settings.prefix(),
            settings.endpoint(),
            settings.secret(),
            settings.access(),
            settings.bucket()
    );
}

py::tuple to_tuple(const s3::S3Settings& settings) {
    return py::make_tuple(
            s3::NativeSettingsType::S3,
            settings.aws_auth(),
            settings.aws_profile(),
            settings.use_internal_client_wrapper_for_testing()
    );
}

void register_common_storage_bindings(py::module& storage, BindingScope scope) {
    bool local_bindings = (scope == BindingScope::LOCAL);
    py::enum_<NativeVariantStorageContentType>(
            storage, "NativeVariantStorageContentType", py::module_local(local_bindings)
    )
            .value("EMPTY", NativeVariantStorageContentType::EMPTY)
            .value("S3", NativeVariantStorageContentType::S3)
            .value("GCPXML", NativeVariantStorageContentType::GCPXML);

    py::enum_<s3::AWSAuthMethod>(storage, "AWSAuthMethod", py::module_local(local_bindings))
            .value("DISABLED", s3::AWSAuthMethod::DISABLED)
            .value("DEFAULT_CREDENTIALS_PROVIDER_CHAIN", s3::AWSAuthMethod::DEFAULT_CREDENTIALS_PROVIDER_CHAIN)
            .value("STS_PROFILE_CREDENTIALS_PROVIDER", s3::AWSAuthMethod::STS_PROFILE_CREDENTIALS_PROVIDER);

    py::enum_<s3::NativeSettingsType>(storage, "NativeSettingsType", py::module_local(local_bindings))
            .value("S3", s3::NativeSettingsType::S3)
            .value("GCPXML", s3::NativeSettingsType::GCPXML);

    py::class_<s3::S3Settings>(storage, "S3Settings", py::module_local(local_bindings))
            .def(py::init<s3::AWSAuthMethod, const std::string&, bool>(),
                 py::arg("aws_auth"),
                 py::arg("aws_profile"),
                 py::arg("use_internal_client_wrapper_for_testing"))
            .def(py::pickle(
                    [](const s3::S3Settings& settings) { return to_tuple(settings); },
                    [](py::tuple t) { return s3_settings(t); }
            ))
            .def_property_readonly("aws_profile", [](const s3::S3Settings& settings) { return settings.aws_profile(); })
            .def_property_readonly("aws_auth", [](const s3::S3Settings& settings) { return settings.aws_auth(); })
            .def_property_readonly("use_internal_client_wrapper_for_testing", [](const s3::S3Settings& settings) {
                return settings.use_internal_client_wrapper_for_testing();
            });

    py::class_<s3::GCPXMLSettings>(storage, "GCPXMLSettings", py::module_local(local_bindings))
            .def(py::init<>())
            .def(py::pickle(
                    [](const s3::GCPXMLSettings& settings) { return to_tuple(settings); },
                    [](py::tuple t) { return gcp_settings(t); }
            ))
            .def_property("bucket", &s3::GCPXMLSettings::bucket, &s3::GCPXMLSettings::set_bucket)
            .def_property("endpoint", &s3::GCPXMLSettings::endpoint, &s3::GCPXMLSettings::set_endpoint)
            .def_property("access", &s3::GCPXMLSettings::access, &s3::GCPXMLSettings::set_access)
            .def_property("secret", &s3::GCPXMLSettings::secret, &s3::GCPXMLSettings::set_secret)
            .def_property("prefix", &s3::GCPXMLSettings::prefix, &s3::GCPXMLSettings::set_prefix)
            .def_property("aws_auth", &s3::GCPXMLSettings::aws_auth, &s3::GCPXMLSettings::set_aws_auth)
            .def_property("https", &s3::GCPXMLSettings::https, &s3::GCPXMLSettings::set_https)
            .def_property("ssl", &s3::GCPXMLSettings::ssl, &s3::GCPXMLSettings::set_ssl)
            .def_property("ca_cert_path", &s3::GCPXMLSettings::ca_cert_path, &s3::GCPXMLSettings::set_cert_path)
            .def_property("ca_cert_dir", &s3::GCPXMLSettings::ca_cert_dir, &s3::GCPXMLSettings::set_cert_dir);

    py::class_<NativeVariantStorage>(storage, "NativeVariantStorage", py::module_local(local_bindings))
            .def(py::init<>())
            .def(py::init<NativeVariantStorage::VariantStorageConfig>())
            .def(py::pickle(
                    [](const NativeVariantStorage& settings) {
                        return util::variant_match(
                                settings.variant(),
                                [](const s3::S3Settings& settings) { return to_tuple(settings); },
                                [](const s3::GCPXMLSettings& settings) { return to_tuple(settings); },
                                [](const auto&) -> py::tuple { util::raise_rte("Invalid native storage setting type"); }
                        );
                    },
                    [](py::tuple t) {
                        util::check(t.size() >= 1, "Expected at least one attribute in Native Settings pickle");
                        auto type =
                                t[static_cast<uint32_t>(S3SettingsPickleOrder::TYPE)].cast<s3::NativeSettingsType>();
                        switch (type) {
                        case s3::NativeSettingsType::S3:
                            return NativeVariantStorage(s3_settings(t));
                        case s3::NativeSettingsType::GCPXML:
                            return NativeVariantStorage(gcp_settings(t));
                        }
                        util::raise_rte("Inaccessible");
                    }
            ))
            .def("update", &NativeVariantStorage::update)
            .def("as_s3_settings", &NativeVariantStorage::as_s3_settings)
            .def("as_gcpxml_settings", &NativeVariantStorage::as_gcpxml_settings)
            .def("__repr__", &NativeVariantStorage::to_string)
            .def_property_readonly("setting_type", &NativeVariantStorage::setting_type);

    py::implicitly_convertible<NativeVariantStorage::VariantStorageConfig, NativeVariantStorage>();

    storage.def(
            "create_mem_config_resolver",
            [](const py::object& env_config_map_py) -> std::shared_ptr<ConfigResolver> {
                arcticdb::proto::storage::EnvironmentConfigsMap ecm;
                pb_from_python(env_config_map_py, ecm);
                auto resolver = std::make_shared<storage::details::InMemoryConfigResolver>();
                for (auto& [env, cfg] : ecm.env_by_id()) {
                    EnvironmentName env_name{env};
                    for (auto& [id, variant_storage] : cfg.storage_by_id()) {
                        resolver->add_storage(env_name, StorageName{id}, variant_storage);
                    }
                    for (auto& [id, lib_desc] : cfg.lib_by_path()) {
                        resolver->add_library(env_name, lib_desc);
                    }
                }
                return resolver;
            }
    );
    py::class_<ConfigResolver, std::shared_ptr<ConfigResolver>>(
            storage, "ConfigResolver", py::module_local(local_bindings)
    );

    py::class_<LibraryIndex, std::shared_ptr<LibraryIndex>>(storage, "LibraryIndex", py::module_local(local_bindings))
            .def(py::init<>([](const std::string& environment_name) {
                auto resolver = std::make_shared<details::InMemoryConfigResolver>();
                return std::make_unique<LibraryIndex>(EnvironmentName{environment_name}, resolver);
            }))
            .def_static(
                    "create_from_resolver",
                    [](const std::string& environment_name, std::shared_ptr<ConfigResolver> resolver) {
                        return std::make_shared<LibraryIndex>(EnvironmentName{environment_name}, resolver);
                    }
            )
            .def("list_libraries",
                 [](LibraryIndex& library_index, std::string_view prefix = "") {
                     std::vector<std::string> res;
                     for (const auto& lp : library_index.list_libraries(prefix)) {
                         res.emplace_back(lp.to_delim_path());
                     }
                     return res;
                 })
            .def("get_library",
                 [](LibraryIndex& library_index,
                    const std::string& library_path,
                    OpenMode open_mode = OpenMode::DELETE,
                    const NativeVariantStorage& native_storage_config = NativeVariantStorage()) {
                     LibraryPath path = LibraryPath::from_delim_path(library_path);
                     return library_index.get_library(path, open_mode, UserAuth{}, native_storage_config);
                 });

    py::class_<Library, std::shared_ptr<Library>>(storage, "Library", py::module_local(local_bindings))
            .def_property_readonly(
                    "library_path", [](const Library& library) { return library.library_path().to_delim_path(); }
            )
            .def_property_readonly("open_mode", [](const Library& library) { return library.open_mode(); })
            .def_property_readonly("config", [](const Library& library) {
                return util::variant_match(
                        library.config(),
                        [](const arcticdb::proto::storage::VersionStoreConfig& cfg) { return pb_to_python(cfg); },
                        [](const std::monostate&) -> py::object { return py::none{}; }
                );
            });

    py::enum_<OpenMode>(storage, "OpenMode", py::module_local(local_bindings))
            .value("READ", OpenMode::READ)
            .value("WRITE", OpenMode::WRITE)
            .value("DELETE", OpenMode::DELETE);
}
} // namespace arcticdb::storage::apy
