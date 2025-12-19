#pragma once

#include <pybind11/pybind11.h>
#include <arcticdb/util/error_code.hpp>
#include <arcticdb/storage/s3/s3_settings.hpp>
#include <arcticdb/storage/common.hpp>

namespace arcticdb::storage::apy {

enum class S3SettingsPickleOrder : uint32_t {
    TYPE = 0,
    AWS_AUTH = 1,
    AWS_PROFILE = 2,
    USE_INTERNAL_CLIENT_WRAPPER_FOR_TESTING = 3
};

enum class GCPXMLSettingsPickleOrder : uint32_t {
    TYPE = 0,
    AWS_AUTH = 1,
    CA_CERT_PATH = 2,
    CA_CERT_DIR = 3,
    SSL = 4,
    HTTPS = 5,
    PREFIX = 6,
    ENDPOINT = 7,
    SECRET = 8,
    ACCESS = 9,
    BUCKET = 10,
};

s3::GCPXMLSettings gcp_settings(const pybind11::tuple& t);
s3::S3Settings s3_settings(const pybind11::tuple& t);
pybind11::tuple to_tuple(const s3::GCPXMLSettings& settings);
pybind11::tuple to_tuple(const s3::S3Settings& settings);
void register_common_bindings(pybind11::module& m, bool local_bindings);
NativeVariantStorage reconstruct_native_variant_storage_py_tuple(const pybind11::tuple& t);

} // namespace arcticdb::storage::apy