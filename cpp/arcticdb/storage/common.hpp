/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <variant>
#include <arcticdb/util/string_wrapping_value.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/s3/s3_settings.hpp>
#include <sstream>

namespace arcticdb {
class Segment;
}

namespace arcticdb::storage {

using ReadVisitor = std::function<void(const entity::VariantKey&, Segment&&)>;

struct EnvironmentNameTag {};
using EnvironmentName = util::StringWrappingValue<EnvironmentNameTag>;

struct StorageNameTag {};
using StorageName = util::StringWrappingValue<StorageNameTag>;

struct InstanceUriTag {};
using InstanceUri = util::StringWrappingValue<InstanceUriTag>;

template<class T>
requires std::is_same_v<T, EnvironmentName> || std::is_same_v<T, StorageName>
bool operator==(const T& l, const T& r) {
    return l.value == r.value;
}

/*
 * Placeholder class for now
 */
class UserAuthTag {};
using UserAuth = util::StringWrappingValue<UserAuthTag>;

struct LibraryDescriptor {
    std::string name_;
    std::string description_;
    std::vector<StorageName> storage_ids_;

    using VariantStoreConfig = std::variant<
            std::monostate, //  make variant default constructible and unconfigured
            arcticdb::proto::storage::VersionStoreConfig>;

    VariantStoreConfig config_ = std::monostate{};
};

inline size_t get_stream_length(std::iostream& src) {
    src.seekg(0, std::ios::end);
    auto len = src.tellg();
    src.seekg(0, std::ios::beg);
    return static_cast<size_t>(len);
}

inline std::vector<char> stream_to_vector(std::iostream& src) {
    ARCTICDB_SAMPLE(StreamToVector, 0)
    auto len = get_stream_length(src);
    std::vector<char> v(len);
    src.read(v.data(), static_cast<long>(len));
    return v;
}

enum class NativeVariantStorageContentType : uint32_t { EMPTY = 0, S3 = 1, GCPXML = 2 };

class NativeVariantStorage {
  public:
    using VariantStorageConfig = std::variant<std::monostate, s3::S3Settings, s3::GCPXMLSettings>;
    explicit NativeVariantStorage(VariantStorageConfig config = std::monostate()) : config_(std::move(config)) {};
    const VariantStorageConfig& variant() const { return config_; }

    void update(const s3::S3Settings& config) { config_ = config; }

    std::string to_string() {
        return util::variant_match(
                config_,
                [](std::monostate) -> std::string { return "empty"; },
                [](s3::S3Settings s3) { return fmt::format("{}", s3); },
                [](s3::GCPXMLSettings gcpxml) { return fmt::format("{}", gcpxml); }
        );
    }

    s3::S3Settings as_s3_settings() {
        util::check(std::holds_alternative<s3::S3Settings>(config_), "Expected s3 settings but was {}", to_string());
        return std::get<s3::S3Settings>(config_);
    }

    s3::GCPXMLSettings as_gcpxml_settings() {
        util::check(
                std::holds_alternative<s3::GCPXMLSettings>(config_), "Expected gcpxml settings but was {}", to_string()
        );
        return std::get<s3::GCPXMLSettings>(config_);
    }

    NativeVariantStorageContentType setting_type() const {
        if (std::holds_alternative<std::monostate>(config_)) {
            return NativeVariantStorageContentType::EMPTY;
        } else if (std::holds_alternative<s3::S3Settings>(config_)) {
            return NativeVariantStorageContentType::S3;
        } else if (std::holds_alternative<s3::GCPXMLSettings>(config_)) {
            return NativeVariantStorageContentType::GCPXML;
        }
        util::raise_rte("Unknown variant storage type");
    }

  private:
    VariantStorageConfig config_;
};

} // namespace arcticdb::storage
