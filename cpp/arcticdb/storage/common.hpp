/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/util/string_wrapping_value.hpp>
#include <folly/Range.h>
#include <sstream>
#include <variant>

namespace arcticdb::storage {

struct EnvironmentNameTag {};
using EnvironmentName = util::StringWrappingValue<EnvironmentNameTag>;

struct StorageNameTag {};
using StorageName = util::StringWrappingValue<StorageNameTag>;

struct InstanceUriTag {};
using InstanceUri = util::StringWrappingValue<InstanceUriTag>;

template <class T>
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

  using VariantStoreConfig =
      std::variant<std::monostate, //  make variant default constructible and
                                   //  unconfigured
                   arcticdb::proto::storage::VersionStoreConfig>;

  VariantStoreConfig config_ = std::monostate{};
};

inline std::vector<char> stream_to_vector(std::vector<char>& src) { return src; }

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
  src.read(v.data(), len);
  return v;
}

inline void vector_to_stream(std::vector<char>& src, std::stringstream& output) {
  output.write(src.data(), src.size());
}

template <typename T> struct is_key_type : std::false_type {};

template <> struct is_key_type<entity::AtomKey> : std::true_type {};

template <> struct is_key_type<entity::RefKey> : std::true_type {};

template <> struct is_key_type<entity::VariantKey> : std::true_type {};

template <typename T> inline constexpr bool is_key_type_v = is_key_type<T>::value;

} // namespace arcticdb::storage
