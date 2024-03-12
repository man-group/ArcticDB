/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <string_view>

#include <folly/hash/Hash.h>

#define XXH_STATIC_LINKING_ONLY
#include <xxhash.h>
#undef XXH_STATIC_LINKING_ONLY

//#include <highwayhash/highwayhash.h>

namespace arcticdb {

using HashedValue = XXH64_hash_t;
constexpr std::size_t DEFAULT_SEED = 0x42;

// xxhash
//template<class T, std::size_t seed = DEFAULT_SEED>
//HashedValue hash(T *d, std::size_t count) {
//    return XXH64(reinterpret_cast<const void *>(d), count * sizeof(T), seed);
//}
//
//// size argument to XXH64 being compile-time constant improves performance
//template<class T, std::size_t seed = DEFAULT_SEED>
//HashedValue hash(T *d) {
//    return XXH64(reinterpret_cast<const void *>(d), sizeof(T), seed);
//}
//
//inline HashedValue hash(std::string_view sv) {
//    return hash(sv.data(), sv.size());
//}

// Highwayhash
//using namespace highwayhash;
//HH_ALIGNAS(32) const HHKey key = {1, 2, 3, 4};

//template<class T>
//inline HHResult64 hash(T *d, std::size_t count) {
//    HHResult64 result;
//    HHStateT<HH_TARGET> state(key);
//    HighwayHashT(&state, reinterpret_cast<const char*>(d), count * sizeof(T), &result);
//    return result;
//}
//
//template<class T>
//inline HHResult64 hash(T *d) {
//    HHResult64 result;
//    HHStateT<HH_TARGET> state(key);
//    HighwayHashT(&state, reinterpret_cast<const char*>(d), sizeof(T), &result);
//    return result;
//}
//
//inline HHResult64 hash(std::string_view sv) {
//    HHResult64 result;
//    HHStateT<HH_TARGET> state(key);
//    HighwayHashT(&state, reinterpret_cast<const char*>(sv.data()), sv.size(), &result);
//    return result;
//}

// std::hash

template<class T>
inline size_t hash(T *d) {
    return std::hash<T>{}(*d);
}

inline size_t hash(std::string_view sv) {
    return std::hash<std::string_view>{}(sv);
}

class HashAccum {
  public:
    explicit HashAccum(HashedValue seed = DEFAULT_SEED) {
        reset(seed);
    }

    void reset(HashedValue seed = DEFAULT_SEED) {
        XXH64_reset(&state_, seed);
    }

    template<typename T>
    void operator()(T *d, std::size_t count = 1) {
        XXH64_update(&state_, d, sizeof(T) * count);
    }

    [[nodiscard]] HashedValue digest() const {
        return XXH64_digest(&state_);
    }
  private:
    XXH64_state_t state_ = XXH64_state_t{};
};

} // namespace arcticdb
