/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <string_view>

// Adapt macro inlining depending on xxhash's linkage for the build.
#ifdef ARCTICDB_USING_STATIC_XXHASH
#define XXH_STATIC_LINKING_ONLY
#else
#define XXH_INLINE_ALL
#endif 
#include <xxhash.h>
#ifdef ARCTICDB_USING_STATIC_XXHASH
#undef XXH_STATIC_LINKING_ONLY
#else
#undef XXH_INLINE_ALL
#endif 

#include <folly/hash/Hash.h>

namespace arcticdb {

using HashedValue = XXH32_hash_t;
constexpr std::size_t DEFAULT_SEED = 0x42;

template<class T, std::size_t seed = DEFAULT_SEED>
HashedValue hash(T *d, std::size_t count = 1) {
    return XXH32(reinterpret_cast<const void *>(d), count * sizeof(T), seed);
}

inline HashedValue hash(std::string_view sv) {
    return hash(sv.data(), sv.size());
}

class HashAccum {
  public:
    explicit HashAccum(HashedValue seed = DEFAULT_SEED) {
        reset(seed);
    }

    void reset(HashedValue seed = DEFAULT_SEED) {
        XXH32_reset(&state_, seed);
    }

    template<typename T>
    void operator()(T *d, std::size_t count = 1) {
        XXH32_update(&state_, d, sizeof(T) * count);
    }

    [[nodiscard]] HashedValue digest() const {
        return XXH32_digest(&state_);
    }
  private:
    XXH32_state_t state_ = XXH32_state_t{};
};

} // namespace arcticdb
