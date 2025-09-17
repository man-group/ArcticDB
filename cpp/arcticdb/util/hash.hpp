/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <string_view>

#include <folly/hash/Hash.h>

#define XXH_STATIC_LINKING_ONLY
#include <xxhash.h>
#undef XXH_STATIC_LINKING_ONLY

namespace arcticdb {

template<class T>
inline size_t hash(T d) {
    return std::hash<T>{}(d);
}

inline size_t hash(std::string_view sv) { return std::hash<std::string_view>{}(sv); }

using HashedValue = XXH64_hash_t;

class HashAccum {
  public:
    explicit HashAccum(HashedValue seed = DEFAULT_SEED) { reset(seed); }

    void reset(HashedValue seed = DEFAULT_SEED) { XXH64_reset(&state_, seed); }

    template<typename T>
    void operator()(T* d, std::size_t count = 1) {
        XXH64_update(&state_, d, sizeof(T) * count);
    }

    [[nodiscard]] HashedValue digest() const { return XXH64_digest(&state_); }

  private:
    XXH64_state_t state_ = XXH64_state_t{};
    static constexpr std::size_t DEFAULT_SEED = 0x42;
};

} // namespace arcticdb
