/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>

#include <folly/hash/Hash.h>

#define XXH_STATIC_LINKING_ONLY
#include <xxhash.h>
#undef XXH_STATIC_LINKING_ONLY

namespace arcticdb {

template<class T>
inline size_t hash(T d) {
    // Poor quality hash implementations of integral types, including at least some implementations of std::hash are
    // basically a static cast. e.g. std::hash<int64_t>{}(100) == 100. This is fast, but leads to poor distributions in
    // our bucketing, where we mod the hash with the number of buckets. In particular, if performing a grouping hash on
    // a timeseries where the time points are dates results in all of the rows being partitioned into bucket zero, which
    // then results in no parallelism in the aggregation clause.
    return folly::hasher<T>{}(d);
}

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
