/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <vector>

namespace arcticdb {

struct VectorBucketizer {
    std::vector<size_t> vec_;
    size_t num_buckets_;

    VectorBucketizer(std::vector<size_t>&& vec, size_t num_buckets) : vec_(vec), num_buckets_(num_buckets) {}

    [[nodiscard]] size_t get_bucket(size_t row) const { return vec_[row]; }

    [[nodiscard]] size_t num_buckets() const { return num_buckets_; }
};

} // namespace arcticdb
