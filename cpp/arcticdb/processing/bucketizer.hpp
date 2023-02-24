/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <vector>

namespace arcticdb {

struct VectorBucketizer {
    std::vector<size_t> vec_;
    size_t num_buckets_;

    VectorBucketizer(std::vector<size_t> &&vec, size_t num_buckets) :
        vec_(vec),
        num_buckets_(num_buckets) {
    }

    [[nodiscard]] size_t get_bucket(size_t row) const {
        return vec_[row];
    }

    [[nodiscard]] size_t num_buckets() const {
        return num_buckets_;
    }
};

} //namespace arcticdb
