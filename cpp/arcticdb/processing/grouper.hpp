/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <unordered_map>
#include <vector>

#include <fmt/core.h>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/processing/execution_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/filter_segment.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/util/string_utils.hpp>
#include <arcticdb/util/variant.hpp>
#include <folly/Poly.h>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/hash.hpp>

namespace arcticdb::grouping {

class HashingGroupers {
public:
    template<typename TDT>
    class Grouper {
    public:
        size_t group(typename TDT::DataTypeTag::raw_type key, std::shared_ptr<StringPool> sp) const {
            constexpr DataType dt = TDT::DataTypeTag::data_type;
            HashedValue hash_result;
            if constexpr(dt == DataType::ASCII_FIXED64 || dt == DataType::ASCII_DYNAMIC64 || dt == DataType::UTF_FIXED64 || dt == DataType::UTF_DYNAMIC64) {
                // TODO (AN-468): This will throw on Nones/NaNs
                hash_result = hash(sp->get_view(key));
            } else {
                hash_result = hash<typename TDT::DataTypeTag::raw_type>(&key, 1);
            }

            return hash_result;
        }
    };
};

class Bucketizer {
    public:
    virtual size_t bucket(size_t group) const = 0;

    virtual size_t num_buckets() const = 0;

    virtual ~Bucketizer() {};
};

class ModuloBucketizer : Bucketizer {
    size_t mod_;
public:
    ModuloBucketizer(size_t mod) :
        mod_(mod) {
    }

    ARCTICDB_MOVE_COPY_DEFAULT(ModuloBucketizer)

    size_t bucket(size_t group) const {
        return group % mod_;
    }

    virtual size_t num_buckets() const {
        return mod_;
    }

    virtual ~ModuloBucketizer() = default;
};

class IdentityBucketizer : Bucketizer {
public:
    size_t bucket(size_t group) const {
        return group;
    }

    virtual size_t num_buckets() const {
        return 0;
    }
};
}