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
#include <arcticdb/processing/expression_context.hpp>
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
        using GrouperDescriptor = TDT;
        using DataTypeTag = typename GrouperDescriptor::DataTypeTag;
        using RawType = typename DataTypeTag::raw_type;

        // The multiple return statements in here are ugly, but avoids creating temporaries as far as possible
        std::optional<size_t> group(RawType key, const std::shared_ptr<StringPool>& sp) {
            constexpr DataType dt = DataTypeTag::data_type;
            if constexpr (dt == DataType::ASCII_FIXED64 || dt == DataType::ASCII_DYNAMIC64 ||
                          dt == DataType::UTF_FIXED64 || dt == DataType::UTF_DYNAMIC64) {
                if (auto it = cache_.find(key); it != cache_.end()) {
                    return it->second;
                } else {
                    if (is_a_string(key)) {
                        auto hashed_value = hash(sp->get_view(key));
                        cache_.insert(std::make_pair(std::move(key), std::optional<size_t>(hashed_value)));
                        return hashed_value;
                    } else {
                        cache_.insert(std::make_pair(std::move(key), std::optional<size_t>()));
                        return std::nullopt;
                    }
                }
            } else if constexpr(dt == DataType::FLOAT32 || dt == DataType::FLOAT64) {
                if (std::isnan(key)) {
                    return std::nullopt;
                } else {
                    return hash<RawType>(&key);
                }
            } else {
                return hash<RawType>(&key);
            }
        }
    private:
        // Only use a cache for grouping on string columns to avoid getting and hashing the same string view repeatedly
        // No point for numeric types, as we would have to hash it to look it up in this map anyway
        // This will be slower in cases where there aren't many repeats in string grouping columns
        // Maybe track cache hit ratio and stop using it if it is too low?
        // Tested with 100,000,000 row dataframe with 100,000 unique values in the grouping column. Timings:
        // 10.39 seconds without caching
        // 11.01 seconds with caching
        // Not worth worrying about right now
        ankerl::unordered_dense::map<RawType, std::optional<size_t>> cache_;
    };
};

class Bucketizer {
    public:
    virtual uint8_t bucket(size_t group) const = 0;

    virtual uint8_t num_buckets() const = 0;

    virtual ~Bucketizer() {};
};

class ModuloBucketizer : Bucketizer {
    uint8_t mod_;
public:
    ModuloBucketizer(uint8_t mod) :
        mod_(mod) {
    }

    ARCTICDB_MOVE_COPY_DEFAULT(ModuloBucketizer)

    uint8_t bucket(size_t group) const {
        return group % mod_;
    }

    virtual uint8_t num_buckets() const {
        return mod_;
    }

    virtual ~ModuloBucketizer() = default;
};

class IdentityBucketizer : Bucketizer {
public:
    uint8_t bucket(size_t group) const {
        return group;
    }

    virtual uint8_t num_buckets() const {
        return 0;
    }
};
}