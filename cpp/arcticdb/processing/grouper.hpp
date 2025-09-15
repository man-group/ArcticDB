/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

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
        std::optional<uint8_t> group(RawType key, const std::shared_ptr<StringPool>& sp) const {
            constexpr DataType dt = DataTypeTag::data_type;
            if constexpr (dt == DataType::ASCII_FIXED64 || dt == DataType::ASCII_DYNAMIC64 ||
                          dt == DataType::UTF_FIXED64 || dt == DataType::UTF_DYNAMIC64) {
                if (ARCTICDB_LIKELY(is_a_string(key))) {
                    return static_cast<uint8_t>(hash(sp->get_view(key)) & 0xFF);
                } else {
                    return std::nullopt;
                }
            } else if constexpr (dt == DataType::FLOAT32 || dt == DataType::FLOAT64) {
                if (ARCTICDB_UNLIKELY(std::isnan(key))) {
                    return std::nullopt;
                } else {
                    return static_cast<uint8_t>(hash<RawType>(key) & 0xFF);
                }
            } else {
                return static_cast<uint8_t>(hash<RawType>(key) & 0xFF);
            }
        }
    };
};

class ModuloBucketizer {
    uint8_t mod_;

  public:
    ModuloBucketizer(uint8_t mod) : mod_(mod) {}

    ARCTICDB_MOVE_COPY_DEFAULT(ModuloBucketizer)

    uint8_t bucket(uint8_t group) const { return group % mod_; }

    uint8_t num_buckets() const { return mod_; }
};

} // namespace arcticdb::grouping