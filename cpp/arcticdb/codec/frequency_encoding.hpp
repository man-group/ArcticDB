/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/codec/codec.hpp>
#include <ankerl/unordered_dense.h>
#include <util/bitset.hpp>
#include <bitmagic/bmalgo.h>
#include <cstdint>

namespace arcticdb {
template <typename T, size_t required_percentage = 90>
struct FrequencyEncoding {

    std::pair<T, uint32_t> leader_;
    std::optional<util::BitSet> bitset_;
    std::optional<size_t> expected_bytes_;

    struct Data {
        T leader_;
        uint32_t exceptions_;
        uint32_t bitset_bytes_;
        uint32_t num_rows_;
    };

    size_t bitset_max_bytes() {
        bm::serializer<util::BitSet>::statistics_type stat{};
        bitset_->calc_stat(&stat);
        log::version().info("Bitset predicted bytes: {}", stat.max_serialize_mem);
        return stat.max_serialize_mem;
    }

    std::optional<std::pair<T, size_t>> find_leader(const T* data_in, size_t num_rows) {
        auto size = 0UL;
        T value;
        for(auto k = 0UL; k < num_rows; ++k) {
            if (size == 0) {
                ++size;
                value = data_in[k];
            } else {
                if(value != data_in[k])
                    --size;
                else
                    ++size;
            }
        }

        if(size == 0)
            return std::nullopt;

        auto count = 0UL;
        for(auto k = 0UL; k < num_rows; ++k) {
            if(data_in[k] == value)
                ++count;
        }
        return std::make_pair(value, count);
    }

    std::optional<size_t> max_required_bytes(const T *data_in, size_t num_rows) {
        if (num_rows == 0)
            return 0;

        auto maybe_leader = find_leader(data_in, num_rows);
        if(!maybe_leader)
            return std::nullopt;

        auto [leader_value, leader_count] = maybe_leader.value();

        auto percent = double(leader_count) / num_rows * 100;
        if(percent > required_percentage) {
            leader_.first = leader_value;
            leader_.second = leader_count;
            auto num_exceptions = num_rows - leader_count;
            util::check(leader_count <= num_rows, "Count of leader cannot be more than num_rows in frequency encoding");
            bitset_.emplace(util::BitSet(num_rows));
            expected_bytes_ = sizeof(Data) + (num_exceptions * sizeof(T)) + bitset_max_bytes();
            log::version().info("Frequency encoding max required bytes: {}", *expected_bytes_);
            util::check(*expected_bytes_ != 0, "Frequency encoding expects non-zero output bytes");
            return expected_bytes_;
        } else {
            return std::nullopt;
        }
    }

    size_t encode(const T *data_in, size_t num_rows, uint8_t *data_out) {
        if (num_rows == 0)
            return 0;

        const auto *pos = data_in;
        const auto *end = pos + num_rows;
        auto *target = data_out;
        auto* data = reinterpret_cast<Data*>(target);
        const auto leader = leader_.first;
        data->leader_ = leader;
        data->exceptions_ = num_rows - leader_.second;
        data->num_rows_ = num_rows;
        target += sizeof(Data);
        auto count = 0U;
        util::BitSet::bulk_insert_iterator inserter(*bitset_);
        auto* exception_ptr = reinterpret_cast<T*>(target);
        do {
            if(*pos != leader) {
                inserter = count;
                *exception_ptr++ = *pos;
            }
            ++pos;
            ++count;
        } while (pos != end);

        inserter.flush();
        auto buffer = encode_bitmap(*bitset_);
        log::version().info("Bitset actual bytes: {}", buffer.size());
        target += data->exceptions_ * sizeof(T);
        memcpy(target, buffer.data(), buffer.size());
        data->bitset_bytes_ = buffer.size();
        target += buffer.size();
        log::version().info("Frequency encoding actual bytes: {}", target - data_out);
        return target - data_out;
    }

    size_t decode(const uint8_t *data_in, size_t bytes, T *data_out) {
        auto* data = reinterpret_cast<const Data*>(data_in);
        const auto exceptions_bytes = data->exceptions_ * sizeof(T);
        util::check(sizeof(Data) + exceptions_bytes + data->bitset_bytes_ == bytes, "Size mismatch, expected {} + {} + {} = {}", sizeof(Data), exceptions_bytes, data->bitset_bytes_, bytes);

        const auto bitset_offset = sizeof(Data) + (data->exceptions_ * sizeof(T));
        auto bitmap_ptr = &data_in[bitset_offset];
        auto bitmap = util::deserialize_bytes_to_bitmap(bitmap_ptr, data->bitset_bytes_);
        auto *target = reinterpret_cast<T*>(data_out);
        const auto num_rows = data->num_rows_;
        auto* end = target + num_rows;

        std::fill(target, end, data->leader_);
        auto* exceptions = reinterpret_cast<const T*>(data_in + sizeof(Data));
        BitVisitorFunctor visitor{[target, exceptions] (util::BitSetSizeType offset, uint64_t rank) {
            target[offset] = exceptions[rank];
        }};
        bm::for_each_bit(bitmap, visitor);
        return num_rows;
    }
};
}