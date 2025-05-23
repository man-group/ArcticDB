/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/codec/bitmap_utils.hpp>
#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/util//sparse_utils.hpp>
#include <arcticdb/codec/compression/compressor.hpp>

#include <bitmagic/bmalgo.h>
#include <ankerl/unordered_dense.h>

#include <cstdint>

namespace arcticdb {

template <typename T>
struct FrequencyHeader {
    T leader_;
    uint32_t exceptions_;
    uint32_t bitset_bytes_;
    uint32_t num_rows_;
};

template <typename T, size_t required_percentage = 90>
struct FrequencyCompressor : public FrequencyEncodingData {
    static_assert(required_percentage != 0, "required_percentage must be non-zero");

    FrequencyCompressor() = default;

    explicit FrequencyCompressor(const FrequencyEncodingData &parent)
        : FrequencyEncodingData(parent) {}

    size_t bitset_max_bytes() {
        typename bm::serializer<util::BitSet>::statistics_type stat{};
        bitset_->calc_stat(&stat);
        ARCTICDB_DEBUG(log::version(), "Bitset predicted bytes: {}", stat.max_serialize_mem);
        return stat.max_serialize_mem;
    }

    void scan(ColumnData data) {
        T value = T();
        for (auto block_num = 0UL; block_num < data.num_blocks(); ++block_num) {
            auto block = data.buffer().blocks()[block_num];
            size_t num_rows = block->bytes() / sizeof(T);
            auto data_in = reinterpret_cast<const T *>(block->data());
            for (size_t k = 0; k < num_rows; ++k) {
                if (count_ == 0) {
                    ++count_;
                    value = data_in[k];
                } else {
                    if (value != data_in[k])
                        --count_;
                    else
                        ++count_;
                }
            }
        }
        value_.set_value(value);
    }

    FrequencyEncodingData data() {
        return static_cast<FrequencyEncodingData>(*this);
    }

    void fill_bitset(ColumnData data, size_t num_rows) {
        bitset_ = std::make_shared<util::BitSet>(num_rows);
        util::BitSet::bulk_insert_iterator inserter(*bitset_);
        auto value = value_.template get_value<T>();
        auto pos = 0UL;
        for (auto block_num = 0UL; block_num < data.num_blocks(); ++block_num) {
            auto block = data.buffer().blocks()[block_num];
            size_t block_rows = block->bytes() / sizeof(T);
            auto data_in = reinterpret_cast<const T *>(block->data());
            for (size_t k = 0; k < block_rows; ++k) {
                if (*data_in != value)
                    inserter = pos;

                ++pos;
                ++data_in;
            }
        }
        inserter.flush();
    }

    std::optional<size_t> max_required_bytes(ColumnData data, size_t num_rows) {
        if (num_rows == 0)
            return 0;

        fill_bitset(data, num_rows);
        auto leader = num_rows - bitset_->count();
        double percent = double(leader) / num_rows * 100;
        if (percent > required_percentage) {
            leader_.first.template set_value<T>(value_.template get_value<T>());
            leader_.second = leader;
            auto num_exceptions = num_rows - leader;
            util::check(leader <= num_rows, "Count of leader {} cannot be more than num_rows {} in frequency encoding", count_, num_rows);

            expected_bytes_ = sizeof(FrequencyHeader<T>) + (num_exceptions * sizeof(T)) + bitset_max_bytes();
            ARCTICDB_DEBUG(log::version(), "Frequency encoding max required bytes: {}", *expected_bytes_);
            util::check(*expected_bytes_ != 0, "Frequency encoding expects non-zero output bytes");
            return expected_bytes_;
        } else {
            return std::nullopt;
        }
    }

    size_t compress(ColumnData column_data, size_t num_rows, uint8_t *data_out, size_t estimated_size) {
        if (num_rows == 0)
            return 0;

        auto *target = reinterpret_cast<uint8_t*>(data_out);
        auto *header = reinterpret_cast<FrequencyHeader<T> *>(target);

        const T leaderValue = leader_.first.get_value<T>();
        header->leader_ = leaderValue;
        header->exceptions_ = num_rows - leader_.second;
        header->num_rows_ = num_rows;

        target += sizeof(FrequencyHeader<T>);
        auto *exception_ptr = reinterpret_cast<T *>(target);

        if (column_data.num_blocks() == 1) {
            auto pos = reinterpret_cast<const T *>(column_data.buffer().data());
            BitVisitorFunctor visitor{[&exception_ptr, pos](uint32_t offset, uint64_t /*rank*/) {
                *exception_ptr++ = pos[offset];
            }};
            bm::for_each_bit(*bitset_, visitor);
        } else {
            BitVisitorFunctor visitor{[&exception_ptr, column_data](uint32_t offset, uint64_t /*rank*/) {
                *exception_ptr++ = column_data.buffer()[offset];
            }};
            bm::for_each_bit(*bitset_, visitor);
        }

        auto buffer = encode_bitmap(*bitset_);
        ARCTICDB_DEBUG(log::version(), "Frequency bitset bytes: {}", buffer.size());

        target += header->exceptions_ * sizeof(T);
        std::memcpy(target, buffer.data(), buffer.size());
        header->bitset_bytes_ = buffer.size();
        target += buffer.size();

        ARCTICDB_DEBUG(log::version(), "Frequency encoding actual bytes: {}", target - data_out);
        const size_t used_bytes = target - reinterpret_cast<uint8_t*>(data_out);
        util::check(used_bytes < estimated_size, "Size mismatch in frequency encoding, {} != {}", estimated_size, used_bytes);
        return used_bytes;
    }
};

template <typename T>
struct FrequencyDecompressor {
    static DecompressResult decompress(const uint8_t* data_in, T* data_out) {
        auto* data = reinterpret_cast<const FrequencyHeader<T>*>(data_in);
        const auto exceptions_bytes = data->exceptions_ * sizeof(T);

        const auto bitset_offset = sizeof(FrequencyHeader<T>) + (data->exceptions_ * sizeof(T));
        auto bitmap_ptr = &data_in[bitset_offset];
        auto bitmap = util::deserialize_bytes_to_bitmap(bitmap_ptr, data->bitset_bytes_);

        T* target = data_out;
        const auto num_rows = data->num_rows_;
        std::fill(target, target + num_rows, data->leader_);

        const auto* exceptions = reinterpret_cast<const T*>(data_in + sizeof(FrequencyHeader<T>));
        BitVisitorFunctor visitor{[target, exceptions](uint32_t offset, uint64_t rank) {
            target[offset] = exceptions[rank];
        }};
        bm::for_each_bit(bitmap, visitor);
        auto compressed_size = sizeof(FrequencyHeader<T>) + exceptions_bytes + data->bitset_bytes_;
        return {.compressed_=compressed_size, .uncompressed_=num_rows * sizeof(T)};
    }
};

}