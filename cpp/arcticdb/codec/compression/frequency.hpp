/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include "arcticdb/log/log.hpp"
#include "arcticdb/util/preconditions.hpp"
#include "arcticdb/codec/codec.hpp"
#include "arcticdb/log/log.hpp"
#include <ankerl/unordered_dense.h>
#include "arcticdb/util/bitset.hpp"
#include <bitmagic/bmalgo.h>
#include <cstdint>

namespace arcticdb {

template <typename T, typename F>
void batch_apply(T* data, size_t num_rows, F functor) {
    if(num_rows == 0)
        return;

    constexpr size_t CHUNK_SIZE = 64;

    auto loops = num_rows / CHUNK_SIZE;
    for(auto i = 0UL; i < loops; ++i) {
#ifdef __clang__
#pragma clang loop vectorize(enable) interleave(enable)
#endif
        for(size_t j = 0; j < CHUNK_SIZE; ++j) {
            functor(data[i * CHUNK_SIZE + j]);
        }
    }

    auto remainder = num_rows - (loops * CHUNK_SIZE);
    for(auto i = num_rows - remainder; i < num_rows; ++i) {
        functor(data[i]);
    }
}

template <typename T>
void fill(T* data, size_t num_rows, const T value) {
    auto setter = [value](T& x) { x = value; };
    batch_apply(data, num_rows, setter);
}

template <typename T, size_t required_percentage = 90>
struct FrequencyEncoding {
    static_assert(required_percentage != 0);
    T value_;
    uint32_t count_ = 0;
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
        ARCTICDB_DEBUG(log::version(), "Bitset predicted bytes: {}", stat.max_serialize_mem);
        return stat.max_serialize_mem;
    }

    void scan(const T* data_in, size_t num_rows) {
        for(auto k = 0UL; k < num_rows; ++k) {
            if (count_ == 0) {
                ++count_;
                value_ = data_in[k];
            } else {
                if(value_ != data_in[k])
                    --count_;
                else
                    ++count_;
            }
        }
    }

    void fill_bitset(const T* begin, size_t num_rows) {
        const T* pos = begin;
        const auto* end = begin + num_rows;
        bitset_.emplace(util::BitSet(num_rows));
        util::BitSet::bulk_insert_iterator inserter(*bitset_);
        do {
            if (*pos != value_) {
                inserter = std::distance(begin, pos);
            }
            ++pos;
        } while (pos != end);
        inserter.flush();
    }
    
    
    std::optional<size_t> max_required_bytes(const T *data_in, size_t num_rows) {
        if (num_rows == 0)
            return 0;
        
        fill_bitset(data_in, num_rows);
        auto leader = num_rows - bitset_->count();
        auto percent = double(leader) / num_rows * 100;
        if(percent > required_percentage) {
            leader_.first = value_;
            leader_.second = leader;
            auto num_exceptions = num_rows - leader;
            util::check(leader <= num_rows, "Count of leader {} cannot be more than num_rows {} in frequency encoding", count_, num_rows);
            
            expected_bytes_ = sizeof(Data) + (num_exceptions * sizeof(T)) + bitset_max_bytes();
            ARCTICDB_DEBUG(log::version(), "Frequency encoding max required bytes: {}", *expected_bytes_);
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
        auto *target = data_out;
        auto* data = reinterpret_cast<Data*>(target);
        const auto leader = leader_.first;
        data->leader_ = leader;
        data->exceptions_ = num_rows - leader_.second;
        data->num_rows_ = num_rows;
        target += sizeof(Data);
        auto *exception_ptr = reinterpret_cast<T *>(target);
        BitVisitorFunctor visitor{[&exception_ptr, pos] (util::BitSetSizeType offset, uint64_t) {
            *exception_ptr++ = pos[offset];
        }};
        bm::for_each_bit(*bitset_, visitor);

        auto buffer = encode_bitmap(*bitset_);
        ARCTICDB_DEBUG(log::version(), "Bitset actual bytes: {}", buffer.size());
        target += data->exceptions_ * sizeof(T);
        memcpy(target, buffer.data(), buffer.size());
        data->bitset_bytes_ = buffer.size();
        target += buffer.size();
        ARCTICDB_DEBUG(log::version(), "Frequency encoding actual bytes: {}", target - data_out);
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
        std::fill(target, target + num_rows, data->leader_);
        auto* exceptions = reinterpret_cast<const T*>(data_in + sizeof(Data));
        BitVisitorFunctor visitor{[target, exceptions] (util::BitSetSizeType offset, uint64_t rank) {
            target[offset] = exceptions[rank];
        }};
        bm::for_each_bit(bitmap, visitor);
        return num_rows;
    }
};

template <typename T, size_t required_percentage = 90>
struct OptimizedFrequencyEncoding {
    T value_{};
    int32_t count_ = 0;
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
        ARCTICDB_DEBUG(log::version(), "Bitset predicted bytes: {}", stat.max_serialize_mem);
        return stat.max_serialize_mem;
    }

    // Unrolled scanning for dominant value
    void scan(const T* data_in, size_t num_rows) {
        size_t i = 0;

        // Process 4 values at a time
        for (; i + 4 <= num_rows; i += 4) {
            for (size_t j = 0; j < 4; ++j) {
                if (count_ == 0) {
                    ++count_;
                    value_ = data_in[i + j];
                } else {
                    if (value_ != data_in[i + j])
                        --count_;
                    else
                        ++count_;
                }
            }
        }

        // Handle remaining values
        for (; i < num_rows; ++i) {
            if (count_ == 0) {
                ++count_;
                value_ = data_in[i];
            } else {
                if (value_ != data_in[i])
                    --count_;
                else
                    ++count_;
            }
        }
    }

    void fill_bitset(const T* begin, size_t num_rows) {
        bitset_.emplace(util::BitSet(num_rows));
        auto& bs = *bitset_;

        // Process 4 values at a time
        size_t i = 0;
        for (; i + 4 <= num_rows; i += 4) {
            bool is_exception[4];

            // Gather exceptions
            for (size_t j = 0; j < 4; ++j) {
                is_exception[j] = (begin[i + j] != value_);
            }

            // Set bits for exceptions
            for (size_t j = 0; j < 4; ++j) {
                if (is_exception[j]) {
                    bs.set(i + j);
                }
            }
        }

        // Handle remaining values
        for (; i < num_rows; ++i) {
            if (begin[i] != value_) {
                bs.set(i);
            }
        }
    }

    std::optional<size_t> max_required_bytes(const T* data_in, size_t num_rows) {
        if (num_rows == 0)
            return 0;

        fill_bitset(data_in, num_rows);
        auto leader = num_rows - bitset_->count();
        auto percent = double(leader) / num_rows * 100;

        if (percent > required_percentage) {
            leader_.first = value_;
            leader_.second = leader;
            auto num_exceptions = num_rows - leader;
            util::check(leader <= num_rows, "Count of leader {} cannot be more than num_rows {} in frequency encoding", count_, num_rows);

            expected_bytes_ = sizeof(Data) + (num_exceptions * sizeof(T)) + bitset_max_bytes();
            util::check(*expected_bytes_ != 0, "Frequency encoding expects non-zero output bytes");
            return expected_bytes_;
        }

        return std::nullopt;
    }

    size_t encode(const T* data_in, size_t num_rows, uint8_t* data_out) {
        if (num_rows == 0)
            return 0;

        auto* data = reinterpret_cast<Data*>(data_out);
        data->leader_ = leader_.first;
        data->exceptions_ = num_rows - leader_.second;
        data->num_rows_ = num_rows;

        auto* target = data_out + sizeof(Data);
        auto* exception_ptr = reinterpret_cast<T*>(target);

        // Process exceptions 4 values at a time
        size_t exception_idx = 0;
        size_t i = 0;
        for (; i + 4 <= num_rows; i += 4) {
            bool has_exception[4];
            T values[4];

            // Gather exceptions
            for (size_t j = 0; j < 4; ++j) {
                has_exception[j] = bitset_->test(i + j);
                if (has_exception[j]) {
                    values[j] = data_in[i + j];
                }
            }

            // Store exceptions
            for (size_t j = 0; j < 4; ++j) {
                if (has_exception[j]) {
                    exception_ptr[exception_idx++] = values[j];
                }
            }
        }

        // Handle remaining values
        for (; i < num_rows; ++i) {
            if (bitset_->test(i)) {
                exception_ptr[exception_idx++] = data_in[i];
            }
        }

        target += data->exceptions_ * sizeof(T);
        auto buffer = encode_bitmap(*bitset_);
        memcpy(target, buffer.data(), buffer.size());
        data->bitset_bytes_ = buffer.size();
        target += buffer.size();

        return target - data_out;
    }

    size_t decode(const uint8_t* data_in, size_t bytes, T* data_out) {
        auto* data = reinterpret_cast<const Data*>(data_in);
        const auto exceptions_bytes = data->exceptions_ * sizeof(T);
        util::check(sizeof(Data) + exceptions_bytes + data->bitset_bytes_ == bytes,
                    "Size mismatch, expected {} + {} + {} = {}",
                    sizeof(Data), exceptions_bytes, data->bitset_bytes_, bytes);

        // Fill with leader value first
        std::fill_n(data_out, data->num_rows_, data->leader_);

        const auto* exceptions = reinterpret_cast<const T*>(data_in + sizeof(Data));
        const auto bitset_offset = sizeof(Data) + exceptions_bytes;
        auto bitmap_ptr = &data_in[bitset_offset];
        auto bitmap = util::deserialize_bytes_to_bitmap(bitmap_ptr, data->bitset_bytes_);

        // Process exceptions 4 values at a time
        size_t exception_idx = 0;
        size_t i = 0;
        for (; i + 4 <= data->num_rows_; i += 4) {
            bool has_exception[4];

            // Gather exception flags
            for (size_t j = 0; j < 4; ++j) {
                has_exception[j] = bitmap.test(i + j);
            }

            // Apply exceptions
            for (size_t j = 0; j < 4; ++j) {
                if (has_exception[j]) {
                    data_out[i + j] = exceptions[exception_idx++];
                }
            }
        }

        // Handle remaining values
        for (; i < data->num_rows_; ++i) {
            if (bitmap.test(i)) {
                data_out[i] = exceptions[exception_idx++];
            }
        }

        return data->num_rows_;
    }
};

}