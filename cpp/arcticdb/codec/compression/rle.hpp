#pragma once

/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <cstdint>
#include <cstddef>
#include <limits>
#include <algorithm>
#include <bit>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/util/preprocess.hpp>
/*
namespace arcticdb {
template<typename T>
class RLECompress {
    static constexpr size_t alignment = 32;
    const size_t num_lanes_;
    const size_t values_per_lane_;
    std::vector<T> last_values_;    // Last value seen in each lane
    std::vector<size_t> run_counts_; // Current run length for each lane

public:
    ALWAYS_INLINE
    RLECompress(size_t num_lanes, size_t values_per_lane)
        : num_lanes_(num_lanes)
        , values_per_lane_(values_per_lane)
        , last_values_(num_lanes, T{})
        , run_counts_(num_lanes, 0) {
#if defined(__clang__)
        __builtin_assume(reinterpret_cast<uintptr_t>(last_values_.data()) % alignment == 0);
        __builtin_assume(reinterpret_cast<uintptr_t>(run_counts_.data()) % alignment == 0);
#endif
    }

    HOT_FUNCTION
        ALWAYS_INLINE
    VECTOR_HINT
        T operator()(const T value, size_t idx) const {
        const size_t lane = idx % num_lanes_;
        const size_t pos_in_lane = idx / num_lanes_;

        // First value in lane or value different from last
        if (pos_in_lane == 0 || value != last_values_[lane]) {
            last_values_[lane] = value;
            run_counts_[lane] = 1;
            // Return value for first occurrence
            return value;
        }

        // Continuing a run
        run_counts_[lane]++;
        // Return run length for subsequent occurrences
        return static_cast<T>(run_counts_[lane]);
    }
};

template<typename T>
class RLEUncompress {
    static constexpr size_t alignment = 32;
    const size_t num_lanes_;
    const size_t values_per_lane_;
    mutable std::vector<T> current_values_;    // Current value for each lane
    mutable std::vector<size_t> remaining_runs_; // Remaining count in current run

public:
    ALWAYS_INLINE
    RLEUncompress(size_t num_lanes, size_t values_per_lane)
        : num_lanes_(num_lanes)
        , values_per_lane_(values_per_lane)
        , current_values_(num_lanes, T{})
        , remaining_runs_(num_lanes, 0) {
#if defined(__clang__)
        __builtin_assume(reinterpret_cast<uintptr_t>(current_values_.data()) % alignment == 0);
        __builtin_assume(reinterpret_cast<uintptr_t>(remaining_runs_.data()) % alignment == 0);
#endif
    }

    HOT_FUNCTION
        ALWAYS_INLINE
    VECTOR_HINT
        T operator()(const T value, size_t idx) const {
        const size_t lane = idx % num_lanes_;
        const size_t pos_in_lane = idx / num_lanes_;

        // First value in lane or no remaining runs
        if (pos_in_lane == 0 || remaining_runs_[lane] == 0) {
            current_values_[lane] = value;
            remaining_runs_[lane] = 1;  // Will be updated if this starts a run
            return value;
        }

        // If this is a run length
        if (remaining_runs_[lane] > 0) {
            remaining_runs_[lane] = value;  // Update remaining count
            return current_values_[lane];   // Return the repeated value
        }

        // New value
        current_values_[lane] = value;
        remaining_runs_[lane] = 1;
        return value;
    }
};

// Helper function to encode using RLE
template<typename T>
void encode_rle(const T* in, T* out, size_t count) {
    const size_t values_per_block = 1024 / (sizeof(T) * 8);
    const size_t num_blocks = (count + values_per_block - 1) / values_per_block;
    const size_t total_size = num_blocks * values_per_block;

    // Calculate max bits needed for run lengths
    T max_run = 0;
    for (size_t i = 0; i < count; ++i) {
        size_t run = 1;
        while (i + 1 < count && in[i] == in[i + 1]) {
            run++;
            i++;
        }
        max_run = std::max(max_run, static_cast<T>(run));
    }

    size_t bits_needed = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_run));

    // Create compression kernel
    RLECompress<T> kernel(values_per_block, total_size / values_per_block);

    // Use dispatch function to call appropriate BitPackFused instantiation
    dispatch_bitwidth(in, out, bits_needed, kernel);
}

// Helper function to decode RLE
template<typename T>
void decode_rle(const T* in, T* out, size_t count, size_t bits_needed) {
    const size_t values_per_block = 1024 / (sizeof(T) * 8);
    const size_t num_blocks = (count + values_per_block - 1) / values_per_block;
    const size_t total_size = num_blocks * values_per_block;

    // Create decompression kernel
    RLEUncompress<T> kernel(values_per_block, total_size / values_per_block);

    // Use dispatch function to call appropriate BitUnpackFused instantiation
    dispatch_bitwidth_unpack(in, out, bits_needed, kernel);
}

}

*/

