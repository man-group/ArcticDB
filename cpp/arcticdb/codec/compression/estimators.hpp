#pragma once

#include <cstddef>
#include <algorithm>
#include <bit>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include "frequency.hpp"

namespace arcticdb {
template<typename T>
struct CompressionSample {
    double ratio;
    size_t bits_needed;
};

template<typename T>
struct CompressionEstimate {
    double estimated_ratio;
    size_t estimated_bits_needed;
    std::vector<CompressionSample<T>> samples;
};

// Generic block analyzer that takes a function to calculate bits needed
template<typename T, typename BitsCalculator>
CompressionSample<T> analyze_block(
        FieldStatsImpl field_stats,
        const T* data,
        size_t block_size,
        BitsCalculator&& calc_bits) {
    size_t bits_needed = calc_bits(field_stats, data, block_size);
    double ratio = static_cast<double>(bits_needed * block_size) / (sizeof(T) * CHAR_BIT * block_size);
    return {ratio, bits_needed};
}

template<typename T, typename BitsCalculator>
CompressionEstimate<T> estimate_compression(
    FieldStatsImpl field_stats,
    const T* data,
    size_t row_count,
    BitsCalculator&& calc_bits,
    size_t num_samples = 10) {

    static constexpr size_t bits_per_block = 1024;
    static constexpr size_t values_per_block = bits_per_block / (sizeof(T) * CHAR_BIT);

    // Calculate how many complete blocks we have
    const size_t num_blocks = row_count / values_per_block;
    if (num_blocks == 0) {
        throw std::runtime_error("Data size too small for sampling");
    }

    // Calculate sampling stride to spread samples across the data
    const size_t samples_to_take = std::min(num_samples, num_blocks);
    const size_t block_stride = num_blocks / samples_to_take;

    // Collect samples
    std::vector<CompressionSample<T>> samples;
    samples.reserve(samples_to_take);

    double total_ratio = 0.0;
    size_t max_bits_needed = 0;

    for (size_t i = 0; i < samples_to_take; ++i) {
        size_t block_start = i * block_stride * values_per_block;
        auto sample = analyze_block(
            field_stats,
            data + block_start,
            values_per_block,
            calc_bits);

        samples.push_back(sample);
        total_ratio += sample.ratio;
        max_bits_needed = std::max(max_bits_needed, sample.bits_needed);
    }

    return {
        total_ratio / samples_to_take,  // average ratio
        max_bits_needed,                // conservative estimate of bits needed
        std::move(samples)             // individual sample data
    };
}

template<typename T>
struct RunLengthEstimator {
    size_t operator()(const T* data, size_t block_size) const {
        T max_run = 1;
        size_t current_run = 1;

        for (size_t i = 1; i < block_size; ++i) {
            if (data[i] == data[i-1]) {
                current_run++;
            } else {
                max_run = std::max(max_run, static_cast<T>(current_run));
                current_run = 1;
            }
        }
        max_run = std::max(max_run, static_cast<T>(current_run));

        return std::bit_width(static_cast<std::make_unsigned_t<T>>(max_run));
    }
};

template<typename T>
struct DeltaEstimator {
    static constexpr size_t overhead() {
        return Helper<T>::num_lanes * sizeof(T);
    }

    size_t operator()(
            FieldStatsImpl,
            const T* data,
            size_t block_size) const {
        T max_delta = 0;

        for (size_t i = 1; i < block_size; ++i) {
            T delta = data[i] - data[i-1];
            max_delta = std::max(max_delta, delta);
        }

        return std::bit_width(static_cast<std::make_unsigned_t<T>>(max_delta));
    }
};

template<typename T>
struct FForEstimator {
    size_t operator()(
            FieldStatsImpl field_stats,
            const T* data,
            size_t block_size) const {
        auto reference = field_stats.get_min<T>();

        T max_delta = 0;
        for (size_t i = 0; i < block_size; ++i) {
            max_delta = std::max(max_delta, data[i] - reference);
        }

        return std::bit_width(static_cast<std::make_unsigned_t<T>>(max_delta));
    }
};

template<typename T>
struct FrequencyBitsCalculator {
    const double required_percentage_;

    explicit FrequencyBitsCalculator(double required_percentage = 90.0) :
        required_percentage_(required_percentage) {}

    size_t operator()(const T* data, size_t block_size) const {
        // First pass: find candidate for dominant value using Boyer-Moore majority vote
        T candidate{};
        int32_t count = 0;

        for (size_t i = 0; i < block_size; ++i) {
            if (count == 0) {
                candidate = data[i];
                count = 1;
            } else {
                if (candidate == data[i])
                    count++;
                else
                    count--;
            }
        }

        size_t frequency = 0;
        for (size_t i = 0; i < block_size; ++i) {
            if (data[i] == candidate)
                frequency++;
        }

        double percent = static_cast<double>(frequency) / block_size * 100.0;

        if (percent > required_percentage_) {
            size_t num_exceptions = block_size - frequency;

            size_t header_bits = sizeof(typename FrequencyEncoding<T>::Data) * 8;
            size_t exception_bits = num_exceptions * sizeof(T) * 8;
            size_t bitmap_bits = block_size + 64;  // Rough estimate of bitmap overhead

            // Return average bits per value
            return (header_bits + exception_bits + bitmap_bits) / block_size;
        }

        // If frequency requirement not met, return original size
        return sizeof(T) * 8;
    }
};

} // namespace arcticdb
