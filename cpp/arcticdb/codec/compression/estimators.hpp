#pragma once

#include <cstddef>
#include <algorithm>
#include <bit>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include "frequency.hpp"
#include <arcticdb/codec/compression/contiguous_range_adaptor.hpp>
#include <arcticdb/codec/compression/alp/rd.hpp>
#include <arcticdb/codec/compression/alp_header.hpp>

namespace arcticdb {
struct CompressionSample {
    size_t bits_needed_;
    size_t bit_width_;
    size_t exceptions_;
};

struct CompressionEstimate {
    size_t estimated_bytes_;
    size_t max_bit_width_;
    size_t max_exceptions_;
    std::vector<CompressionSample> samples_;
};

constexpr std::size_t round_up_bits(std::size_t bits) noexcept {
    return (bits + (CHAR_BIT - 1)) / CHAR_BIT;
}

template<typename T, typename Estimator>
CompressionSample analyze_block(
        FieldStatsImpl field_stats,
        const T* data,
        size_t block_size,
        Estimator&& estimator) {
    return estimator(field_stats, data, block_size);
}

template<typename T, typename Estimator>
CompressionEstimate estimate_compression(
    FieldStatsImpl field_stats,
    ColumnData data,
    size_t row_count,
    Estimator&& estimator,
    size_t num_samples = 10) {

    static constexpr size_t values_per_block = BLOCK_SIZE;

    const size_t num_blocks = row_count / values_per_block;
    if (num_blocks == 0) {
        throw std::runtime_error("Data size too small for sampling");
    }

    const size_t samples_to_take = std::min(num_samples, num_blocks);
    const size_t block_stride = num_blocks / samples_to_take;

    std::vector<CompressionSample> samples;
    samples.reserve(samples_to_take);

    size_t max_bit_width = 0;
    size_t max_exceptions = 0;
    size_t total_sample_compressed_size = 0;

    ContiguousRangeRandomAccessAdaptor<T, values_per_block> adaptor{data};
    for (size_t i = 0; i < samples_to_take; ++i) {
        size_t block_start = i * block_stride * values_per_block;
        auto ptr = adaptor.at(block_start);
        auto sample = analyze_block(
            field_stats,
            ptr,
            values_per_block,
            estimator);

        samples.push_back(sample);
        max_bit_width = std::max(max_bit_width, sample.bits_needed_);
        max_exceptions = std::max(max_exceptions, sample.exceptions_);
        total_sample_compressed_size += round_up_bits(sample.bits_needed_);
    }

    const auto estimated_bytes = (total_sample_compressed_size / samples_to_take) * num_blocks;

    return {
        .estimated_bytes_ = estimated_bytes,
        .max_bit_width_ = max_bit_width,
        .max_exceptions_ = max_exceptions,
        .samples_ = std::move(samples)
    };
}

template<typename T>
struct RunLengthEstimator {
    CompressionSample operator()(const T* data, size_t block_size) const {
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
        auto bit_width = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_run));
        return {.bits_needed_ = bit_width * block_size, .bit_width_ = bit_width, .exceptions_ = 0};
    }
};

template<typename T>
struct DeltaEstimator {
    static constexpr size_t overhead() {
        return Helper<T>::num_lanes * sizeof(T);
    }

    CompressionSample operator()(
            FieldStatsImpl,
            const T* data,
            size_t block_size) const {
        T max_delta = 0;

        for (size_t i = 1; i < block_size; ++i) {
            T delta = data[i] - data[i-1];
            max_delta = std::max(max_delta, delta);
        }

        const auto bit_width = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_delta));
        return {.bits_needed_ = bit_width * block_size, .bit_width_ = bit_width, .exceptions_ = 0};
    }
};


template <typename T>
struct ALPEstimator {
    static constexpr size_t overhead() {
        return 0;
    }

    CompressionSample operator()(
        FieldStatsImpl,
        const T* data,
        size_t block_size) const {
        alp::state<T> state;
        std::array<T, alp::config::VECTOR_SIZE> sample_buf;
        alp::encoder<T>::init(data, 0, block_size, sample_buf.data(), state);
        if(block_size < alp::config::VECTOR_SIZE)
            return {.bits_needed_ = sizeof(T) * block_size * CHAR_BIT, .bit_width_ = 0, .exceptions_ = 0};

        switch(state.scheme) {
        case alp::Scheme::ALP_RD: {
            std::array<uint16_t, alp::config::VECTOR_SIZE> exceptions;
            std::array<uint16_t, alp::config::VECTOR_SIZE> positions;
            std::array<uint16_t, alp::config::VECTOR_SIZE> excp_count;
            std::array<typename StorageType<T>::unsigned_type, alp::config::VECTOR_SIZE> right;
            std::array<uint16_t, alp::config::VECTOR_SIZE> left;

            alp::rd_encoder<T>::init(data, 0UL, alp::config::VECTOR_SIZE, sample_buf.data(), state);
            alp::rd_encoder<T>::encode(
                data,
                exceptions.data(),
                positions.data(),
                excp_count.data(),
                right.data(),
                left.data(),
                state);

            RealDoubleHeader<T> header{state};

            return {
                .bits_needed_ = header.total_size() * block_size,
                .bit_width_ = std::max(state.right_bit_width, state.left_bit_width),
                .exceptions_ = state.exceptions_count
            };
        }
        case alp::Scheme::ALP: {
            std::array<T, BLOCK_SIZE> exceptions;
            std::array<uint16_t, BLOCK_SIZE> exception_positions;
            uint16_t exception_count = 0;
            std::array<int64_t, BLOCK_SIZE> encoded;

            alp::encoder<T>::init(data, 0, 1024, sample_buf.data(), state);
            alp::encoder<T>::encode(data, exceptions.data(), exception_positions.data(), &exception_count, encoded.data(), state);

            ALPDecimalHeader<T> header{state};

            return {
                .bits_needed_ = header.total_size() * block_size,
                .bit_width_ = state.bit_width,
                .exceptions_ = state.exceptions_count
            };
        }
        case alp::Scheme::INVALID: {
            return {sizeof(T) * block_size, 0, 0};
        }
        }
    }
};

template<typename T>
struct FForEstimator {
    constexpr static size_t overhead() {
        return sizeof(FForHeader<T>);
    }

    CompressionSample operator()(
            FieldStatsImpl field_stats,
            const T* data,
            size_t block_size) const {
        auto reference = field_stats.get_min<T>();

        T max_delta = 0;
        for (size_t i = 0; i < block_size; ++i) {
            max_delta = std::max<T>(max_delta, data[i] - reference);
        }

        const auto bit_width = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_delta));
        return {.bits_needed_ = bit_width * block_size, .bit_width_ = bit_width, .exceptions_ = 0};
    }
};

template<typename T>
struct FrequencyEstimator {
    const double required_percentage_;

    explicit FrequencyEstimator(double required_percentage = 90.0) :
        required_percentage_(required_percentage) {}

    CompressionSample operator()(const T* data, size_t block_size) const {
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

            size_t header_bits = sizeof(typename FrequencyCompressor<T>::Data) * 8;
            size_t exception_bits = num_exceptions * sizeof(T) * 8;
            size_t bitmap_bits = block_size + 64;  // Rough estimate of bitmap overhead

            auto required_size = (header_bits + exception_bits + bitmap_bits) / block_size;
            return {.bits_needed_ = required_size, .bit_width_ = 0, .exceptions_ = 0};
        }

        // If no dominator found, return full size
        return {.bits_needed_ = sizeof(T) * block_size, .bit_width_ = 0, .exceptions_ = 0};
    }
};

} // namespace arcticdb
