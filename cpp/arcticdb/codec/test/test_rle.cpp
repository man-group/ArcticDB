//
// Created by root on 1/31/25.
//
#include <chrono>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>

#include <gtest/gtest.h>
#include <arcticdb/codec/compression/rle.hpp>

namespace arcticdb {
/*
TEST_F(FastlanesCodecTest, RunLength) {
    using T = int32_t;
    const size_t block_size = calc_block_size<T>();
    const size_t num_blocks = 4;
    std::vector<T> data(block_size * num_blocks);

    // Create data with runs
    size_t i = 0;
    while (i < data.size()) {
        T value = i / 3;  // Create runs of length 3
        size_t run_length = std::min<size_t>(3, data.size() - i);
        std::fill_n(data.begin() + i, run_length, value);
        i += run_length;
    }

    std::vector<T> compressed(data.size());
    std::vector<T> decompressed(data.size());

    // Calculate bits needed
    T max_run = 3;  // We know our maximum run length
    size_t bits_needed = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_run));

    // Encode
    encode_rle(data.data(), compressed.data(), data.size());

    // Decode
    decode_rle(compressed.data(), decompressed.data(), data.size(), bits_needed);

    // Verify
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(data[i], decompressed[i])
                    << "Mismatch at index " << i;
    }
}


class Timer {
    using Clock = std::chrono::high_resolution_clock;
    using TimePoint = Clock::time_point;
    using Duration = std::chrono::nanoseconds;

    TimePoint start_;
    std::string name_;

public:
    explicit Timer(std::string name) : start_(Clock::now()), name_(std::move(name)) {}

    ~Timer() {
        auto end = Clock::now();
        auto duration = std::chrono::duration_cast<Duration>(end - start_);
        double ms = duration.count() / 1e6;
        std::cout << std::fixed << std::setprecision(3)
                  << name_ << ": " << ms << "ms" << std::endl;
    }
};

void run_rle_stress_test() {
    using T = uint32_t;
    static constexpr size_t values_per_block = 1024 / (sizeof(T) * 8);
    static constexpr size_t num_blocks = 1000;
    static constexpr size_t total_values = values_per_block * num_blocks;
    static constexpr size_t num_iterations = 1000000;

    // Allocate and initialize data
    std::cout << "Initializing " << total_values << " values..." << std::endl;
    std::vector<T> original(total_values);
    std::vector<T> compressed(total_values);
    std::vector<T> decompressed(total_values);

    // Generate test data with runs
    {
        Timer t("Data generation");
        std::mt19937 rng(42);
        std::geometric_distribution<int> run_length_dist(0.3); // Mean run length ~3.3

        size_t i = 0;
        while (i < total_values) {
            // Generate a value
            T value = static_cast<T>(i / values_per_block); // Different value per block

            // Generate run length (minimum 1, maximum 16)
            size_t run_length = 1 + (run_length_dist(rng) % 15);
            run_length = std::min(run_length, total_values - i);

            // Fill run
            std::fill_n(original.begin() + i, run_length, value);
            i += run_length;
        }
    }

    // Calculate bits needed
    size_t bits_needed;
    {
        Timer t("Bits calculation");
        T max_run = 0;
        size_t current_run = 1;

        for (size_t i = 1; i < total_values; ++i) {
            if (original[i] == original[i-1]) {
                current_run++;
            } else {
                max_run = std::max(max_run, static_cast<T>(current_run));
                current_run = 1;
            }
        }
        max_run = std::max(max_run, static_cast<T>(current_run));

        bits_needed = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_run));
        std::cout << "Max run length: " << max_run << ", bits needed: " << bits_needed << std::endl;
    }

    // Initial compression and verification
    {
        Timer t("Initial compression and verification");
        encode_rle(original.data(), compressed.data(), total_values);
        decode_rle(compressed.data(), decompressed.data(), total_values, bits_needed);

        for (size_t i = 0; i < total_values; ++i) {
            if (original[i] != decompressed[i]) {
                std::cerr << "Verification failed at index " << i
                          << ": " << original[i] << " != " << decompressed[i] << std::endl;
                return;
            }
        }
    }

    // Stress test compression
    {
        Timer t("Compression stress test");
        std::vector<double> compression_times;
        compression_times.reserve(num_iterations);

        for (size_t i = 0; i < num_iterations; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            encode_rle(original.data(), compressed.data(), total_values);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            compression_times.push_back(duration.count() / 1e6); // Convert to ms
        }

        // Calculate statistics
        double avg_compression = std::accumulate(compression_times.begin(),
                                                 compression_times.end(), 0.0) / num_iterations;

        std::sort(compression_times.begin(), compression_times.end());
        double median_compression = compression_times[num_iterations / 2];
        double p95_compression = compression_times[num_iterations * 95 / 100];
        double p99_compression = compression_times[num_iterations * 99 / 100];

        std::cout << "\nCompression Statistics (ms):\n"
                  << "  Average: " << std::fixed << std::setprecision(3) << avg_compression << "\n"
                  << "  Median:  " << median_compression << "\n"
                  << "  P95:     " << p95_compression << "\n"
                  << "  P99:     " << p99_compression << "\n"
                  << "  Throughput: " << std::fixed << std::setprecision(2)
                  << (total_values / avg_compression / 1000) << " M values/s\n";
    }

    // Stress test decompression
    {
        Timer t("Decompression stress test");
        std::vector<double> decompression_times;
        decompression_times.reserve(num_iterations);

        for (size_t i = 0; i < num_iterations; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            decode_rle(compressed.data(), decompressed.data(), total_values, bits_needed);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            decompression_times.push_back(duration.count() / 1e6); // Convert to ms
        }

        // Calculate statistics
        double avg_decompression = std::accumulate(decompression_times.begin(),
                                                   decompression_times.end(), 0.0) / num_iterations;

        std::sort(decompression_times.begin(), decompression_times.end());
        double median_decompression = decompression_times[num_iterations / 2];
        double p95_decompression = decompression_times[num_iterations * 95 / 100];
        double p99_decompression = decompression_times[num_iterations * 99 / 100];

        std::cout << "\nDecompression Statistics (ms):\n"
                  << "  Average: " << std::fixed << std::setprecision(3) << avg_decompression << "\n"
                  << "  Median:  " << median_decompression << "\n"
                  << "  P95:     " << p95_decompression << "\n"
                  << "  P99:     " << p99_decompression << "\n"
                  << "  Throughput: " << std::fixed << std::setprecision(2)
                  << (total_values / avg_decompression / 1000) << " M values/s\n";
    }

    // Calculate compression ratio
    {
        size_t original_bits = total_values * sizeof(T) * 8;
        size_t compressed_bits = total_values * bits_needed;
        double compression_ratio = static_cast<double>(compressed_bits) / original_bits;

        std::cout << "\nCompression Statistics:\n"
                  << "  Original size:    " << original_bits / 8 << " bytes\n"
                  << "  Compressed size:  " << compressed_bits / 8 << " bytes\n"
                  << "  Compression ratio: " << std::fixed << std::setprecision(3)
                  << compression_ratio * 100 << "%\n";
    }
}
 */
}  // namespace arcticdb