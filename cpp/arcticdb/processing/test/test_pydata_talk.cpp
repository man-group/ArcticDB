/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <random>

#include <bitmagic/bm.h>
#include <bitmagic/bmserial.h>

#include <gtest/gtest.h>


#include <arcticdb/util/bitset.hpp>

std::random_device rd;
std::mt19937 gen(rd());

using namespace arcticdb;

util::BitSet generate_bitset_random(const size_t num_rows, const int dense_percentage) {
    util::BitSet bitset;
    bitset.resize(num_rows);
    util::BitSet::bulk_insert_iterator inserter(bitset);
    std::uniform_int_distribution<> dis(1, 100);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (dis(gen) <= dense_percentage) {
            inserter = idx;
        }
    }
    inserter.flush();
    return bitset;
}

util::BitSet generate_bitset_runs(const size_t num_rows, const int dense_percentage) {
    util::BitSet bitset;
    bitset.resize(num_rows);
    auto num_values = (num_rows * dense_percentage) / 100;
    bitset.set_range(0, num_values - 1);
    return bitset;
}

TEST(PyDataTalk, BitSetCompressionRandom) {
    size_t num_repeats = 100;
    std::vector<int> dense_percentages{99, 90, 50, 10, 1};
    for (auto dense_percentage: dense_percentages) {
        double size{0};
        for (size_t idx = 0; idx < num_repeats; idx++) {
            auto bitset = generate_bitset_random(100'000, dense_percentage);
            bm::serializer<bm::bvector<>> serializer;
            bm::serializer<bm::bvector<>>::buffer buffer;
            serializer.serialize(bitset, buffer);
            size += static_cast<double>(buffer.size()) / num_repeats;
        }
        std::cout << "Dense percentage: " << dense_percentage << "%, Serialized size: " << size << std::endl;
    }
}

TEST(PyDataTalk, BitSetCompressionRuns) {
    size_t num_repeats = 1;
    std::vector<int> dense_percentages{99, 90, 50, 10, 1};
    for (auto dense_percentage: dense_percentages) {
        double size{0};
        for (size_t idx = 0; idx < num_repeats; idx++) {
            auto bitset = generate_bitset_runs(100'000, dense_percentage);
//            std::cout << bitset.count() << std::endl;
            bm::serializer<bm::bvector<>> serializer;
            bm::serializer<bm::bvector<>>::buffer buffer;
            serializer.serialize(bitset, buffer);
            size += static_cast<double>(buffer.size()) / num_repeats;
        }
        std::cout << "Dense percentage: " << dense_percentage << "%, Serialized size: " << size << std::endl;
    }
}