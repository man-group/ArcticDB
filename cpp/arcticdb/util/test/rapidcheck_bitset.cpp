/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "gtest/gtest.h"
#include <arcticdb/util/test/rapidcheck.hpp>

#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/sparse_utils.hpp>

#include <folly/container/Enumerate.h>

RC_GTEST_PROP(BitSet, PackedBitsToBuffer, (const std::vector<bool>& input)) {
    using namespace arcticdb;
    RC_PRE(!input.empty());
    auto n = input.size();
    util::BitSet bitset(n);
    util::BitSet::bulk_insert_iterator inserter(bitset);
    for (auto [idx, el] : folly::enumerate(input)) {
        if (el) {
            inserter = idx;
        }
    }
    inserter.flush();
    auto bytes = bitset_packed_size_bytes(n);

    std::vector<uint8_t> packed_bits(bytes);
    bitset_to_packed_bits(bitset, packed_bits.data());

    auto offset = *rc::gen::inRange(size_t(0), n);
    auto num_bits = *rc::gen::inRange(size_t(1), n - offset);

    std::vector<uint8_t> dest(num_bits);
    packed_bits_to_buffer(packed_bits.data(), num_bits, offset, dest.data());

    for (size_t idx = 0; idx < num_bits; ++idx) {
        RC_ASSERT(dest[idx] == (input.at(idx + offset) ? 1u : 0u));
    }
}

namespace {
constexpr size_t bitmagic_block_bits = bm::gap_max_bits;

// A bitset of num_bits bits built from runs of set and clear bits, of random density and run length, so that
// optimize() leaves a mix of bit, GAP, full and empty BitMagic blocks
arcticdb::util::BitSet random_run_bitset(size_t num_bits, bool optimize) {
    using namespace arcticdb;
    const auto density = *rc::gen::element(0.0, 0.01, 0.5, 0.95, 0.999, 1.0);
    // Runs of 1 give mostly bit blocks at mid densities, runs longer than a block give full and empty ones
    const auto max_run = *rc::gen::element(size_t(1), size_t(64), size_t(70'000));
    util::BitSet bitset(bv_size(num_bits));
    size_t pos = 0;
    while (pos < num_bits) {
        const auto run = std::min(num_bits - pos, *rc::gen::inRange(size_t(1), max_run + 1));
        const bool run_is_set = *rc::gen::inRange(0, 1000) < static_cast<int>(density * 1000);
        if (run_is_set)
            bitset.set_range(bv_size(pos), bv_size(pos + run - 1), true);
        pos += run;
    }
    if (optimize)
        bitset.optimize();
    return bitset;
}

// The reference implementation of bitset_to_packed_bits: one bit at a time
std::vector<uint8_t> packed_bits_one_at_a_time(const arcticdb::util::BitSet& bv, size_t num_bits) {
    std::vector<uint8_t> packed(arcticdb::bitset_packed_size_bytes(num_bits));
    for (auto en = bv.first(); en != bv.end() && *en < num_bits; ++en)
        packed[*en / 8] |= uint8_t(1) << (*en % 8);
    return packed;
}
} // namespace

RC_GTEST_PROP(BitSet, PackedBitsMatchBitByBit, ()) {
    using namespace arcticdb;
    // Sizes at and either side of word and block boundaries
    const auto num_bits = *rc::gen::element(
            size_t(0),
            size_t(1),
            size_t(63),
            bitmagic_block_bits - 1,
            bitmagic_block_bits,
            bitmagic_block_bits + 1,
            3 * bitmagic_block_bits + 3'395
    );
    const auto bitset = random_run_bitset(num_bits, *rc::gen::arbitrary<bool>());
    // Start from garbage, as bitset_to_packed_bits must zero the destination itself
    std::vector<uint8_t> packed(bitset_packed_size_bytes(num_bits), 0xAB);
    bitset_to_packed_bits(bitset, packed.data());
    RC_ASSERT(packed == packed_bits_one_at_a_time(bitset, num_bits));
}

RC_GTEST_PROP(BitSet, ExpansionMatchesBitByBit, ()) {
    using namespace arcticdb;
    const auto num_rows = *rc::gen::element(size_t(1), size_t(63), size_t(64), size_t(1000), bitmagic_block_bits + 1);
    const auto bitset = random_run_bitset(num_rows, *rc::gen::arbitrary<bool>());
    // Distinct dense values, promoted from int32 to double, with -1 marking rows the expansion must not write
    std::vector<int32_t> dense(bitset.count());
    for (size_t i = 0; i < dense.size(); ++i)
        dense[i] = static_cast<int32_t>(i * 7 - 3);
    constexpr double not_written = -1.0;
    std::vector<double> expected(num_rows, not_written);
    size_t dense_pos = 0;
    for (size_t row = 0; row < num_rows; ++row)
        if (bitset.test(bv_size(row)))
            expected[row] = dense[dense_pos++];
    std::vector<double> received(num_rows, not_written);
    util::expand_dense_buffer_and_promote_type<int32_t, double>(
            bitset, reinterpret_cast<const uint8_t*>(dense.data()), reinterpret_cast<uint8_t*>(received.data())
    );
    RC_ASSERT(received == expected);
}
