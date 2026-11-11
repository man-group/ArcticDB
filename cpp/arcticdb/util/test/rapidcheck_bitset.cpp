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
// A bitset over num_bits made of runs of set and clear bits, so that optimize() leaves a mix of bit, GAP, full and
// empty BitMagic blocks
arcticdb::util::BitSet random_bitset(size_t num_bits, bool optimize) {
    using namespace arcticdb;
    const auto density = *rc::gen::element(0.0, 0.01, 0.5, 0.95, 0.999, 1.0);
    const auto max_run = *rc::gen::element(size_t(1), size_t(64), size_t(70'000));
    util::BitSet bitset(bv_size(num_bits));
    size_t pos = 0;
    while (pos < num_bits) {
        const auto run = std::min(num_bits - pos, *rc::gen::inRange(size_t(1), max_run + 1));
        if (*rc::gen::inRange(0, 1000) < static_cast<int>(density * 1000))
            bitset.set_range(bv_size(pos), bv_size(pos + run - 1), true);
        pos += run;
    }
    if (optimize)
        bitset.optimize();
    return bitset;
}

std::vector<uint8_t> packed_bits_one_at_a_time(const arcticdb::util::BitSet& bv, size_t num_bits) {
    std::vector<uint8_t> packed(arcticdb::bitset_packed_size_bytes(num_bits));
    for (auto en = bv.first(); en != bv.end() && *en < num_bits; ++en)
        packed[*en / 8] |= uint8_t(1) << (*en % 8);
    return packed;
}
} // namespace

RC_GTEST_PROP(BitSet, PackedBitsMatchBitByBit, ()) {
    using namespace arcticdb;
    const auto num_bits = *rc::gen::element(
            size_t(0), size_t(1), size_t(63), size_t(65'535), size_t(65'536), size_t(65'537), size_t(200'003)
    );
    const auto bitset = random_bitset(num_bits, *rc::gen::arbitrary<bool>());
    std::vector<uint8_t> packed(bitset_packed_size_bytes(num_bits), 0xAB);
    bitset_to_packed_bits(bitset, packed.data());
    RC_ASSERT(packed == packed_bits_one_at_a_time(bitset, num_bits));

    // A prefix, and more bits than the bitset holds
    const auto prefix = *rc::gen::inRange(size_t(0), num_bits + 100);
    std::vector<uint8_t> prefix_packed(bitset_packed_size_bytes(prefix), 0xAB);
    bitset_to_packed_bits(bitset, prefix_packed.data(), prefix);
    RC_ASSERT(prefix_packed == packed_bits_one_at_a_time(bitset, prefix));
}

RC_GTEST_PROP(BitSet, ForEachSetBitMatchesEnumerator, ()) {
    using namespace arcticdb;
    const auto num_bits = *rc::gen::element(size_t(1), size_t(64), size_t(65), size_t(65'537), size_t(130'000));
    const auto bitset = random_bitset(num_bits, *rc::gen::arbitrary<bool>());
    std::vector<uint64_t> words((num_bits + 63) / 64);
    bitset_to_packed_bits(bitset, reinterpret_cast<uint8_t*>(words.data()), num_bits);
    const auto from = *rc::gen::inRange(size_t(0), num_bits + 1);
    const auto to = *rc::gen::inRange(from, num_bits + 1);
    std::vector<size_t> visited;
    for_each_set_bit(words.data(), from, to, [&](size_t pos) { visited.push_back(pos); });
    std::vector<size_t> expected;
    for (auto en = bitset.first(); en != bitset.end(); ++en)
        if (*en >= from && *en < to)
            expected.push_back(*en);
    RC_ASSERT(visited == expected);
}

RC_GTEST_PROP(BitSet, WordWiseExpansionMatchesEnumerator, ()) {
    using namespace arcticdb;
    const auto num_rows = *rc::gen::element(size_t(1), size_t(63), size_t(64), size_t(1000), size_t(65'537));
    const auto bitset = random_bitset(num_rows, *rc::gen::arbitrary<bool>());
    const auto count = bitset.count();
    std::vector<int32_t> dense(count);
    for (size_t i = 0; i < count; ++i)
        dense[i] = static_cast<int32_t>(i * 7 - 3);
    std::vector<uint64_t> words((num_rows + 63) / 64);
    bitset_to_packed_bits(bitset, reinterpret_cast<uint8_t*>(words.data()), num_rows);

    std::vector<double> expected(num_rows, -1.0);
    util::expand_dense_buffer_and_promote_type<int32_t, double>(
            bitset, reinterpret_cast<const uint8_t*>(dense.data()), reinterpret_cast<uint8_t*>(expected.data())
    );
    std::vector<double> received(num_rows, -1.0);
    util::expand_dense_buffer_using_bitmap_words<int32_t, double>(
            words.data(),
            num_rows,
            reinterpret_cast<const uint8_t*>(dense.data()),
            count,
            reinterpret_cast<uint8_t*>(received.data())
    );
    for (auto en = bitset.first(); en != bitset.end(); ++en)
        RC_ASSERT(received[*en] == expected[*en]);
}
