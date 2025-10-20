/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "gtest/gtest.h"
#include <arcticdb/util/test/rapidcheck.hpp>

#include <arcticdb/util/bitset.hpp>

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