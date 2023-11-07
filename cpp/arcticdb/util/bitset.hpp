/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/magic_num.hpp>

#include <bitmagic/bm.h>

namespace arcticdb {

namespace util {

using BitSet = bm::bvector<>;
using BitMagic = bm::bvector<>;
using BitMagicStart = SmallMagicNum<'M', 's'>;
using BitMagicEnd = SmallMagicNum<'M', 'e'>;
using BitSetSizeType = bm::bvector<>::size_type;
using BitIndex = bm::bvector<>::rs_index_type;

} // namespace util

constexpr bm::bvector<>::size_type bv_size(uint64_t val) {
    return static_cast<bm::bvector<>::size_type>(val);
}
}
