/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/magic_num.hpp>

#include <boost/dynamic_bitset.hpp>
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
