/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/column_store/string_pool.hpp>

namespace arcticdb {

OffsetString::OffsetString(OffsetString::offset_t offset, StringPool* pool) : offset_(offset), pool_(pool) {}

OffsetString::operator std::string_view() const { return pool_->get_view(offset()); }

OffsetString::offset_t OffsetString::offset() const { return offset_; }

// Given a set of string pool offsets, removes any that represent None or NaN
void remove_nones_and_nans(ankerl::unordered_dense::set<OffsetString::offset_t>& offsets) {
    offsets.erase(not_a_string());
    offsets.erase(nan_placeholder());
}

} // namespace arcticdb
