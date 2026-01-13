/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

// Unset the definition of `copysign` that is defined in `Python.h` for Python < 3.8 on Windows.
// See: https://github.com/python/cpython/pull/23326
#if defined(_MSC_VER) && PY_VERSION_HEX < 0x03080000
#undef copysign
#endif

#include <ankerl/unordered_dense.h>
#include <arcticdb/entity/types.hpp> // for entity::position_t
#include <arcticdb/util/constants.hpp>

namespace arcticdb {

class StringPool;

class OffsetString {
  public:
    using offset_t = entity::position_t;

    explicit OffsetString(offset_t offset, StringPool* pool);

    explicit operator std::string_view() const;

    [[nodiscard]] offset_t offset() const;

  private:
    entity::position_t offset_;
    StringPool* pool_;
};

constexpr OffsetString::offset_t not_a_string() { return string_none; }

constexpr OffsetString::offset_t nan_placeholder() { return string_nan; }

// Returns true if the provided offset does not represent None or NaN
constexpr bool is_a_string(OffsetString::offset_t offset) { return offset < nan_placeholder(); }

// Given a set of string pool offsets, removes any that represent None or NaN
void remove_nones_and_nans(ankerl::unordered_dense::set<OffsetString::offset_t>& offsets);

} // namespace arcticdb
