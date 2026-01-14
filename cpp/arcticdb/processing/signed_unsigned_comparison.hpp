/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>

namespace arcticdb::comparison {

constexpr uint64_t msb = uint64_t{1} << 63;

inline bool msb_set(uint64_t val) { return static_cast<bool>(val & msb); }

inline int64_t to_signed(uint64_t val) { return static_cast<int64_t>(val); }

inline bool equals(uint64_t left, int64_t right) { return !msb_set(left) && to_signed(left) == right; }

inline bool equals(int64_t left, uint64_t right) { return !msb_set(right) && left == to_signed(right); }

inline bool not_equals(uint64_t left, int64_t right) { return msb_set(left) || to_signed(left) != right; }

inline bool not_equals(int64_t left, uint64_t right) { return msb_set(right) || left != to_signed(right); }

inline bool less_than(uint64_t left, int64_t right) { return !msb_set(left) && to_signed(left) < right; }

inline bool less_than(int64_t left, uint64_t right) { return msb_set(right) || left < to_signed(right); }

inline bool less_than_equals(uint64_t left, int64_t right) { return !msb_set(left) && to_signed(left) <= right; }

inline bool less_than_equals(int64_t left, uint64_t right) { return msb_set(right) || left <= to_signed(right); }

inline bool greater_than(uint64_t left, int64_t right) { return msb_set(left) || to_signed(left) > right; }

inline bool greater_than(int64_t left, uint64_t right) { return !msb_set(right) && left > to_signed(right); }

inline bool greater_than_equals(uint64_t left, int64_t right) { return msb_set(left) || to_signed(left) >= right; }

inline bool greater_than_equals(int64_t left, uint64_t right) { return !msb_set(right) && left >= to_signed(right); }
} // namespace arcticdb::comparison