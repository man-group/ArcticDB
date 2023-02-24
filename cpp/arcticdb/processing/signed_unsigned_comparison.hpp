/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <cstdint>

namespace arcticdb::comparison {

    constexpr uint64_t msb = uint64_t{1} << 63;

    inline bool msb_set(uint64_t val) {
        return static_cast<bool>(val & msb);
    }

    inline int64_t to_signed(uint64_t val) {
        return static_cast<int64_t>(val);
    }

    inline bool less_than(uint64_t left, int64_t right) {
        return !msb_set(left) && to_signed(left) < right;
    }

    inline bool less_than(int64_t left, uint64_t right) {
        return msb_set(right) || left < to_signed(right);
    }

    inline bool less_than_equals(uint64_t left, int64_t right) {
        return !msb_set(left) && to_signed(left) <= right;
    }

    inline bool less_than_equals(int64_t left, uint64_t right) {
        return msb_set(right) || left <= to_signed(right);
    }

    inline bool greater_than(uint64_t left, int64_t right) {
        return msb_set(left) || to_signed(left) > right;
    }

    inline bool greater_than(int64_t left, uint64_t right) {
        return !msb_set(right) && left > to_signed(right);
    }

    inline bool greater_than_equals(uint64_t left, int64_t right) {
        return msb_set(left) || to_signed(left) >= right;
    }

    inline bool greater_than_equals(int64_t left, uint64_t right) {
        return !msb_set(right) && left >= to_signed(right);
    }
}