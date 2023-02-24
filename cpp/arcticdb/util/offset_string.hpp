/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <limits>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/third_party/emilib_set.hpp>

namespace arcticdb {

class StringPool;

class OffsetString {
public:
    using offset_t = entity::position_t;

    explicit OffsetString(offset_t offset, StringPool *pool) :
        offset_(offset), pool_(pool) {}

    explicit operator std::string_view() const;

    [[nodiscard]] offset_t offset() const { return offset_; }

  private:
    entity::position_t offset_;
    StringPool *pool_;
};

inline constexpr OffsetString::offset_t not_a_string() { return std::numeric_limits<OffsetString::offset_t>::max(); }

inline constexpr OffsetString::offset_t nan_placeholder() { return not_a_string() - 1; }

// Returns true if the provided offset does not represent None or NaN
inline bool is_a_string(OffsetString::offset_t offset) {
    return offset < nan_placeholder();
}

// Given a set of string pool offsets, removes any that represent None or NaN
inline void remove_nones_and_nans(emilib::HashSet<OffsetString::offset_t>& offsets) {
    offsets.erase(not_a_string());
    offsets.erase(nan_placeholder());
}

template <typename LockPtrType>
inline PyObject* create_py_nan(LockPtrType& lock) {
    lock->lock();
    auto ptr = PyFloat_FromDouble(std::numeric_limits<double>::quiet_NaN());
    lock->unlock();
    util::check(ptr != nullptr, "Got null nan ptr");
    return ptr;
}
}