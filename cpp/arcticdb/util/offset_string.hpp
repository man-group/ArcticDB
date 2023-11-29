/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <limits>
#include <arcticdb/entity/types.hpp>
#ifdef ARCTICDB_USING_CONDA
    #include <robin_hood.h>
#else
    #include <arcticdb/util/third_party/robin_hood.hpp>
#endif

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
inline constexpr bool is_a_string(OffsetString::offset_t offset) {
    return offset < nan_placeholder();
}

// Given a set of string pool offsets, removes any that represent None or NaN
inline void remove_nones_and_nans(robin_hood::unordered_set<OffsetString::offset_t>& offsets) {
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
