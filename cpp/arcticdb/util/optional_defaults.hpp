/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <optional>

namespace arcticdb {

inline bool opt_true(const std::optional<bool> &opt) {
    return !opt || opt.value();
}

inline bool opt_false(const std::optional<bool> &opt) {
    return opt && opt.value();
}
} //namespace