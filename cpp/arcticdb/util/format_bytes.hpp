/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <fmt/format.h>

namespace arcticdb {
static char bytes_format [] = {' ', 'K', 'M', 'G', 'T', 'P', 'E', 'Z'};

inline std::string format_bytes(double num, char suffix='B') {
    for (auto unit : bytes_format) {
        if(std::abs(num) < 1000.0)
            return  fmt::format("{:.2f}{}{}", num, unit, suffix);

        num /= 1000.0;
    }
    return fmt::format("{:.2f}{}{}", num, "Y", suffix);
}
}