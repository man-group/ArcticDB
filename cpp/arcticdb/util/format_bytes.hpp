/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/format.h>

namespace arcticdb {
static char bytes_format[] = {' ', 'K', 'M', 'G', 'T', 'P', 'E', 'Z'};

inline std::string format_bytes(double num, char suffix = 'B') {
    for (auto unit : bytes_format) {
        if (std::abs(num) < 1000.0)
            return fmt::format("{:.2f}{}{}", num, unit, suffix);

        num /= 1000.0;
    }
    return fmt::format("{:.2f}{}{}", num, "Y", suffix);
}
} // namespace arcticdb