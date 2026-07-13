/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <variant>

namespace arcticdb {

enum class ArrowOutputStringFormat : uint8_t { CATEGORICAL, LARGE_STRING, SMALL_STRING };

enum class PandasStringFormat : uint8_t { OBJECT, ARROW };

struct PandasOutputConfig {
    PandasStringFormat default_string_format_ = PandasStringFormat::OBJECT;
};

struct ArrowOutputConfig {
    ArrowOutputStringFormat default_string_format_ = ArrowOutputStringFormat::LARGE_STRING;
    std::unordered_map<std::string, ArrowOutputStringFormat> per_column_string_format_;
};

using OutputConfig = std::variant<PandasOutputConfig, ArrowOutputConfig>;

} // namespace arcticdb
