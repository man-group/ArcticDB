/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

namespace arcticdb {

enum class ArrowOutputStringFormat : uint8_t { CATEGORICAL, LARGE_STRING, SMALL_STRING };

struct ArrowOutputConfig {
    ArrowOutputStringFormat default_string_format_ = ArrowOutputStringFormat::LARGE_STRING;
    std::unordered_map<std::string, ArrowOutputStringFormat> per_column_string_format_;
};

} // namespace arcticdb
