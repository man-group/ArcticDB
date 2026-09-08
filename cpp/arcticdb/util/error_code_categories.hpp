/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/error_code.hpp>

#include <unordered_map>

namespace arcticdb {

// // FUTURE(GCC9): use magic_enum
inline std::unordered_map<ErrorCategory, const char*> get_error_category_names() {
    return {
            {ErrorCategory::INTERNAL, "INTERNAL"},
            {ErrorCategory::NORMALIZATION, "NORMALIZATION"},
            {ErrorCategory::MISSING_DATA, "MISSING_DATA"},
            {ErrorCategory::SCHEMA, "SCHEMA"},
            {ErrorCategory::STORAGE, "STORAGE"},
            {ErrorCategory::SORTING, "SORTING"},
            {ErrorCategory::USER_INPUT, "USER_INPUT"},
            {ErrorCategory::COMPATIBILITY, "COMPATIBILITY"},
            {ErrorCategory::CODEC, "CODEC"},
    };
}

}
