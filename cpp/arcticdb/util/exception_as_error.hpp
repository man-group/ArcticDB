/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <folly/Function.h>
#include <variant>

namespace arcticdb {

// Utility to treat exceptions as data
struct Error {

    explicit Error(folly::Function<void(std::string)> raiser, std::string msg);
    void throw_error();

    folly::Function<void(std::string)> raiser_;
    std::string msg_;
};

using CheckOutcome = std::variant<Error, std::monostate>;

}
