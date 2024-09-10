/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/arrow/arrow_utils.hpp>

#include <vector>
#include <memory>

namespace arcticdb {
struct ArrowData;

struct ArrowOutputFrame {
    ArrowOutputFrame(
        std::vector<std::vector<ArrowData>>&& data,
        std::vector<std::string>&& names);

    std::shared_ptr<std::vector<std::vector<ArrowData>>> data_;
    std::vector<std::string> names_;

    std::vector<std::vector<uintptr_t>> arrays();

    std::vector<std::vector<uintptr_t>> schemas();

    std::vector<std::string> names() const;

    size_t num_blocks() const;
};
} // namespace arcticdb