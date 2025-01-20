/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/arrow/arrow_utils.hpp>

#include <sparrow/sparrow.hpp>
#include <sparrow/record_batch.hpp>

#include <vector>
#include <memory>

namespace arcticdb {
struct ArrowData;

struct RecordBatchData {
    uintptr_t array_;
    uintptr_t schema_;
};

struct ArrowOutputFrame {
    ArrowOutputFrame() = default;

    ArrowOutputFrame(
        std::shared_ptr<std::vector<sparrow::record_batch>>&& data,
        std::vector<std::string>&& names);

    std::shared_ptr<std::vector<sparrow::record_batch>> data_;
    std::vector<std::string> names_;

    std::vector<RecordBatchData> record_batches();

    [[nodiscard]] std::vector<std::string> names() const;

    [[nodiscard]] size_t num_blocks() const;
};
} // namespace arcticdb