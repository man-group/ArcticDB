/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/arrow/arrow_utils.hpp>

#include <arcticdb/arrow/include_sparrow.hpp>

#include <vector>
#include <memory>

namespace arcticdb {
struct ArrowData;

struct RecordBatchData {
    RecordBatchData(ArrowArray array, ArrowSchema schema) :
        array_(array),
        schema_(schema) {
    }

    ArrowArray array_;
    ArrowSchema schema_;

    uintptr_t array() {
        return reinterpret_cast<uintptr_t>(&array_);
    }

    uintptr_t schema() {
        return reinterpret_cast<uintptr_t>(&schema_);
    }
};

struct ArrowOutputFrame {
    ArrowOutputFrame() = default;

    ArrowOutputFrame(
        std::shared_ptr<std::vector<sparrow::record_batch>>&& data,
        std::vector<std::string>&& names);

    std::shared_ptr<std::vector<sparrow::record_batch>> data_;
    std::vector<std::string> names_;
    std::vector<sparrow::struct_array> struct_arrays_;

    std::vector<RecordBatchData> record_batches();

    [[nodiscard]] std::vector<std::string> names() const;

    [[nodiscard]] size_t num_blocks() const;
};
} // namespace arcticdb