/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <memory>
#include <vector>

#include <sparrow/c_interface.hpp>

// Anything that transitively includes sparrow.array.hpp takes ages to build the (unused by us) std::format impl
// So avoid including sparrow in headers where possible until this is resolved
namespace sparrow {
class record_batch;
}

namespace arcticdb {

// C arrow representation of a record batch. Can be converted to a pyarrow.RecordBatch zero copy.
struct RecordBatchData {
    RecordBatchData(ArrowArray array, ArrowSchema schema) : array_(array), schema_(schema) {}

    ArrowArray array_;
    ArrowSchema schema_;

    uintptr_t array() { return reinterpret_cast<uintptr_t>(&array_); }

    uintptr_t schema() { return reinterpret_cast<uintptr_t>(&schema_); }
};

struct ArrowOutputFrame {
    ArrowOutputFrame() = default;

    ArrowOutputFrame(std::shared_ptr<std::vector<sparrow::record_batch>>&& data);

    std::shared_ptr<std::vector<sparrow::record_batch>> data_;

    std::vector<RecordBatchData> extract_record_batches();

    [[nodiscard]] size_t num_blocks() const;
};
} // namespace arcticdb