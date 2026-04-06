/* Copyright 2026 Man Group Operations Limited
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
#include <arcticdb/util/constructors.hpp>

// Anything that transitively includes sparrow.array.hpp takes ages to build the (unused by us) std::format impl
// So avoid including sparrow in headers where possible until this is resolved
namespace sparrow {
class record_batch;
}

namespace arcticdb {

// Arrow C Data interface to pass ArrowArray/ArrowSchema pair representing a record batch between C++ and Python
//
// Write path (Python -> C++):
//   1. Python calls _export_to_c into array_/schema_, setting release callbacks that decref
//      the underlying PyArrow buffers.
//   2. py_ndf_to_frame moves array_/schema_ into owning sparrow::record_batches via
//      record_batch(ArrowArray&&, ArrowSchema&&). Sparrow takes ownership and will call
//      release (decref) on destruction.
//   3. The sparrow record batches are used to construct a SegmentInMemory whose external blocks
//      point into the ArrowArray buffers. InputFrame stores the sparrow record_batches
//      to keep the buffers alive for the segment's lifetime.
//   4. WriteToSegmentTask slices the InputFrame and writes each slice to storage.
//   5. Once all slices are persisted the InputFrame is destroyed, sparrow record_batches are
//      destroyed, calling release which decrefs the PyArrow buffers.
//
// Read path (C++ -> Python):
//   1. Arrow buffers are allocated in allocate_chunked_frame / decode_or_expand.
//   2. After decoding, buffers are given to sparrow which sets up release callbacks based
//      on the allocator used.
//   3. ArrowOutputFrame holds the sparrow::record_batches and is returned to Python.
//   4. Python calls extract_record_batches() to create RecordBatchData via
//      sparrow::extract_arrow_structures (which makes the sparrow::record_batch non-owning).
//   5. Python calls _import_from_c which takes ownership.
struct RecordBatchData {
    RecordBatchData() : array_{}, schema_{} {}
    RecordBatchData(ArrowArray array, ArrowSchema schema);

    ARCTICDB_MOVE_ONLY_DEFAULT(RecordBatchData);

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
