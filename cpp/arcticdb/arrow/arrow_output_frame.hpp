/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <cstring>
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
// Follows Rule of Five: move-only semantics to prevent double-free of Arrow structures.
struct RecordBatchData {
    RecordBatchData() {
        std::memset(&array_, 0, sizeof(array_));
        std::memset(&schema_, 0, sizeof(schema_));
    }

    RecordBatchData(ArrowArray array, ArrowSchema schema) : array_(array), schema_(schema) {}

    // Delete copy operations to prevent double-free
    RecordBatchData(const RecordBatchData&) = delete;
    RecordBatchData& operator=(const RecordBatchData&) = delete;

    // Move constructor - transfers ownership
    RecordBatchData(RecordBatchData&& other) noexcept : array_(other.array_), schema_(other.schema_) {
        // Clear source to prevent double-free
        other.array_.release = nullptr;
        other.schema_.release = nullptr;
    }

    // Move assignment - transfers ownership
    RecordBatchData& operator=(RecordBatchData&& other) noexcept {
        if (this != &other) {
            // Release current resources if owned
            release_if_owned();
            // Take ownership from other
            array_ = other.array_;
            schema_ = other.schema_;
            // Clear source
            other.array_.release = nullptr;
            other.schema_.release = nullptr;
        }
        return *this;
    }

    // Destructor - releases Arrow resources if not already transferred to Python
    ~RecordBatchData() { release_if_owned(); }

    ArrowArray array_;
    ArrowSchema schema_;

    uintptr_t array() { return reinterpret_cast<uintptr_t>(&array_); }

    uintptr_t schema() { return reinterpret_cast<uintptr_t>(&schema_); }

  private:
    void release_if_owned() {
        // Arrow C Data Interface: release is set to nullptr after being called
        // If release is non-null, we still own the memory and must free it
        if (array_.release != nullptr) {
            array_.release(&array_);
        }
        if (schema_.release != nullptr) {
            schema_.release(&schema_);
        }
    }
};

struct ArrowOutputFrame {
    ArrowOutputFrame() = default;

    ArrowOutputFrame(std::shared_ptr<std::vector<sparrow::record_batch>>&& data);

    std::shared_ptr<std::vector<sparrow::record_batch>> data_;

    std::vector<RecordBatchData> extract_record_batches();

    [[nodiscard]] size_t num_blocks() const;

  private:
    // Guards against multiple consumption of data_ via extract_record_batches().
    // The method destructively transfers ownership from the underlying sparrow::record_batch objects.
    bool data_consumed_ = false;
};
} // namespace arcticdb