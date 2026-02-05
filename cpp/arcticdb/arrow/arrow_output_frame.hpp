/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <memory>
#include <optional>
#include <vector>

#include <sparrow/c_interface.hpp>

// Anything that transitively includes sparrow.array.hpp takes ages to build the (unused by us) std::format impl
// So avoid including sparrow in headers where possible until this is resolved
namespace sparrow {
class record_batch;
}

namespace arcticdb {

// Forward declaration
class RecordBatchIterator;

// C arrow representation of a record batch. Can be converted to a pyarrow.RecordBatch zero copy.
struct RecordBatchData {
    RecordBatchData() = default;

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

    // Create an iterator for streaming record batches one at a time.
    [[nodiscard]] std::shared_ptr<RecordBatchIterator> create_iterator() const;

    [[nodiscard]] size_t num_blocks() const;
};

// Iterator for streaming record batches one at a time.
// Used for memory-efficient integration with DuckDB and other Arrow consumers.
class RecordBatchIterator {
public:
    RecordBatchIterator() = default;

    explicit RecordBatchIterator(std::shared_ptr<std::vector<sparrow::record_batch>> data);

    // Returns the next record batch, or nullopt if exhausted.
    std::optional<RecordBatchData> next();

    // Returns true if there are more batches to iterate.
    [[nodiscard]] bool has_next() const;

    // Returns the schema from the first batch without consuming it.
    // Throws if iterator is empty.
    RecordBatchData peek_schema_batch();

    // Returns the total number of batches.
    [[nodiscard]] size_t num_batches() const;

    // Returns the current position (0-indexed).
    [[nodiscard]] size_t current_index() const { return current_index_; }

private:
    std::shared_ptr<std::vector<sparrow::record_batch>> data_;
    size_t current_index_ = 0;
};

} // namespace arcticdb