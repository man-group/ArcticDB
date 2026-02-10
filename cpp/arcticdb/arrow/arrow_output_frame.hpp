/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <unordered_set>
#include <variant>
#include <vector>

#include <sparrow/c_interface.hpp>

#include <folly/futures/Future.h>

#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

// Anything that transitively includes sparrow.array.hpp takes ages to build the (unused by us) std::format impl
// So avoid including sparrow in headers where possible until this is resolved
namespace sparrow {
class record_batch;
}

namespace arcticdb {

namespace stream {
class StreamSource;
}

struct ExpressionContext;

// Forward declaration
class LazyRecordBatchIterator;

// FilterRange: same definition as pipelines::FilterRange from read_query.hpp,
// repeated here to avoid pulling in clause.hpp (which is very heavy to compile).
using FilterRange = std::variant<std::monostate, entity::IndexRange, pipelines::RowRange>;

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

// Lazy iterator that reads and decodes segments on-demand from storage.
// Instead of pre-loading all data, it holds segment metadata (keys) and reads
// one segment at a time in next(), with a configurable prefetch buffer for
// latency hiding. This enables querying symbols larger than available memory.
//
// Supports optional row-level truncation (date_range/row_range) and per-segment
// FilterClause application (WHERE pushdown from SQL). These are applied after
// decoding but before Arrow conversion, so DuckDB only sees the filtered data.
//
// Arrow conversion (prepare_segment_for_arrow + segment_to_arrow_data) runs on
// the CPU thread pool in parallel across segments. By the time next() is called,
// the RecordBatchData is already prepared.
class LazyRecordBatchIterator {
  public:
    LazyRecordBatchIterator(
            std::vector<pipelines::SliceAndKey> slice_and_keys, StreamDescriptor descriptor,
            std::shared_ptr<stream::StreamSource> store,
            std::shared_ptr<std::unordered_set<std::string>> columns_to_decode, FilterRange row_filter,
            std::shared_ptr<ExpressionContext> expression_context, std::string filter_root_node_name,
            size_t prefetch_size = 2
    );

    // Returns the next record batch by reading from storage, or nullopt if exhausted.
    std::optional<RecordBatchData> next();

    // Returns true if there are more segments to read.
    [[nodiscard]] bool has_next() const;

    // Returns the total number of segments.
    [[nodiscard]] size_t num_batches() const;

    // Returns the current position (0-indexed).
    [[nodiscard]] size_t current_index() const { return current_index_; }

    // Returns the stream descriptor (schema) for this iterator.
    // Used by Python to build a pyarrow.Schema even when there are no data segments.
    [[nodiscard]] const StreamDescriptor& descriptor() const { return descriptor_; }

  private:
    std::vector<pipelines::SliceAndKey> slice_and_keys_;
    StreamDescriptor descriptor_;
    std::shared_ptr<stream::StreamSource> store_;
    std::shared_ptr<std::unordered_set<std::string>> columns_to_decode_;
    size_t prefetch_size_;
    size_t current_index_ = 0;
    // Next segment index to submit for prefetch (may be ahead of current_index_)
    size_t next_prefetch_index_ = 0;

    // Row-level truncation for date_range/row_range filtering.
    // setup_pipeline_context() already filters segments at segment-granularity;
    // this truncates the boundary segments to exact row boundaries.
    FilterRange row_filter_;

    // Per-segment filter from QueryBuilder WHERE pushdown (SQL path).
    // If expression_context_ is non-null, each decoded segment is filtered through
    // the expression before Arrow conversion.
    std::shared_ptr<ExpressionContext> expression_context_;
    std::string filter_root_node_name_;

    // Prefetch buffer: queue of futures for fully prepared RecordBatchData.
    // Each future reads a segment from storage (IO thread), then runs
    // truncation + filter + prepare_segment_for_arrow + segment_to_arrow_data
    // on the CPU thread pool — all in parallel across segments.
    std::deque<folly::Future<std::vector<RecordBatchData>>> prefetch_buffer_;

    // Buffer for extra record batches when a single segment produces multiple blocks.
    // A segment's column data can span multiple ChunkedBuffer blocks (each 64KB),
    // and segment_to_arrow_data() produces one record_batch per block.
    std::deque<RecordBatchData> pending_batches_;

    // Submit a read+decode+prepare for one segment, returns a future that completes
    // with fully prepared RecordBatchData (Arrow conversion done on CPU thread pool).
    folly::Future<std::vector<RecordBatchData>> read_decode_and_prepare_segment(size_t idx);

    // Fill the prefetch buffer up to prefetch_size_ entries
    void fill_prefetch_buffer();

    // Apply row-level truncation to a decoded segment.
    // Static so it can be called from CPU thread pool lambdas without capturing `this`.
    static void apply_truncation(
            SegmentInMemory& segment, const pipelines::RowRange& slice_row_range, const FilterRange& row_filter
    );

    // Apply FilterClause expression to a decoded segment.
    // Returns true if the segment has rows remaining after filtering, false if empty.
    // Static so it can be called from CPU thread pool lambdas without capturing `this`.
    static bool apply_filter_clause(
            SegmentInMemory& segment, const std::shared_ptr<ExpressionContext>& expression_context,
            const std::string& filter_root_node_name
    );
};

} // namespace arcticdb