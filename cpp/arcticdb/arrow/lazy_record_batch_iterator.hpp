/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

#include <folly/futures/Future.h>

#include <arcticdb/arrow/arrow_output_frame.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/pipeline/filter_range.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb {

namespace stream {
struct StreamSource;
}

struct ExpressionContext;

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
            size_t prefetch_size = 2, size_t max_prefetch_bytes = 4ULL * 1024 * 1024 * 1024,
            ReadOptions read_options = ReadOptions{}
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

    // Returns the SliceAndKey at the current consumption position.
    // Used by column-slice merging (Phase 5) to check slice boundaries.
    [[nodiscard]] const pipelines::SliceAndKey& current_slice_and_key() const {
        return slice_and_keys_[current_index_];
    }

    // Peeks at the SliceAndKey at position (current_index_ + offset).
    // Returns nullptr if the position is out of range.
    [[nodiscard]] const pipelines::SliceAndKey* peek_slice_and_key(size_t offset) const {
        auto idx = current_index_ + offset;
        return idx < slice_and_keys_.size() ? &slice_and_keys_[idx] : nullptr;
    }

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

    // Fill the prefetch buffer up to prefetch_size_ entries (with dual-cap backpressure)
    void fill_prefetch_buffer();

    // Maximum prefetch bytes in flight (dual-cap backpressure, default 4GB).
    // Prevents OOM with wide tables where each segment may be hundreds of MB.
    size_t max_prefetch_bytes_;
    // Current estimated uncompressed bytes in the prefetch buffer.
    size_t current_prefetch_bytes_ = 0;

    // ReadOptions controlling string format (SMALL_STRING vs LARGE_STRING vs CATEGORICAL).
    // Passed through to prepare_segment_for_arrow() and used to build the target schema.
    ReadOptions read_options_;

    // True if this symbol has column slicing (multiple column slices per row group).
    // Detected at construction time by scanning slice_and_keys_ for consecutive entries
    // with the same row_range. When true AND expression_context_ is set, per-segment
    // filter evaluation is skipped (DuckDB applies WHERE post-merge instead).
    bool has_column_slicing_ = false;

    // Target schema for padding: each batch is padded to have exactly these columns
    // in this order. Built from the descriptor at construction time, with formats
    // lazily resolved from the first batch containing each column.
    std::vector<TargetField> target_fields_;
};

} // namespace arcticdb
