/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/arrow_output_frame.hpp>

#include <vector>

#include <sparrow/record_batch.hpp>

#include <arcticdb/arrow/arrow_handlers.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/async/tasks.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/pipeline/filter_segment.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/type_handler.hpp>

namespace arcticdb {

namespace {

// Converts all inline (non-external) blocks in a column to detachable blocks.
// This is required before calling segment_to_arrow_data(), which calls block.release()
// to transfer ownership of memory to Arrow. release() only works on external/detachable blocks.
void make_column_blocks_detachable(Column& column) {
    for (auto* block : column.blocks()) {
        if (!block->is_external() && block->bytes() > 0) {
            auto* detachable_ptr = allocate_detachable_memory(block->bytes());
            std::memcpy(detachable_ptr, block->data(), block->bytes());
            block->external_data_ = detachable_ptr;
            block->owns_external_data_ = true;
        }
    }
}

// Prepares a decoded segment for Arrow conversion.
// The decoded segment from batch_read_uncompressed has DYNAMIC allocation (inline blocks)
// and string columns contain raw string pool offsets. This function:
// 1. For non-string columns: converts inline blocks to detachable (for block.release())
// 2. For string columns: creates a new DETACHABLE column with proper Arrow structure
//    (dictionary keys + OFFSET/STRING extra buffers) using ArrowStringHandler
void prepare_segment_for_arrow(SegmentInMemory& segment) {
    auto string_pool = segment.string_pool_ptr();
    DecodePathData shared_data;
    std::any handler_data;
    ReadOptions read_options;
    read_options.set_output_format(OutputFormat::ARROW);

    for (auto col_idx = 0UL; col_idx < segment.num_columns(); ++col_idx) {
        auto& src_column_ptr = segment.columns()[col_idx];
        const auto& field = segment.field(col_idx);

        if (is_sequence_type(field.type().data_type())) {
            // String column: use ArrowStringHandler to create proper Arrow buffers
            ArrowStringHandler arrow_handler;
            auto [output_type, extra_bytes] =
                    arrow_handler.output_type_and_extra_bytes(field.type(), field.name(), read_options);

            const auto num_rows = static_cast<size_t>(src_column_ptr->row_count());
            const auto dest_size = data_type_size(output_type);
            const auto dest_bytes = num_rows * dest_size;

            // Create a new DETACHABLE column for Arrow output.
            // Use expected_rows=0 then explicitly allocate to get exactly one block,
            // matching the block structure of the other (non-string) columns in the segment.
            // Note: extra_bytes is passed to the Column constructor which sets extra_bytes_per_block
            // on the ChunkedBuffer. create_detachable_block() automatically adds extra_bytes_per_block
            // to each block's capacity, so we must NOT add extra_bytes to alloc_bytes here.
            auto dest_column = std::make_shared<Column>(
                    output_type, 0, AllocationType::DETACHABLE, Sparsity::PERMITTED, extra_bytes
            );
            if (dest_bytes > 0) {
                dest_column->allocate_data(dest_bytes);
                dest_column->advance_data(dest_bytes);
            }

            const ColumnMapping mapping{
                    src_column_ptr->type(),
                    output_type,
                    field,
                    dest_size,
                    num_rows,
                    0, // first_row
                    0, // offset_bytes (single block, starts at 0)
                    dest_bytes,
                    col_idx
            };

            arrow_handler.convert_type(
                    *src_column_ptr, *dest_column, mapping, shared_data, handler_data, string_pool, read_options
            );
            dest_column->set_inflated(num_rows);

            // Replace the column shared_ptr in the segment
            src_column_ptr = std::move(dest_column);

            // Update the field type if it changed (e.g. UTF_DYNAMIC64 -> UTF_DYNAMIC32 for CATEGORICAL)
            if (output_type != field.type()) {
                segment.descriptor().mutable_field(col_idx).mutable_type() = output_type;
            }
        } else {
            // Non-string column: just make blocks detachable
            make_column_blocks_detachable(*src_column_ptr);
        }
    }
}

} // anonymous namespace

ArrowOutputFrame::ArrowOutputFrame(std::shared_ptr<std::vector<sparrow::record_batch>>&& data) :
    data_(std::move(data)) {}

size_t ArrowOutputFrame::num_blocks() const {
    if (!data_ || data_->empty())
        return 0;

    return data_->size();
}

std::shared_ptr<RecordBatchIterator> ArrowOutputFrame::create_iterator() {
    util::check(
            !data_consumed_,
            "Cannot create iterator: data has already been consumed by extract_record_batches() or create_iterator()"
    );
    data_consumed_ = true;

    return std::make_shared<RecordBatchIterator>(data_);
}

std::vector<RecordBatchData> ArrowOutputFrame::extract_record_batches() {
    util::check(
            !data_consumed_,
            "Cannot extract record batches: data has already been consumed by extract_record_batches() or "
            "create_iterator()"
    );
    data_consumed_ = true;

    std::vector<RecordBatchData> output;
    if (!data_) {
        return output;
    }
    output.reserve(data_->size());

    for (auto& batch : *data_) {
        auto struct_array = sparrow::array{batch.extract_struct_array()};
        auto [arr, schema] = sparrow::extract_arrow_structures(std::move(struct_array));

        output.emplace_back(arr, schema);
    }

    return output;
}

// RecordBatchIterator implementation

RecordBatchIterator::RecordBatchIterator(std::shared_ptr<std::vector<sparrow::record_batch>> data) :
    data_(std::move(data)),
    current_index_(0) {}

bool RecordBatchIterator::has_next() const { return data_ && current_index_ < data_->size(); }

size_t RecordBatchIterator::num_batches() const {
    if (!data_) {
        return 0;
    }
    return data_->size();
}

std::optional<RecordBatchData> RecordBatchIterator::next() {
    if (!has_next()) {
        return std::nullopt;
    }

    auto& batch = (*data_)[current_index_++];
    auto struct_array = sparrow::array{batch.extract_struct_array()};
    auto [arr, schema] = sparrow::extract_arrow_structures(std::move(struct_array));

    return RecordBatchData{arr, schema};
}

// LazyRecordBatchIterator implementation

LazyRecordBatchIterator::LazyRecordBatchIterator(
        std::vector<pipelines::SliceAndKey> slice_and_keys, StreamDescriptor descriptor,
        std::shared_ptr<stream::StreamSource> store, std::shared_ptr<std::unordered_set<std::string>> columns_to_decode,
        FilterRange row_filter, std::shared_ptr<ExpressionContext> expression_context,
        std::string filter_root_node_name, size_t prefetch_size
) :
    slice_and_keys_(std::move(slice_and_keys)),
    descriptor_(std::move(descriptor)),
    store_(std::move(store)),
    columns_to_decode_(std::move(columns_to_decode)),
    prefetch_size_(std::max(prefetch_size, size_t{1})),
    row_filter_(std::move(row_filter)),
    expression_context_(std::move(expression_context)),
    filter_root_node_name_(std::move(filter_root_node_name)) {
    fill_prefetch_buffer();
}

bool LazyRecordBatchIterator::has_next() const { return !pending_batches_.empty() || !prefetch_buffer_.empty(); }

size_t LazyRecordBatchIterator::num_batches() const { return slice_and_keys_.size(); }

folly::Future<pipelines::SegmentAndSlice> LazyRecordBatchIterator::read_and_decode_segment(size_t idx) {
    auto& sk = slice_and_keys_[idx];
    pipelines::RangesAndKey ranges_and_key(sk.slice_, entity::AtomKey(sk.key()), false);
    std::vector<pipelines::RangesAndKey> ranges;
    ranges.emplace_back(std::move(ranges_and_key));
    auto futures = store_->batch_read_uncompressed(std::move(ranges), columns_to_decode_);
    util::check(!futures.empty(), "Expected at least one future from batch_read_uncompressed");
    return std::move(futures[0]);
}

void LazyRecordBatchIterator::fill_prefetch_buffer() {
    while (prefetch_buffer_.size() < prefetch_size_ && next_prefetch_index_ < slice_and_keys_.size()) {
        prefetch_buffer_.emplace_back(read_and_decode_segment(next_prefetch_index_));
        ++next_prefetch_index_;
    }
}

void LazyRecordBatchIterator::apply_truncation(SegmentInMemory& segment, const pipelines::RowRange& slice_row_range) {
    util::variant_match(
            row_filter_,
            [&segment, &slice_row_range](const entity::IndexRange& index_filter) {
                // Timestamp-based truncation (date_range).
                // The segment's index column (column 0) contains timestamps.
                // Binary search to find the exact row boundaries within this segment.
                const auto& time_filter = static_cast<const TimestampRange&>(index_filter);
                const auto num_rows = segment.row_count();
                if (num_rows == 0) {
                    return;
                }
                auto index_column = segment.column_ptr(0);
                auto first_ts = *index_column->scalar_at<timestamp>(0);
                auto last_ts = *index_column->scalar_at<timestamp>(num_rows - 1);

                // Only truncate if the filter boundary falls inside this segment
                if ((time_filter.first > first_ts && time_filter.first <= last_ts) ||
                    (time_filter.second >= first_ts && time_filter.second < last_ts)) {
                    auto start_row = index_column->search_sorted<timestamp>(time_filter.first, false);
                    auto end_row = index_column->search_sorted<timestamp>(time_filter.second, true);
                    segment = segment.truncate(start_row, end_row, false);
                } else if (time_filter.first > last_ts) {
                    // Filter starts after all rows in this segment — empty result
                    segment = segment.truncate(0, 0, false);
                }
            },
            [&segment, &slice_row_range](const pipelines::RowRange& row_filter) {
                // Row-based truncation (row_range / LIMIT).
                // Calculate the overlap between this segment's row range and the filter.
                const auto num_rows = segment.row_count();
                if (num_rows == 0) {
                    return;
                }
                auto seg_start = static_cast<int64_t>(slice_row_range.first);
                auto filter_start = static_cast<int64_t>(row_filter.first);
                auto filter_end = static_cast<int64_t>(row_filter.second);

                // Local row indices within this segment
                auto local_start = std::max(int64_t{0}, filter_start - seg_start);
                auto local_end = std::min(static_cast<int64_t>(num_rows), filter_end - seg_start);

                if (local_start > 0 || local_end < static_cast<int64_t>(num_rows)) {
                    segment = segment.truncate(
                            static_cast<size_t>(local_start),
                            static_cast<size_t>(std::max(local_end, int64_t{0})),
                            false
                    );
                }
            },
            [](const std::monostate&) {
                // No filter — nothing to truncate
            }
    );
}

bool LazyRecordBatchIterator::apply_filter_clause(SegmentInMemory& segment) {
    if (!expression_context_) {
        return true; // No filter — keep all rows
    }
    if (segment.row_count() == 0) {
        return false;
    }

    ExpressionName root_node_name(filter_root_node_name_);
    ProcessingUnit proc(std::move(segment));
    proc.set_expression_context(expression_context_);
    auto variant_data = proc.get(root_node_name);

    bool has_rows = false;
    util::variant_match(
            variant_data,
            [&proc, &has_rows](util::BitSet& bitset) {
                if (bitset.count() > 0) {
                    proc.apply_filter(std::move(bitset), PipelineOptimisation::SPEED);
                    has_rows = true;
                }
            },
            [](EmptyResult) { /* Empty — no rows match */ },
            [&has_rows](FullResult) {
                has_rows = true; // All rows match — no filtering needed
            },
            [](const auto&) { util::raise_rte("Expected bitset from filter clause in lazy iterator"); }
    );

    if (has_rows) {
        // Extract the filtered segment back from the ProcessingUnit
        segment = std::move(*proc.segments_->at(0));
    }
    return has_rows;
}

std::optional<RecordBatchData> LazyRecordBatchIterator::next() {
    // Drain any buffered batches from a previous multi-block segment first
    if (!pending_batches_.empty()) {
        auto batch_data = std::move(pending_batches_.front());
        pending_batches_.pop_front();
        return batch_data;
    }

    if (prefetch_buffer_.empty()) {
        return std::nullopt;
    }

    // Block on the next segment (should already be ready or nearly ready due to prefetch)
    auto segment_and_slice = std::move(prefetch_buffer_.front()).get();
    prefetch_buffer_.pop_front();
    ++current_index_;

    // Kick off reads for the next segments
    fill_prefetch_buffer();

    auto& segment = segment_and_slice.segment_in_memory_;

    // Apply row-level truncation for date_range/row_range
    apply_truncation(segment, segment_and_slice.ranges_and_key_.row_range());

    // Apply FilterClause expression (WHERE pushdown)
    if (!apply_filter_clause(segment)) {
        // All rows filtered out — try next segment
        return next();
    }

    // Prepare the decoded segment for Arrow conversion:
    // - Non-string columns: make inline blocks detachable for block.release()
    // - String columns: resolve string pool offsets into proper Arrow dictionary/string buffers
    prepare_segment_for_arrow(segment);

    // Convert the decoded SegmentInMemory to Arrow record batches.
    // A single segment can produce multiple record batches when column data spans
    // multiple ChunkedBuffer blocks (each block = one batch, blocks are ~64KB).
    auto arrow_batches = segment_to_arrow_data(segment);
    if (!arrow_batches || arrow_batches->empty()) {
        // Empty segment — try next
        return next();
    }

    // Convert all batches from this segment to RecordBatchData
    for (auto& batch : *arrow_batches) {
        auto struct_array = sparrow::array{batch.extract_struct_array()};
        auto [arr, schema] = sparrow::extract_arrow_structures(std::move(struct_array));
        pending_batches_.emplace_back(arr, schema);
    }

    // Return the first, rest will be returned by subsequent next() calls
    auto batch_data = std::move(pending_batches_.front());
    pending_batches_.pop_front();
    return batch_data;
}

} // namespace arcticdb