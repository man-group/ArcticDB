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
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/async/tasks.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/pipeline/filter_segment.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/util/lambda_inlining.hpp>
#include <arcticdb/util/offset_string.hpp>
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

// Shared string dictionary built once per segment from the string pool.
// All string columns in a segment share the same pool, so we walk it once
// and build Arrow-ready dictionary buffers + an offset→index mapping that
// each column can use for O(1) dictionary key lookups during its row scan.
struct SharedStringDictionary {
    // Pool offset → sequential dictionary index (0, 1, 2, ...)
    ankerl::unordered_dense::map<StringPool::offset_t, int32_t> offset_to_index;
    // Arrow dictionary values: cumulative byte offsets into dict_strings
    std::vector<int64_t> dict_offsets;
    // Arrow dictionary values: concatenated UTF-8 string data
    std::vector<char> dict_strings;
    int32_t unique_count = 0;
};

// Walk the string pool buffer sequentially, building a SharedStringDictionary.
// The pool stores entries as [uint32_t size][char data...] packed back-to-back,
// with minimum entry size of 8 bytes (sizeof(uint32_t) + 4 inline data bytes).
// This is O(U) where U = number of unique strings in the pool, typically much
// smaller than the total row count across all columns.
SharedStringDictionary build_shared_dictionary(const std::shared_ptr<StringPool>& string_pool) {
    SharedStringDictionary dict;
    dict.dict_offsets.push_back(0); // Arrow offsets start at 0

    if (!string_pool || string_pool->size() == 0) {
        return dict;
    }

    // Use the public StringPool API to walk entries.
    // get_const_view(offset) returns the string at that pool offset.
    // Entry layout: [uint32_t size_][char data_[>=4]] with min entry size 8 bytes.
    // We read the size field directly from the buffer to calculate stride.
    const auto& buffer = string_pool->data();
    const auto pool_bytes = buffer.bytes();

    // Minimum entry size: sizeof(uint32_t) + 4 = 8 bytes (matching StringHead layout)
    constexpr size_t MIN_ENTRY_SIZE = 8;
    constexpr size_t SIZE_FIELD_BYTES = sizeof(uint32_t);

    position_t pos = 0;
    int64_t string_buffer_pos = 0;
    while (static_cast<size_t>(pos) + MIN_ENTRY_SIZE <= pool_bytes) {
        // Read the uint32_t size field at the start of the entry
        auto* size_ptr = buffer.internal_ptr_cast<uint32_t>(pos, SIZE_FIELD_BYTES);
        auto str_size = static_cast<size_t>(*size_ptr);
        auto entry_size = std::max(SIZE_FIELD_BYTES + str_size, MIN_ENTRY_SIZE);

        // Bounds check: ensure the full entry fits in the buffer
        if (static_cast<size_t>(pos) + entry_size > pool_bytes) {
            break;
        }

        // Use the public get_const_view API to get the string data safely
        auto str = string_pool->get_const_view(pos);

        // Map this pool offset to a sequential dictionary index
        dict.offset_to_index[pos] = dict.unique_count++;

        // Append string data to the dictionary buffers
        dict.dict_strings.insert(dict.dict_strings.end(), str.begin(), str.end());
        string_buffer_pos += static_cast<int64_t>(str.size());
        dict.dict_offsets.push_back(string_buffer_pos);

        pos += static_cast<position_t>(entry_size);
    }

    return dict;
}

// Encode a string column's dictionary keys using a pre-built SharedStringDictionary.
// Instead of the per-column encode_dictionary() which does find-or-insert per row
// (building the dictionary incrementally), this does read-only lookups against the
// shared dictionary. The hash map is small (sized to unique count, not row count)
// and read-only, giving better cache behavior and branch prediction.
void encode_dictionary_with_shared_dict(
        const Column& source_column, Column& dest_column, const ColumnMapping& mapping,
        const SharedStringDictionary& shared_dict
) {
    auto dest_ptr = reinterpret_cast<int32_t*>(dest_column.bytes_at(mapping.offset_bytes_, mapping.dest_bytes_));

    util::BitSet dest_bitset;
    util::BitSet::bulk_insert_iterator inserter(dest_bitset);
    bool populate_inverted_bitset = !source_column.opt_sparse_map().has_value();

    details::visit_type(source_column.type().data_type(), [&](auto source_tag) {
        using source_type_info = ScalarTypeInfo<decltype(source_tag)>;
        if constexpr (is_sequence_type(source_type_info::data_type)) {
            for_each_enumerated<typename source_type_info::TDT>(
                    source_column,
                    [&] ARCTICDB_LAMBDA_INLINE(const auto& en) {
                        if (is_a_string(en.value())) {
                            auto it = shared_dict.offset_to_index.find(en.value());
                            util::check(
                                    it != shared_dict.offset_to_index.end(),
                                    "String pool offset {} not found in shared dictionary",
                                    en.value()
                            );
                            dest_ptr[en.idx()] = it->second;
                            if (!populate_inverted_bitset) {
                                inserter = en.idx();
                            }
                        } else if (populate_inverted_bitset) {
                            inserter = en.idx();
                        }
                    }
            );
        } else {
            util::raise_rte("Unexpected non-string type in shared dictionary encoder");
        }
    });

    inserter.flush();
    if (populate_inverted_bitset) {
        dest_bitset.invert();
    }
    dest_bitset.resize(mapping.num_rows_);

    if (dest_bitset.count() != dest_bitset.size()) {
        handle_truncation(dest_bitset, mapping.truncate_);
        create_dense_bitmap(mapping.offset_bytes_, dest_bitset, dest_column, AllocationType::DETACHABLE);
    }

    // Attach dictionary buffers (OFFSET + STRING) copied from the shared dictionary.
    // Each column gets its own copy because Column owns its extra buffers.
    if (dest_bitset.count() > 0 && shared_dict.unique_count > 0) {
        auto& string_buffer = dest_column.create_extra_buffer(
                mapping.offset_bytes_,
                ExtraBufferType::STRING,
                shared_dict.dict_strings.size(),
                AllocationType::DETACHABLE
        );
        std::memcpy(string_buffer.data(), shared_dict.dict_strings.data(), shared_dict.dict_strings.size());

        auto& offsets_buffer = dest_column.create_extra_buffer(
                mapping.offset_bytes_,
                ExtraBufferType::OFFSET,
                shared_dict.dict_offsets.size() * sizeof(int64_t),
                AllocationType::DETACHABLE
        );
        std::memcpy(
                offsets_buffer.data(),
                shared_dict.dict_offsets.data(),
                shared_dict.dict_offsets.size() * sizeof(int64_t)
        );
    }
}

// Prepares a decoded segment for Arrow conversion.
// The decoded segment from batch_read_uncompressed has DYNAMIC allocation (inline blocks)
// and string columns contain raw string pool offsets. This function:
// 1. Builds a shared string dictionary from the pool (once per segment, shared across columns)
// 2. For string columns (CATEGORICAL): encodes dictionary keys using the shared dictionary
// 3. For string columns (LARGE/SMALL_STRING): falls back to per-column ArrowStringHandler
// 4. For non-string columns: converts inline blocks to detachable (for block.release())
void prepare_segment_for_arrow(SegmentInMemory& segment) {
    auto string_pool = segment.string_pool_ptr();
    DecodePathData shared_data;
    std::any handler_data;
    ReadOptions read_options;
    read_options.set_output_format(OutputFormat::ARROW);

    // Check if we have any dynamic string columns that can use the shared dictionary path.
    // UTF_FIXED64 columns store UTF-32 data and need special conversion, so they fall back
    // to the per-column ArrowStringHandler which handles UTF-32→UTF-8 conversion.
    bool has_dynamic_string_cols = false;
    for (auto col_idx = 0UL; col_idx < segment.num_columns(); ++col_idx) {
        if (is_dynamic_string_type(segment.field(col_idx).type().data_type())) {
            has_dynamic_string_cols = true;
            break;
        }
    }

    // Build shared dictionary from the string pool once per segment.
    // Only for dynamic strings — fixed-width strings need per-column UTF-32→UTF-8 conversion.
    std::optional<SharedStringDictionary> shared_dict;
    if (has_dynamic_string_cols && string_pool && string_pool->size() > 0) {
        shared_dict = build_shared_dictionary(string_pool);
    }

    for (auto col_idx = 0UL; col_idx < segment.num_columns(); ++col_idx) {
        auto& src_column_ptr = segment.columns()[col_idx];
        const auto& field = segment.field(col_idx);

        if (is_sequence_type(field.type().data_type())) {
            // String column: determine output type and create destination column
            ArrowStringHandler arrow_handler;
            auto [output_type, extra_bytes] =
                    arrow_handler.output_type_and_extra_bytes(field.type(), field.name(), read_options);

            const auto num_rows = static_cast<size_t>(src_column_ptr->row_count());
            const auto dest_size = data_type_size(output_type);
            const auto dest_bytes = num_rows * dest_size;

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

            // Use shared dictionary for dynamic string columns with CATEGORICAL output
            auto string_format = arrow_handler.output_string_format(field.name(), read_options);
            if (shared_dict.has_value() && is_dynamic_string_type(field.type().data_type()) &&
                string_format == ArrowOutputStringFormat::CATEGORICAL) {
                encode_dictionary_with_shared_dict(*src_column_ptr, *dest_column, mapping, *shared_dict);
            } else {
                // Fallback: fixed-width strings or non-CATEGORICAL format
                arrow_handler.convert_type(
                        *src_column_ptr, *dest_column, mapping, shared_data, handler_data, string_pool, read_options
                );
            }
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

std::vector<RecordBatchData> ArrowOutputFrame::extract_record_batches() {
    util::check(
            !data_consumed_, "Cannot extract record batches: data has already been consumed by extract_record_batches()"
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

folly::Future<std::vector<RecordBatchData>> LazyRecordBatchIterator::read_decode_and_prepare_segment(size_t idx) {
    auto& sk = slice_and_keys_[idx];
    auto slice_row_range = sk.slice_.row_range;
    pipelines::RangesAndKey ranges_and_key(sk.slice_, entity::AtomKey(sk.key()), false);
    std::vector<pipelines::RangesAndKey> ranges;
    ranges.emplace_back(std::move(ranges_and_key));
    auto futures = store_->batch_read_uncompressed(std::move(ranges), columns_to_decode_);
    util::check(!futures.empty(), "Expected at least one future from batch_read_uncompressed");

    // Capture shared state by value/copy for the CPU task lambda.
    // row_filter_ is cheap to copy (variant of small structs).
    // expression_context_ is shared_ptr (immutable after construction, safe for concurrent reads).
    auto row_filter = row_filter_;
    auto expr_ctx = expression_context_;
    auto filter_name = filter_root_node_name_;

    // Chain CPU-intensive work (truncation, filter, Arrow conversion) onto the IO future.
    // This runs on the CPU thread pool, enabling parallel Arrow conversion across segments.
    return std::move(futures[0])
            .via(&async::cpu_executor())
            .thenValue(
                    [slice_row_range,
                     row_filter = std::move(row_filter),
                     expr_ctx = std::move(expr_ctx),
                     filter_name = std::move(filter_name)](pipelines::SegmentAndSlice&& segment_and_slice
                    ) -> std::vector<RecordBatchData> {
                        auto& segment = segment_and_slice.segment_in_memory_;

                        apply_truncation(segment, slice_row_range, row_filter);

                        if (!apply_filter_clause(segment, expr_ctx, filter_name)) {
                            return {}; // All rows filtered out
                        }

                        prepare_segment_for_arrow(segment);

                        auto arrow_batches = segment_to_arrow_data(segment);
                        if (!arrow_batches || arrow_batches->empty()) {
                            return {};
                        }

                        std::vector<RecordBatchData> result;
                        result.reserve(arrow_batches->size());
                        for (auto& batch : *arrow_batches) {
                            auto struct_array = sparrow::array{batch.extract_struct_array()};
                            auto [arr, schema] = sparrow::extract_arrow_structures(std::move(struct_array));
                            result.emplace_back(arr, schema);
                        }
                        return result;
                    }
            );
}

void LazyRecordBatchIterator::fill_prefetch_buffer() {
    while (prefetch_buffer_.size() < prefetch_size_ && next_prefetch_index_ < slice_and_keys_.size()) {
        prefetch_buffer_.emplace_back(read_decode_and_prepare_segment(next_prefetch_index_));
        ++next_prefetch_index_;
    }
}

void LazyRecordBatchIterator::apply_truncation(
        SegmentInMemory& segment, const pipelines::RowRange& slice_row_range, const FilterRange& row_filter
) {
    util::variant_match(
            row_filter,
            [&segment](const entity::IndexRange& index_filter) {
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
            [&segment, &slice_row_range](const pipelines::RowRange& rr_filter) {
                // Row-based truncation (row_range / LIMIT).
                // Calculate the overlap between this segment's row range and the filter.
                const auto num_rows = segment.row_count();
                if (num_rows == 0) {
                    return;
                }
                auto seg_start = static_cast<int64_t>(slice_row_range.first);
                auto filter_start = static_cast<int64_t>(rr_filter.first);
                auto filter_end = static_cast<int64_t>(rr_filter.second);

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

bool LazyRecordBatchIterator::apply_filter_clause(
        SegmentInMemory& segment, const std::shared_ptr<ExpressionContext>& expression_context,
        const std::string& filter_root_node_name
) {
    if (!expression_context) {
        return true; // No filter — keep all rows
    }
    if (segment.row_count() == 0) {
        return false;
    }

    ExpressionName root_node_name(filter_root_node_name);
    ProcessingUnit proc(std::move(segment));
    proc.set_expression_context(expression_context);
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

    // Each future already contains fully prepared RecordBatchData
    // (truncation, filter, prepare_segment_for_arrow, segment_to_arrow_data
    // all ran on the CPU thread pool).
    while (!prefetch_buffer_.empty()) {
        auto batches = std::move(prefetch_buffer_.front()).get();
        prefetch_buffer_.pop_front();
        ++current_index_;
        fill_prefetch_buffer();

        if (batches.empty()) {
            continue; // Segment was empty after truncation/filtering
        }

        // Queue extra batches from multi-block segments
        for (size_t i = 1; i < batches.size(); ++i) {
            pending_batches_.emplace_back(std::move(batches[i]));
        }
        return std::move(batches[0]);
    }

    return std::nullopt;
}

} // namespace arcticdb