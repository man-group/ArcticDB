/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/lazy_record_batch_iterator.hpp>

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
#include <arcticdb/pipeline/lazy_read_helpers.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/util/lambda_inlining.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/type_handler.hpp>

namespace arcticdb {

namespace {

// Converts a column's buffer to DETACHABLE allocation if it isn't already.
// segment_to_arrow_data() calls block.release() to transfer memory ownership
// to Arrow, which only works on ExternalMemBlock (DETACHABLE allocation).
// When batch_read_uncompressed() is called with AllocationType::DETACHABLE (as the
// lazy iterator does), numeric columns are already detachable and this is a no-op.
// The memcpy path is only hit for:
//  - Sparse columns after unsparsify() (creates PRESIZED buffer)
//  - Fixed-width string columns (ASCII_FIXED64/UTF_FIXED64) which are explicitly
//    downgraded to PRESIZED in SegmentInMemoryImpl::create_columns()
void make_column_blocks_detachable(Column& column) {
    auto& buf = column.data().buffer();
    if (buf.allocation_type() == entity::AllocationType::DETACHABLE || buf.bytes() == 0) {
        return;
    }
    ChunkedBuffer detachable(buf.bytes(), entity::AllocationType::DETACHABLE);
    detachable.ensure(buf.bytes());
    auto* dest = detachable.data();
    for (const auto* block : buf.blocks()) {
        block->copy_to(dest);
        dest += block->logical_size();
    }
    std::swap(buf, detachable);
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

// Build a SharedStringDictionary from the unique string pool offsets actually
// referenced by CATEGORICAL columns in the segment. After truncation the string
// pool is shared and may contain entries not referenced by the (now smaller)
// column data, so scanning the columns directly gives a tight dictionary.
SharedStringDictionary build_shared_dictionary(const SegmentInMemory& segment, const ReadOptions& read_options) {
    SharedStringDictionary dict;
    dict.dict_offsets.push_back(0); // Arrow offsets start at 0

    auto string_pool = segment.string_pool_ptr();
    if (!string_pool || string_pool->size() == 0) {
        return dict;
    }

    // Collect unique pool offsets referenced by CATEGORICAL string columns.
    ArrowStringHandler arrow_string_handler;
    ankerl::unordered_dense::set<StringPool::offset_t> referenced_offsets;

    for (auto col_idx = 0UL; col_idx < segment.num_columns(); ++col_idx) {
        const auto& field = segment.field(col_idx);
        if (!is_dynamic_string_type(field.type().data_type())) {
            continue;
        }
        auto string_format = arrow_string_handler.output_string_format(field.name(), read_options);
        if (string_format != ArrowOutputStringFormat::CATEGORICAL) {
            continue;
        }
        const auto& column = *segment.columns()[col_idx];
        details::visit_type(column.type().data_type(), [&](auto source_tag) {
            using source_type_info = ScalarTypeInfo<decltype(source_tag)>;
            if constexpr (is_sequence_type(source_type_info::data_type)) {
                for_each_enumerated<typename source_type_info::TDT>(column, [&](const auto& en) {
                    if (is_a_string(en.value())) {
                        referenced_offsets.insert(en.value());
                    }
                });
            }
        });
    }

    if (referenced_offsets.empty()) {
        return dict;
    }

    // Sort by pool offset for deterministic dictionary ordering
    std::vector<StringPool::offset_t> sorted_offsets(referenced_offsets.begin(), referenced_offsets.end());
    std::sort(sorted_offsets.begin(), sorted_offsets.end());

    int64_t string_buffer_pos = 0;
    for (auto offset : sorted_offsets) {
        auto str = string_pool->get_const_view(offset);
        dict.offset_to_index[offset] = dict.unique_count++;
        dict.dict_strings.insert(dict.dict_strings.end(), str.begin(), str.end());
        string_buffer_pos += static_cast<int64_t>(str.size());
        dict.dict_offsets.push_back(string_buffer_pos);
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
// String columns contain raw string pool offsets that must be resolved. This function:
// 1. Builds a shared string dictionary from the pool (once per segment, shared across columns)
// 2. For string columns (CATEGORICAL): encodes dictionary keys using the shared dictionary
// 3. For string columns (LARGE/SMALL_STRING): falls back to per-column ArrowStringHandler
// 4. For non-string columns: ensures blocks are detachable (no-op when decoded with
//    AllocationType::DETACHABLE; only copies for sparse or fixed-width string columns)
void prepare_segment_for_arrow(SegmentInMemory& segment, const ReadOptions& caller_read_options) {
    auto string_pool = segment.string_pool_ptr();
    DecodePathData shared_data;
    std::any handler_data;
    // Start with the caller's read options (which may have arrow_string_format_default set).
    // Ensure output_format is ARROW so ArrowStringHandler is used.
    ReadOptions read_options = caller_read_options.clone();
    if (read_options.output_format() != OutputFormat::ARROW) {
        read_options.set_output_format(OutputFormat::ARROW);
    }

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
        shared_dict = build_shared_dictionary(segment, read_options);
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
            // Non-string column: handle sparse columns, ensure blocks are detachable
            if (src_column_ptr->opt_sparse_map().has_value()) {
                // Sparse float column (from sparsify_floats=True): create Arrow
                // validity bitmap from the sparse map, then densify the column.
                // Must extract bitmap BEFORE unsparsify() clears the sparse map.
                auto& bv = src_column_ptr->sparse_map();
                bv.resize(segment.row_count());
                create_dense_bitmap(0, bv, *src_column_ptr, AllocationType::DETACHABLE);
                src_column_ptr->unsparsify(segment.row_count());
            }
            make_column_blocks_detachable(*src_column_ptr);
        }
    }
}

} // anonymous namespace

// LazyRecordBatchIterator implementation

LazyRecordBatchIterator::LazyRecordBatchIterator(
        std::vector<pipelines::SliceAndKey> slice_and_keys, StreamDescriptor descriptor,
        std::shared_ptr<stream::StreamSource> store, std::shared_ptr<std::unordered_set<std::string>> columns_to_decode,
        FilterRange row_filter, std::shared_ptr<ExpressionContext> expression_context,
        std::string filter_root_node_name, size_t prefetch_size, size_t max_prefetch_bytes, ReadOptions read_options
) :
    slice_and_keys_(std::move(slice_and_keys)),
    descriptor_(std::move(descriptor)),
    store_(std::move(store)),
    columns_to_decode_(std::move(columns_to_decode)),
    prefetch_size_(std::max(prefetch_size, size_t{1})),
    row_filter_(std::move(row_filter)),
    expression_context_(std::move(expression_context)),
    filter_root_node_name_(std::move(filter_root_node_name)),
    max_prefetch_bytes_(max_prefetch_bytes),
    read_options_(std::move(read_options)) {
    // Detect column slicing: slice_and_keys_ is sorted by (row_range, col_range).
    // If any two consecutive entries share the same row_range, the symbol has
    // column slicing (multiple column slices per row group).
    for (size_t i = 1; i < slice_and_keys_.size(); ++i) {
        if (slice_and_keys_[i].slice_.row_range == slice_and_keys_[i - 1].slice_.row_range) {
            has_column_slicing_ = true;
            break;
        }
    }

    // Build target schema from descriptor for schema padding.
    // The descriptor contains ALL columns (including index) from the merged TimeseriesDescriptor.
    // All formats are resolved eagerly here so that pad_batch_to_schema can create
    // correctly-typed null columns without waiting for the first batch (which matters
    // for column-sliced symbols where string columns may arrive in a later slice).
    ArrowStringHandler arrow_string_handler;
    for (const auto& field : descriptor_.fields()) {
        std::string name(field.name());
        // If column projection is active, only include projected columns
        if (columns_to_decode_ && !columns_to_decode_->empty() && !columns_to_decode_->count(name)) {
            continue;
        }
        TargetField tf;
        tf.name = name;
        if (is_sequence_type(field.type().data_type())) {
            auto string_format = arrow_string_handler.output_string_format(name, read_options_);
            if (string_format == ArrowOutputStringFormat::CATEGORICAL) {
                // Dictionary-encoded: int32 keys with large_string dictionary
                tf.arrow_format = "i";
                tf.is_dictionary = true;
            } else {
                auto [output_type, _] =
                        arrow_string_handler.output_type_and_extra_bytes(field.type(), name, read_options_);
                tf.arrow_format = default_arrow_format_for_type(output_type.data_type());
            }
        } else {
            tf.arrow_format = default_arrow_format_for_type(field.type().data_type());
        }
        tf.format_resolved = true;
        target_fields_.push_back(std::move(tf));
    }
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
    auto futures =
            store_->batch_read_uncompressed(std::move(ranges), columns_to_decode_, entity::AllocationType::DETACHABLE);
    util::check(!futures.empty(), "Expected at least one future from batch_read_uncompressed");

    // Capture shared state by value/copy for the CPU task lambda.
    // row_filter_ is cheap to copy (variant of small structs).
    // expression_context_ is shared_ptr (immutable after construction, safe for concurrent reads).
    // read_options_ is cheap to copy (shared_ptr to data internally).
    auto row_filter = row_filter_;
    auto expr_ctx = expression_context_;
    auto filter_name = filter_root_node_name_;
    auto read_opts = read_options_;
    auto skip_filter = has_column_slicing_;

    // Chain CPU-intensive work (truncation, filter, Arrow conversion) onto the IO future.
    // This runs on the CPU thread pool, enabling parallel Arrow conversion across segments.
    return std::move(futures[0])
            .via(&async::cpu_executor())
            .thenValue(
                    [slice_row_range,
                     row_filter = std::move(row_filter),
                     expr_ctx = std::move(expr_ctx),
                     filter_name = std::move(filter_name),
                     read_opts = std::move(read_opts),
                     skip_filter](pipelines::SegmentAndSlice&& segment_and_slice) -> std::vector<RecordBatchData> {
                        auto& segment = segment_and_slice.segment_in_memory_;

                        // Use shared helpers from pipeline/lazy_read_helpers.hpp
                        arcticdb::apply_truncation(segment, slice_row_range, row_filter);

                        // For column-sliced symbols, skip per-segment filter evaluation.
                        // The filter column may be in a different column slice, so applying
                        // it per-segment would produce row count mismatches after horizontal
                        // merge. DuckDB applies WHERE post-merge instead.
                        if (!skip_filter) {
                            if (!arcticdb::apply_filter_clause(segment, expr_ctx, filter_name)) {
                                return {}; // All rows filtered out
                            }
                        }

                        prepare_segment_for_arrow(segment, read_opts);

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
    while (prefetch_buffer_.size() < prefetch_size_ && current_prefetch_bytes_ < max_prefetch_bytes_ &&
           next_prefetch_index_ < slice_and_keys_.size()) {
        auto estimated_bytes = estimate_segment_bytes(slice_and_keys_[next_prefetch_index_], descriptor_);
        prefetch_buffer_.emplace_back(read_decode_and_prepare_segment(next_prefetch_index_));
        current_prefetch_bytes_ += estimated_bytes;
        ++next_prefetch_index_;
    }
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
        // Decrement byte estimate for this segment before consuming the future
        auto consumed_bytes = estimate_segment_bytes(slice_and_keys_[current_index_], descriptor_);
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                consumed_bytes <= current_prefetch_bytes_,
                "Prefetch byte accounting mismatch: consumed {} > tracked {}",
                consumed_bytes,
                current_prefetch_bytes_
        );
        current_prefetch_bytes_ -= std::min(consumed_bytes, current_prefetch_bytes_);

        auto row_range = slice_and_keys_[current_index_].slice_.row_range;

        auto batches = std::move(prefetch_buffer_.front()).get();
        prefetch_buffer_.pop_front();
        ++current_index_;
        fill_prefetch_buffer();

        if (batches.empty()) {
            // This column slice produced no data (filtered out). Consume any remaining
            // same-row-group slices too, since they'd be for the same empty row group.
            while (!prefetch_buffer_.empty() && current_index_ < slice_and_keys_.size() &&
                   slice_and_keys_[current_index_].slice_.row_range == row_range) {
                auto cb = estimate_segment_bytes(slice_and_keys_[current_index_], descriptor_);
                current_prefetch_bytes_ -= std::min(cb, current_prefetch_bytes_);
                std::move(prefetch_buffer_.front()).get(); // Discard
                prefetch_buffer_.pop_front();
                ++current_index_;
                fill_prefetch_buffer();
            }
            continue;
        }

        // Column-slice merging: consume consecutive slices with the same row_range
        // and merge their Arrow batches horizontally (adding columns).
        while (!prefetch_buffer_.empty() && current_index_ < slice_and_keys_.size() &&
               slice_and_keys_[current_index_].slice_.row_range == row_range) {
            auto cb = estimate_segment_bytes(slice_and_keys_[current_index_], descriptor_);
            current_prefetch_bytes_ -= std::min(cb, current_prefetch_bytes_);

            auto next_batches = std::move(prefetch_buffer_.front()).get();
            prefetch_buffer_.pop_front();
            ++current_index_;
            fill_prefetch_buffer();

            if (next_batches.empty()) {
                continue; // This slice was filtered out, skip
            }

            // Merge block-by-block. In practice, prepare_segment_for_arrow consolidates
            // to a single block, so both vectors are typically size 1.
            auto merge_count = std::min(batches.size(), next_batches.size());
            for (size_t i = 0; i < merge_count; ++i) {
                batches[i] = horizontal_merge_arrow_batches(std::move(batches[i]), std::move(next_batches[i]));
            }
            // If next_batches had more blocks, append the extras
            for (size_t i = merge_count; i < next_batches.size(); ++i) {
                batches.push_back(std::move(next_batches[i]));
            }
        }

        // Schema padding: all target formats are resolved eagerly in the constructor.
        // resolve_target_fields_from_batch is kept as a safety net for any edge cases
        // where the batch has a more specific format than the descriptor-derived default.
        if (!target_fields_.empty()) {
            resolve_target_fields_from_batch(target_fields_, batches[0].schema_);
            for (auto& b : batches) {
                b = pad_batch_to_schema(std::move(b), target_fields_);
            }
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
