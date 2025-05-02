#pragma once

#include <arcticdb/pipelines/input_tensor_frame.hpp>
#include <sparrow/record_batch.hpp>

namespace arcticdb {

struct ArrowFrameBundle {
    pipelines::InputTensorFrame tensor_frame;  // ArcticDB descriptor
    std::vector <sparrow::record_batch> record_batches;  // Sparrow record_batches
};


arcticdb::DataType map_sparrow_to_arcticdb(sparrow::data_type sparrow_type) {
    using namespace sparrow;
    using namespace arcticdb;

    switch (sparrow_type) {
        // Primitive Types
    case data_type::BOOL:
        return DataType::BOOL8;
    case data_type::UINT8:
        return DataType::UINT8;
    case data_type::INT8:
        return DataType::INT8;
    case data_type::UINT16:
        return DataType::UINT16;
    case data_type::INT16:
        return DataType::INT16;
    case data_type::UINT32:
        return DataType::UINT32;
    case data_type::INT32:
        return DataType::INT32;
    case data_type::UINT64:
        return DataType::UINT64;
    case data_type::INT64:
        return DataType::INT64;
    case data_type::FLOAT:
        return DataType::FLOAT32;
    case data_type::DOUBLE:
        return DataType::FLOAT64;

        // String and Binary Types
    case data_type::STRING:
    case data_type::LARGE_STRING:
        return DataType::UTF_DYNAMIC64;
    case data_type::BINARY:
    case data_type::LARGE_BINARY:
        return DataType::BYTES;

        // Temporal Types
    case data_type::TIMESTAMP_NANOSECONDS:
        return DataType::NANOSECONDS_UTC64;
    case data_type::DATE_DAYS:
        return DataType::INT32;

        // Special Types
    case data_type::NA:
        return DataType::EMPTYVAL;

        // Unsupported Types
    default:
        throw std::invalid_argument("Unsupported Sparrow data type for ArcticDB mapping");
    }
}
ArrowFrameBundle create_arrow_frame_bundle(
    const StreamId& stream_id,
    const std::vector<uintptr_t>& array_ptrs,
    const std::vector<uintptr_t>& schema_ptrs
) {
    // Validate input sizes
    if (array_ptrs.size() != schema_ptrs.size()) {
        throw std::invalid_argument("Mismatch between number of Arrow arrays and schemas");
    }

    // Initialize the ArrowFrameBundle
    ArrowFrameBundle bundle;

    // Create record_batches from the Arrow pointers
    for (size_t i = 0; i < array_ptrs.size(); ++i) {
        // Cast uintptr_t to ArrowArray and ArrowSchema pointers
        ArrowArray* arrow_array = reinterpret_cast<ArrowArray*>(array_ptrs[i]);
        ArrowSchema* arrow_schema = reinterpret_cast<ArrowSchema*>(schema_ptrs[i]);

        // Wrap Arrow structures into Sparrow arrays
        sparrow::array sparrow_array(arrow_array, arrow_schema);

        // Create a record_batch from the Sparrow arrays
        bundle.record_batches.emplace_back(
            sparrow::record_batch(std::vector<sparrow::array>{sparrow_array})
        );
    }

    // Prepare the InputTensorFrame descriptor
    auto fields = std::make_shared<FieldCollection>();
    for (const auto& batch : bundle.record_batches) {
        for (const auto& column : batch.columns()) {
            fields->add({column.data_type(), column.name().value_or("")});
        }
    }

    // Create the descriptor for the InputTensorFrame
    auto descriptor = std::make_shared<StreamDescriptor>(stream_id, IndexDescriptor{}, fields);
    bundle.tensor_frame = arcticdb::pipelines::InputTensorFrame{descriptor};

    // Return the populated ArrowFrameBundle
    return bundle;
}
struct SliceSegment {
    size_t record_batch_index; // Which record-batch provides the data.
    size_t start_row;          // Row offset within that record batch.
    size_t num_rows;           // How many rows to take.
};

std::vector<std::vector<SliceSegment>> slice_record_batches(
    const std::vector<size_t>& row_counts,
    size_t desired_row_count,
    size_t tolerance)
{
    std::vector<std::vector<SliceSegment>> result;
    std::vector<SliceSegment> current_slice;
    size_t current_slice_rows = 0;

    // Process each record batch
    for (size_t rb_index = 0; rb_index < row_counts.size(); ++rb_index) {
        size_t batch_total = row_counts[rb_index];
        size_t offset = 0;
        while (offset < batch_total) {
            size_t remaining_in_batch = batch_total - offset;
            // How many rows do we still need to finish this slice?
            size_t needed = desired_row_count > current_slice_rows ?
                            desired_row_count - current_slice_rows : 0;
            // We'll take at most 'needed' or, if the batch has extra rows, the whole remainder.
            size_t take = std::min(remaining_in_batch, needed);
            // If needed is zero (i.e. if current_slice_rows is already >= desired_row_count)
            // then we can take all remaining rows from this batch in a new slice.
            if(needed == 0) {
                take = remaining_in_batch;
            }
            current_slice.push_back({rb_index, offset, take});
            current_slice_rows += take;
            offset += take;
            // Check if current slice is “complete” within tolerance.
            if (current_slice_rows >= desired_row_count - tolerance &&
                current_slice_rows <= desired_row_count + tolerance) {
                result.push_back(current_slice);
                current_slice.clear();
                current_slice_rows = 0;
            }
                // If the current slice gets too big, push it out anyway.
            else if (current_slice_rows > desired_row_count + tolerance) {
                result.push_back(current_slice);
                current_slice.clear();
                current_slice_rows = 0;
            }
        }
    }
    // If some rows remain in the current slice, output them.
    if (!current_slice.empty()) {
        result.push_back(current_slice);
    }
    return result;
}

void populate_chunked_buffers(
    ArrowFrameBundle& bundle,
    const std::vector<std::vector<SliceSegment>>& slice_groups,
    size_t /*desired_row_count*/  // not used directly here because each slice group already has its row counts.
) {
    // For simplicity, assume all record_batches have the same number of columns.
    if(bundle.record_batches.empty()) {
        throw std::runtime_error("No record batches in bundle.");
    }
    size_t num_columns = bundle.record_batches.front().nb_columns();
    // Resize the InputTensorFrame's data vector to have an entry for each column.
    bundle.tensor_frame.data_.resize(num_columns);

    // Process each column.
    for (size_t col = 0; col < num_columns; ++col) {
        // Use the first record batch's column as an example to get the type and element size.
        const auto& example_array = bundle.record_batches.front().get_column(col);
        sparrow::data_type sparrow_dt = example_array.data_type(); // Assuming this returns a sparrow::data_type
        arcticdb::DataType arctic_dt = map_sparrow_to_arcticdb(sparrow_dt);
        size_t element_size = get_type_size(arctic_dt); // in bytes

        // Create a new ChunkedBuffer for this column.
        ChunkedBuffer cb;
        size_t current_offset = 0;

        // Loop over each slice group (each slice is a tile covering a set of rows).
        for (const auto& slice_group : slice_groups) {
            // Compute the total number of rows in this slice by summing each segment.
            size_t total_rows = 0;
            for (const SliceSegment& seg : slice_group) {
                total_rows += seg.num_rows;
            }
            size_t block_bytes = total_rows * element_size;

            if (slice_group.empty()) {
                throw std::runtime_error("Empty slice group encountered.");
            }

            if (slice_group.size() == 1) {
                // The entire slice comes from one record-batch.
                const SliceSegment& seg = slice_group.front();
                const auto& col_array = bundle.record_batches[seg.record_batch_index].get_column(col);
                const uint8_t* base_ptr = reinterpret_cast<const uint8_t*>(col_array.data());
                // Compute pointer offset: start_row * element_size.
                const uint8_t* slice_ptr = base_ptr + (seg.start_row * element_size);
                // In the non-copying case, we add one external block.
                cb.add_external_block(slice_ptr, block_bytes, current_offset);
            } else {
                // Multiple segments: We copy the fragmented data into preallocated contiguous memory.
                // Ask the ChunkedBuffer to allocate room for block_bytes.
                // (Assume cb.ensure returns a pointer to the newly allocated contiguous region.)
                uint8_t* dest_ptr = cb.ensure(block_bytes);
                size_t dest_offset = 0;
                // Loop over each segment.
                for (const SliceSegment& seg : slice_group) {
                    const auto& col_array = bundle.record_batches[seg.record_batch_index].get_column(col);
                    const uint8_t* src_base = reinterpret_cast<const uint8_t*>(col_array.data());
                    const uint8_t* src_ptr = src_base + (seg.start_row * element_size);
                    size_t segment_bytes = seg.num_rows * element_size;
                    std::memcpy(dest_ptr + dest_offset, src_ptr, segment_bytes);
                    dest_offset += segment_bytes;
                }
                // Add the pre-allocated block as a detachable block (owned by ChunkedBuffer).
                cb.add_detachable_block(dest_ptr, block_bytes, current_offset);
            }
            current_offset += block_bytes;
        }
        // Store the finished ChunkedBuffer for this column into the InputTensorFrame.
        bundle.tensor_frame.data_[col] = std::move(cb);
    }
}

} // namespace arcticdb