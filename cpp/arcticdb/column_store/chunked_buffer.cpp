/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/chunked_buffer.hpp>

namespace arcticdb {

template <size_t BlockSize>
std::vector<ChunkedBufferImpl<BlockSize>> split(const ChunkedBufferImpl<BlockSize>& input, size_t nbytes) {
    const auto output_size = std::ceil(double(input.bytes()) / nbytes);
    std::vector<ChunkedBufferImpl<BlockSize>> output;
    output.reserve(static_cast<size_t>(output_size));
    ARCTICDB_DEBUG(log::version(), "Split buffer of size {} into chunks of {}", input.bytes(), nbytes);
    auto remaining_current_bytes = std::min(nbytes, input.bytes());
    auto remaining_total_bytes = input.bytes();
    ARCTICDB_DEBUG(log::version(), "Remaining total: {} Remaining current: {}", remaining_total_bytes, remaining_current_bytes);
    std::optional<ChunkedBufferImpl<BlockSize>> current_buf = ChunkedBufferImpl<BlockSize>::presized_in_blocks(std::min(nbytes, remaining_current_bytes));
    auto target_block = current_buf.value().blocks().begin();
    auto target_pos = 0u;
    auto block_num ARCTICDB_UNUSED = 0u;
    for(const auto block : input.blocks()) {
        ARCTICDB_DEBUG(log::version(), "## Block {}", block_num++);
        util::check(block->bytes(), "Zero-sized block");
        auto source_pos = 0u;
        auto source_bytes = block->bytes();
        while(source_bytes != 0) {
            if(!current_buf) {
                remaining_current_bytes = std::min(nbytes, remaining_total_bytes);
                current_buf = ChunkedBufferImpl<BlockSize>::presized_in_blocks(remaining_current_bytes);
                ARCTICDB_DEBUG(log::version(), "Creating new buffer with size {}", remaining_current_bytes);
                target_block = current_buf->blocks().begin();
            }
            const auto remaining_block_bytes = (*target_block)->bytes() - target_pos;
            const auto this_write = std::min({remaining_current_bytes, source_bytes, remaining_block_bytes});
            ARCTICDB_DEBUG(log::version(), "Calculated this write = {} ({}, {}, {})", this_write, remaining_current_bytes, source_bytes, remaining_block_bytes);
            util::check(target_block != current_buf->blocks().end(), "Went past end of blocks");
            ARCTICDB_DEBUG(log::version(), "Copying {} bytes from pos {} to pos {}", this_write, source_pos, target_pos);
            (*target_block)->copy_from(&(*block)[source_pos], this_write, target_pos);
            source_pos += this_write;
            source_bytes -= this_write;
            target_pos += this_write;
            remaining_current_bytes -= this_write;
            remaining_total_bytes -= this_write;
            ARCTICDB_DEBUG(log::version(), "Adjusted values source_pos {} source_bytes {} target_pos {} remaining_current {} remaining_total {}",
                                source_pos, source_bytes, target_pos, remaining_current_bytes, remaining_total_bytes);

            if(static_cast<size_t>((*target_block)->bytes()) == nbytes || target_pos == static_cast<size_t>((*target_block)->bytes())) {
                ARCTICDB_DEBUG(log::version(), "Incrementing block as nbytes == target block bytes: {}", nbytes);
                ++target_block;
                target_pos = 0;
            }

            if(remaining_current_bytes == 0) {
                ARCTICDB_DEBUG(log::version(), "Pushing buffer");
                output.push_back(std::move(current_buf.value()));
                current_buf.reset();
            }
        }
    }
    util::check(output.size() == output_size, "Unexpected size in chunked buffer split {} != {}", output.size(), output_size);
    return output;
}

template std::vector<ChunkedBufferImpl<64>> split(const ChunkedBufferImpl<64>& input, size_t nbytes);
template std::vector<ChunkedBufferImpl<3968>> split(const ChunkedBufferImpl<3968>& input, size_t nbytes);

// Inclusive of start_byte, exclusive of end_byte
template <size_t BlockSize>
ChunkedBufferImpl<BlockSize> truncate(const ChunkedBufferImpl<BlockSize>& input, size_t start_byte, size_t end_byte) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            end_byte >= start_byte,
            "ChunkedBufferImpl::truncate expects end_byte {} to be >= start_byte {}", end_byte, start_byte);
    ARCTICDB_DEBUG(log::version(), "Truncating buffer of size {} between bytes {} and {}", input.bytes(), start_byte, end_byte);
    const auto output_size = end_byte - start_byte;
    // This is trivially extendable to use presized_in_blocks, but there is no use case for this right now, and
    // copy_frame_data_to_buffer expects a contiguous buffer
    auto output = ChunkedBufferImpl<BlockSize>::presized(output_size);
    auto target_block = *output.blocks().begin();

    const auto& input_blocks = input.blocks();
    auto start_block_and_offset = input.block_and_offset(start_byte);
    auto start_idx = start_block_and_offset.block_index_;
    // end_byte is the first byte NOT to include in the output
    auto end_idx = input.block_and_offset(end_byte - 1).block_index_ + 1;

    auto target_pos = 0u;
    auto remaining_bytes = output_size;
    for (auto idx = start_idx; idx < end_idx; idx++) {
        auto input_block = input_blocks.at(idx);
        auto source_pos = idx == start_idx ? start_block_and_offset.offset_: 0u;
        auto source_bytes = std::min(remaining_bytes, input_block->bytes() - source_pos);
        while(source_bytes != 0) {
            const auto this_write = std::min(remaining_bytes, source_bytes);
            ARCTICDB_DEBUG(log::version(), "Calculated this write = {} ({}, {})", this_write, remaining_bytes,
                           source_bytes);
            ARCTICDB_DEBUG(log::version(), "Copying {} bytes from pos {} to pos {}", this_write, source_pos,
                           target_pos);
            target_block->copy_from(&(*input_block)[source_pos], this_write, target_pos);
            source_pos += this_write;
            source_bytes -= this_write;
            target_pos += this_write;
            remaining_bytes -= this_write;
        }
    }
    return output;
}

template ChunkedBufferImpl<64> truncate(const ChunkedBufferImpl<64>& input, size_t start_byte, size_t end_byte);
template ChunkedBufferImpl<3968> truncate(const ChunkedBufferImpl<3968>& input, size_t start_byte, size_t end_byte);

} //namespace arcticdb