/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
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
        auto source_pos = 0u;
        util::check(block->bytes(), "Zero-sized block");
        auto source_bytes = block->bytes() - (source_pos == 0 ? 0 : source_pos - 1);
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

} //namespace arcticdb