/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/util/allocator.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/column_store/block.hpp>

#include <boost/container/small_vector.hpp>

#include <cstdlib>
#include <cstdint>
#include <memory>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <vector>
#include <numeric>

namespace arcticdb {

template<size_t DefaultBlockSize>
class ChunkedBufferImpl {

    using BlockType = MemBlock;

    static_assert(sizeof(BlockType) == BlockType::Align + BlockType::MinSize);
    static_assert(DefaultBlockSize >= BlockType::MinSize);

  public:
    struct Iterator {
        ChunkedBufferImpl* parent_;
        BlockType* block_ = nullptr;
        size_t pos_ = 0;
        size_t block_num_ = 0;
        size_t type_size_;
        bool end_ = false;

        Iterator(
            ChunkedBufferImpl* parent,
            size_t type_size) :
                parent_(parent),
                type_size_(type_size) {
            if(parent_->empty()) {
                end_ = true;
                return;
            }

            block_ = parent_->blocks_[0];
        }

        [[nodiscard]] bool finished() const {
            return end_;
        }

        [[nodiscard]] uint8_t* value() const {
            return &(*block_)[pos_];
        }

        void next() {
            pos_ += type_size_;

            if(pos_ >= block_->bytes()) {
                if(block_num_ + 1 >= parent_->blocks_.size()) {
                    end_ = true;
                    return;
                }

                block_ = parent_->blocks_[++block_num_];
                pos_ = 0;
            }
        }
    };

    ChunkedBufferImpl() = default;

    explicit ChunkedBufferImpl(size_t size) {
        add_block(size ? size : DefaultBlockSize, 0u);
        block_offsets_.push_back(0);
    }

    ChunkedBufferImpl &operator=(ChunkedBufferImpl &&other) noexcept {
        using std::swap;
        swap(*this, other);
        other.clear();
        return *this;
    }

    ChunkedBufferImpl clone() const {
        ChunkedBufferImpl output;
        output.bytes_ = bytes_;
        output.regular_sized_until_ = regular_sized_until_;

        for(auto block : blocks_) {
            output.add_block(block->capacity_, block->offset_);
            (*output.blocks_.rbegin())->copy_from(block->data(), block->bytes(), 0);
            (*output.blocks_.rbegin())->resize(block->bytes());
        }

        output.block_offsets_ = block_offsets_;
        return output;
    }

    Iterator iterator(size_t size = 1) {
        return Iterator(this, size);
    }

    ChunkedBufferImpl(ChunkedBufferImpl&& other) noexcept {
        *this = std::move(other);
    }

    static auto presized(size_t size) {
        ChunkedBufferImpl output(size);
        output.ensure(size);
        return output;
    }

    static auto presized_in_blocks(size_t size) {
        ChunkedBufferImpl output;
        auto remaining = size;
        while(remaining != 0) {
            const auto alloc = std::min(remaining, DefaultBlockSize);
            output.ensure(output.bytes() + alloc);
            remaining -= alloc;
        }
        return output;
    }

    ARCTICDB_NO_COPY(ChunkedBufferImpl)

    ~ChunkedBufferImpl() {
        clear();
    }

    friend void swap(ChunkedBufferImpl &left, ChunkedBufferImpl &right) noexcept {
        using std::swap;
        swap(left.bytes_, right.bytes_);
        swap(left.regular_sized_until_, right.regular_sized_until_);
        swap(left.blocks_, right.blocks_);
        swap(left.block_offsets_, right.block_offsets_);
    }

    const auto &blocks() const { return blocks_; }

    void resize_first(size_t requested_size) {
        util::check(num_blocks() == 1, "resize_first called on buffer with {} blocks", num_blocks());
        if(requested_size <= blocks_[0]->capacity())
            return;

        free_last_block();
        add_block(requested_size, 0);
        bytes_ = requested_size;
    }

    // If the extra space required does not fit in the current last block, and is <=DefaultBlockSize, then if aligned is
    // set to true, the current last block will be padded with zeros, and a new default sized block added. This allows
    // the buffer to stay regular sized for as long as possible, which greatly improves random access performance, and
    // will also be beneficial with the slab allocator.
    uint8_t* ensure(size_t requested_size, bool aligned=false) {
        if (requested_size == 0 || requested_size <= bytes_)
            return last_block().end();

        uint8_t* res;
        auto extra_size = requested_size - bytes_;
        if (extra_size <= free_space()) {
            res = last_block().end();
            last_block().bytes_ += extra_size;
        } else {
            // Still regular-sized, add a new block
            if (is_regular_sized()) {
                auto space = free_space();
                if (extra_size <= DefaultBlockSize && (space == 0 || aligned)) {
                    if (aligned && space > 0) {
                        memset(last_block().end(), 0, space);
                        last_block().bytes_ += space;
                        bytes_ += space;
                    }
                    add_block(DefaultBlockSize, DefaultBlockSize * blocks().size());
                } else {
                    // Used to be regular sized but need an irregular block
                    handle_transition_to_irregular();
                    add_block(std::max(DefaultBlockSize, extra_size), DefaultBlockSize * blocks().size());
                }
            } else {
                // Already irregular sized
                size_t last_off = last_offset();
                util::check(regular_sized_until_ == *block_offsets_.begin(),
                            "Gap between regular sized blocks and irregular block offsets");
                if (last_block().empty()) {
                    free_last_block();
                } else {
                    block_offsets_.push_back(last_off + last_block().bytes());
                }

                add_block(std::max(DefaultBlockSize, extra_size), last_off);
            }
            res = last_block().end();
            last_block().bytes_ = extra_size;
        }
        bytes_ += extra_size;
        return res;
    }

    struct BlockAndOffset {
        MemBlock* block_;
        size_t offset_;
        size_t block_index_;

        BlockAndOffset(MemBlock* block, size_t offset, size_t block_index) :
            block_(block),
            offset_(offset),
            block_index_(block_index){
        }
    };

    BlockAndOffset block_and_offset(size_t pos_bytes) const {
        if(blocks_.size() == 1u)
            return BlockAndOffset(blocks_[0], pos_bytes, 0);

        if (is_regular_sized() || pos_bytes < regular_sized_until_) {
            size_t block_offset = pos_bytes / DefaultBlockSize;
            util::check(block_offset < blocks_.size(),
                        "Request for out of range block {}, only have {} blocks",
                        block_offset,
                        blocks_.size());
            MemBlock *block = blocks_[block_offset];
            block->magic_.check();
            return BlockAndOffset(block, pos_bytes % DefaultBlockSize, block_offset);
        }

        util::check(!block_offsets_.empty(), "Expected an irregular block-sized buffer to have offsets");
        auto block_offset = std::lower_bound(std::begin(block_offsets_), std::end(block_offsets_), pos_bytes);
        if (block_offset == block_offsets_.end() || *block_offset != pos_bytes)
            --block_offset;

        auto irregular_block_num = std::distance(block_offsets_.begin(), block_offset);
        auto first_irregular_block = regular_sized_until_ / DefaultBlockSize;
        auto block = blocks_[first_irregular_block + irregular_block_num];
        return BlockAndOffset(block, pos_bytes - *block_offset, first_irregular_block + irregular_block_num);
    }

    uint8_t &operator[](size_t pos_bytes) {
        auto [block, pos, _] = block_and_offset(pos_bytes);
        return (*block)[pos];
    }

    const uint8_t &operator[](size_t pos_bytes) const {
        return const_cast<ChunkedBufferImpl *>(this)->operator[](pos_bytes);
    }

    template<typename T>
    T &cast(size_t pos) {
        return reinterpret_cast<T &>(operator[](pos * sizeof(T)));
    }

    [[nodiscard]] size_t num_blocks() const {
        return blocks_.size();
    }

    [[nodiscard]] const uint8_t* data() const {
        util::check(blocks_.size() == 1, "Taking a pointer to the beginning of a non-contiguous buffer");
        blocks_[0]->magic_.check();
        return blocks_[0]->data();
    }

    [[nodiscard]] uint8_t* data() {
        return const_cast<uint8_t*>(const_cast<const ChunkedBufferImpl*>(this)->data());
    }

    void check_bytes(size_t pos_bytes, size_t required_bytes) const {
        if (pos_bytes + required_bytes > bytes()) {
            std::string err = fmt::format("Cursor overflow in chunked_buffer ptr_cast, cannot read {} bytes from a buffer of size {} with cursor "
                                          "at {}, as it would required {} bytes. ",
                                          required_bytes,
                                          bytes(),
                                          pos_bytes,
                                          pos_bytes + required_bytes
                                          );
            ARCTICDB_DEBUG(log::storage(), err);
            throw std::invalid_argument(err);
        }
    }

    template<typename T>
    T *ptr_cast(size_t pos_bytes, size_t required_bytes) {
        check_bytes(pos_bytes, required_bytes);
        return reinterpret_cast<T *>(&operator[](pos_bytes));
    }

    template<typename T>
    const T *ptr_cast(size_t pos_bytes, size_t required_bytes) const {
        return (const_cast<ChunkedBufferImpl*>(this)->ptr_cast<T>(pos_bytes, required_bytes));
    }

    template<typename T>
    const T* internal_ptr_cast(size_t pos_bytes, size_t required_bytes) const {
        check_bytes(pos_bytes, required_bytes);
        auto [block, pos, _] = block_and_offset(pos_bytes);
        return reinterpret_cast<const T*>(block->internal_ptr(pos));
    }

    void add_block(size_t capacity, size_t offset) {
        auto [ptr, ts] = Allocator::aligned_alloc(BlockType::alloc_size(capacity));
        new(ptr) MemBlock(capacity, offset, ts);
        blocks_.emplace_back(reinterpret_cast<BlockType*>(ptr));
    }

    void add_external_block(const uint8_t* data, size_t size, size_t offset) {
        if (!no_blocks() && last_block().empty())
            free_last_block();

        auto [ptr, ts] = Allocator::aligned_alloc(sizeof(MemBlock));
        new(ptr) MemBlock(data, size, offset, ts);
        blocks_.emplace_back(reinterpret_cast<BlockType*>(ptr));
        bytes_ += size;
    }

    bool empty() const { return bytes_ == 0; }

    void clear() {
        bytes_ = 0;
        for(auto block : blocks_)
            free_block(block);

        blocks_.clear();
        block_offsets_.clear();
    }

    bool is_regular_sized() const { return block_offsets_.empty(); }

    size_t bytes() const { return bytes_; }

    friend struct BufferView;
    BlockType &last_block() {
        return **blocks_.rbegin();
    }

    [[nodiscard]] size_t free_space() const {
        return no_blocks() ? 0 : last_block().free_space();
    }

    [[nodiscard]] size_t last_offset() const {
        return block_offsets_.empty() ? 0 : *block_offsets_.rbegin();
    }

    inline void assert_size(size_t bytes) const {
        util::check(bytes <= bytes_, "Expected allocation size {} smaller than actual allocation {}", bytes, bytes_);
    }

  private:
    void free_block(BlockType* block) const {
        ARCTICDB_TRACE(log::storage(), "Freeing block at address {:x}", uintptr_t(block));
        block->magic_.check();
        Allocator::free(std::make_pair(reinterpret_cast<uint8_t *>(block), block->ts_));
    }

    void free_last_block() {
        ARCTICDB_TRACE(log::inmem(), "Freeing last empty block from the buffer");
        free_block(&last_block());
        blocks_.pop_back();
    }

    void handle_transition_to_irregular() {
        if (!no_blocks() && last_block().empty()) {
            free_last_block();
        }

        // No blocks, so irregular blocks starts from the beginning
        if (blocks().empty()) {
            regular_sized_until_ = 0;
            block_offsets_.push_back(0);
        } else if (free_space() == 0) {
            // Regular up to here, irregular blocks start at this point
            regular_sized_until_ = bytes_;
            block_offsets_.push_back(bytes_);
        } else {
            // The last block wasn't full, so that block is also an irregular-sized block
            auto last_block_offset = bytes_ - last_block().bytes();
            regular_sized_until_ = last_block_offset;
            block_offsets_.push_back(last_block_offset);
            block_offsets_.push_back(bytes_);
        }
    }

    const BlockType &last_block() const {
        util::check(!blocks_.empty(), "There should never be no blocks");
        return **blocks_.rbegin();
    }

    [[nodiscard]] bool no_blocks() const { return blocks_.empty(); }

    size_t bytes_ = 0;
    size_t regular_sized_until_ = 0;
//#define DEBUG_BUILD
#ifndef DEBUG_BUILD
    boost::container::small_vector<BlockType *, 1> blocks_;
    boost::container::small_vector<size_t, 1> block_offsets_;
#else
    std::vector<BlockType*> blocks_;
    std::vector<size_t> block_offsets_;
#endif
};

constexpr size_t PageSize = 4096;
constexpr size_t BufferSize = MemBlock::raw_size(PageSize);
using ChunkedBuffer = ChunkedBufferImpl<BufferSize>;

template <size_t BlockSize>
std::vector<ChunkedBufferImpl<BlockSize>> split(const ChunkedBufferImpl<BlockSize>& input, size_t nbytes);

template <size_t BlockSize>
ChunkedBufferImpl<BlockSize> truncate(const ChunkedBufferImpl<BlockSize>& input, size_t start_byte, size_t end_byte);
}
