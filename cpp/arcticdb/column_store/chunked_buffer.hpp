

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
#include <arcticdb/util/hash.hpp>

#ifndef DEBUG_BUILD
#include <boost/container/small_vector.hpp>
#endif

namespace arcticdb {

/*
 * ChunkedBuffer is an untyped buffer that is composed of blocks of data that can be either regularly or
 * irregularly sized, with optimizations for the following representations:
 *
 *   - a single block
 *   - multiple blocks that are all regularly sized
 *   - multiple blocks that are regularly sized up until a point, and irregularly sized after that point
 *
 * Lookup is log(n) where n is the number of blocks when the blocks are all irregularly sized.
 *
 * This class can be wrapped in a cursor for the purposes of linear reads and writes (see CursoredBuffer),
 * and subsequently detached if required.
 */
template<size_t DefaultBlockSize>
class ChunkedBufferImpl {

    using BlockType = MemBlock;

    static_assert(sizeof(BlockType) == BlockType::Align + BlockType::MinSize);
    static_assert(DefaultBlockSize >= BlockType::MinSize);

  public:
    constexpr static size_t block_size = DefaultBlockSize;

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

    explicit ChunkedBufferImpl(entity::AllocationType allocation_type) :
            allocation_type_(allocation_type) {}

    ChunkedBufferImpl(size_t size, entity::AllocationType allocation_type) :
            allocation_type_(allocation_type) {
        if(allocation_type == entity::AllocationType::DETACHABLE) {
            add_detachable_block(size, 0UL);
            bytes_ = size;
        } else {
            reserve(size);
        }
    }

    void reserve(size_t size) {
        if(size > 0) {
            if (size > DefaultBlockSize) {
                handle_transition_to_irregular();
            }
            add_block(std::max(size, DefaultBlockSize), 0UL);
        }
    }

    ChunkedBufferImpl &operator=(ChunkedBufferImpl &&other) noexcept {
        using std::swap;
        swap(*this, other);
        other.clear();
        return *this;
    }

    [[nodiscard]] ChunkedBufferImpl clone() const {
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
        ChunkedBufferImpl output(entity::AllocationType::PRESIZED);
        if (size > 0) {
            if (size != DefaultBlockSize) {
                output.handle_transition_to_irregular();
            }
            output.add_block(size, 0UL);
        }
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
        swap(left.allocation_type_, right.allocation_type_);
    }

    [[nodiscard]] const auto &blocks() const { return blocks_; }

    [[nodiscard]] const auto &block_offsets() const { return block_offsets_; }

    BlockType* block(size_t pos) {
        util::check(pos < blocks_.size(), "Requested block {} out of range {}", pos, blocks_.size());
        return blocks_[pos];
    }

    // If the extra space required does not fit in the current last block, and is <=DefaultBlockSize, then if aligned is
    // set to true, the current last block will be padded with zeros, and a new default sized block added. This allows
    // the buffer to stay regular sized for as long as possible, which greatly improves random access performance, and
    // will also be beneficial with the slab allocator.
    uint8_t* ensure(size_t requested_size, bool aligned = false) {
        if (requested_size != 0 && requested_size <= bytes_)
            return last_block().end();

        if(requested_size == 0)
            return nullptr;

        uint8_t* res;
        auto extra_size = requested_size - bytes_;
        if (extra_size <= free_space()) {
            res = last_block().end();
            last_block().bytes_ += extra_size;
        } else {
            if(allocation_type_ == entity::AllocationType::DETACHABLE) {
                add_detachable_block(extra_size, bytes_);
            } else if (is_regular_sized()) {
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

    // Trim will reduce the size of the chunked buffer to the specified size by dropping blocks that are wholly unneeded
    // i.e. no allocation/memcpy is involved, only deallocation
    // Use in performance critical code where a column is being created, the final size is unknown at construction
    // time but a maximum size is known, by creating a Column using a chunked buffer that is presized in blocks. This
    // unlocks ColumnDataIterator usage (more performant than repeated calls to Column::push_back). Once the column is
    // created and the number of elements known, use this to drop unneeded blocks.
    void trim(size_t requested_size) {
        if (requested_size == 0) {
            clear();
        } else {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(requested_size <= bytes_,
                                                            "Cannot trim ChunkedBuffer with {} bytes to {} bytes",
                                                            bytes_,
                                                            requested_size);
            while (bytes_ - last_block().bytes() >= requested_size) {
                bytes_ -= last_block().bytes();
                free_last_block();
            }
            last_block().resize(last_block().bytes() - (bytes_ - requested_size));
            bytes_ = requested_size;
        }
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

    [[nodiscard]] BlockAndOffset block_and_offset(size_t pos_bytes) const {
        if(blocks_.size() == 1u) {
            return BlockAndOffset(blocks_[0], pos_bytes, 0);
        }

        if (is_regular_sized() || pos_bytes < regular_sized_until_) {
            size_t block_offset = pos_bytes / DefaultBlockSize;
            util::check(block_offset < blocks_.size(),
                        "Request for out of range block {}, only have {} blocks",
                        block_offset,
                        blocks_.size());
            ARCTICDB_TRACE(log::inmem(), "Chunked buffer returning regular block {}, position {}", block_offset, pos_bytes % DefaultBlockSize);
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
        const auto block_pos = irregular_block_num + first_irregular_block;
        util::check(block_pos < blocks_.size(), "Block {} out of bounds in blocks buffer of size {}", block_pos, blocks_.size());
        auto block = blocks_[first_irregular_block + irregular_block_num];
        ARCTICDB_TRACE(log::inmem(), "Chunked buffer returning irregular block {}, position {}", first_irregular_block + irregular_block_num, pos_bytes - *block_offset);
        return BlockAndOffset(block, pos_bytes - *block_offset, first_irregular_block + irregular_block_num);
    }

    uint8_t* bytes_at(size_t pos_bytes, size_t required) {
        auto [block, pos, _] = block_and_offset(pos_bytes);
        if (!(pos + required <= block->bytes()))
            util::check(pos + required <= block->bytes(), "Block overflow, position {} is greater than block capacity {}", pos, block->bytes());
        return &(*block)[pos];
    }

    const uint8_t* bytes_at(size_t pos_bytes, size_t required) const {
        return const_cast<ChunkedBufferImpl *>(this)->bytes_at(pos_bytes, required);
    }

    bool bytes_within_one_block(size_t pos_bytes, size_t required) const {
        auto [block, pos, _] = block_and_offset(pos_bytes);
        return pos + required <= block->bytes();
    }

    uint8_t &operator[](size_t pos_bytes) {
        auto [block, pos, _] = block_and_offset(pos_bytes);
        if (!(pos < block->bytes()))
            util::check(pos < block->bytes(), "Block overflow, position {} is greater than block capacity {}", pos, block->bytes());
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
        if (blocks_.empty()) {
            return nullptr;
        }
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(blocks_.size() == 1,
                                                        "Taking a pointer to the beginning of a non-contiguous buffer");
        blocks_[0]->magic_.check();
        return blocks_[0]->data();
    }

    [[nodiscard]] uint8_t* data() {
        return const_cast<uint8_t*>(const_cast<const ChunkedBufferImpl*>(this)->data());
    }

    void check_bytes(size_t pos_bytes, size_t required_bytes) const {
        if (pos_bytes + required_bytes > bytes()) {
            std::string err = fmt::format("Cursor overflow in chunked_buffer ptr_cast, cannot read {} bytes from a buffer of size {} with cursor "
                                          "at {}, as it would require {} bytes. ",
                                          required_bytes,
                                          bytes(),
                                          pos_bytes,
                                          pos_bytes + required_bytes
                                          );
            ARCTICDB_DEBUG(log::storage(), err);
            throw std::invalid_argument(err);
        }
    }

    void memset_buffer(size_t offset, size_t bytes, char value) {
        auto [block, pos, block_index] = block_and_offset(offset);
        while(bytes > 0) {
            const auto size_to_write = block->bytes() - pos;
            memset(block->data() + pos, size_to_write, value);
            bytes -= size_to_write;
            if(bytes > 0) {
                ++block_index;
                if(block_index == blocks_.size())
                    return;

                block = blocks_[block_index];
                pos = 0;
            }
        }
    }

    template<typename T>
    T *ptr_cast(size_t pos_bytes, size_t required_bytes) {
        // TODO: This check doesn't verify we're overreaching outside of block boundaries.
        // We should instead use `bytes_at` which does the correct check like so:
        // return reinterpret_cast<T *>(bytes_at(pos_bytes, required_bytes))
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
        blocks_.emplace_back(create_regular_block(capacity, offset));
    }

    void add_external_block(const uint8_t* data, size_t size, size_t offset) {
        if (!no_blocks() && last_block().empty())
            free_last_block();

        auto [ptr, ts] = Allocator::aligned_alloc(sizeof(MemBlock));
        new(ptr) MemBlock(data, size, offset, ts, false);
        blocks_.emplace_back(reinterpret_cast<BlockType*>(ptr));
        bytes_ += size;
        // TODO: Check if this change is correct for other uses of this method
        // Also check if offset really needs to be a parameter or can always be inferred from block_offsets_
        if(block_offsets_.empty())
            block_offsets_.emplace_back(0);

        block_offsets_.emplace_back(last_offset() + size);
    }

    void add_detachable_block(size_t capacity, size_t offset) {
        if(capacity == 0)
            return;

        if (!no_blocks() && last_block().empty())
            free_last_block();

        blocks_.emplace_back(create_detachable_block(capacity, offset));
        if(block_offsets_.empty())
            block_offsets_.emplace_back(0);

        block_offsets_.emplace_back(last_offset() + capacity);
    }

    [[nodiscard]] bool empty() const { return bytes_ == 0; }

    void clear() {
        bytes_ = 0;
        for(auto block : blocks_)
            free_block(block);

        blocks_.clear();
        block_offsets_.clear();
    }

    [[nodiscard]] bool is_regular_sized() const { return block_offsets_.empty(); }

    [[nodiscard]] size_t bytes() const { return bytes_; }

    friend struct BufferView;

    BlockType &last_block() {
        util::check(!blocks_.empty(), "There should never be no blocks");
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

    // Note that with all the truncate_*_block methods, the bytes_ and offsets are no longer accurate after the methods
    // are called, but downstream logic uses these values to match up blocks with record batches, so this is deliberate
    void truncate_single_block(size_t start_offset, size_t end_offset) {
        // Inclusive of start_offset, exclusive of end_offset
        util::check(end_offset >= start_offset, "Truncate single block expects end ({}) >= start ({})", end_offset, start_offset);
        util::check(blocks_.size() == 1, "Truncate single block expects buffer with only one block");
        auto [block, offset, ts] = block_and_offset(start_offset);
        const auto removed_bytes = block->bytes() - (end_offset - start_offset);
        util::check(removed_bytes <= block->bytes(), "Can't truncate {} bytes from a {} byte block", removed_bytes, block->bytes());
        auto remaining_bytes = block->bytes() - removed_bytes;
        if (remaining_bytes > 0) {
            auto new_block = create_block(remaining_bytes, 0);
            new_block->copy_from(block->data() + start_offset, remaining_bytes, 0);
            blocks_[0] = new_block;
        } else {
            blocks_.clear();
            block_offsets_.clear();
        }
        block->abandon();
        delete block;
    }

    void truncate_first_block(size_t bytes) {
        util::check(blocks_.size() > 0, "Truncate first block expected at least one block");
        auto block = blocks_[0];
        util::check(block == *blocks_.begin(), "Truncate first block position {} not within initial block", bytes);
        // bytes is the number of bytes to remove, and is asserted to be in the first block of the buffer
        // An old bug in update caused us to store a larger end_index value in the index key than needed. Thus, if
        // date_range.start > table_data_key.last_ts && date_range.start < table_data_key.end_index we will load
        // the first data key even if it has no rows within the range. So, we allow clearing the entire first block
        // (i.e. bytes == block->bytes())
        util::check(bytes <= block->bytes(), "Can't truncate {} bytes from a {} byte block", bytes, block->bytes());
        auto remaining_bytes = block->bytes() - bytes;
        auto new_block = create_block(remaining_bytes, block->offset_);
        new_block->copy_from(block->data() + bytes, remaining_bytes, 0);
        blocks_[0] = new_block;
        block->abandon();
        delete block;
    }

    void truncate_last_block(size_t bytes) {
        // bytes is the number of bytes to remove, and is asserted to be in the last block of the buffer
        auto [block, offset, ts] = block_and_offset(bytes_ - bytes);
        util::check(block == *blocks_.rbegin(), "Truncate last block position {} not within last block", bytes);
        util::check(bytes < block->bytes(), "Can't truncate {} bytes from a {} byte block", bytes, block->bytes());
        auto remaining_bytes = block->bytes() - bytes;
        auto new_block = create_block(remaining_bytes, block->offset_);
        new_block->copy_from(block->data(), remaining_bytes, 0);
        *blocks_.rbegin() = new_block;
        block->abandon();
        delete block;
    }

  private:
    MemBlock* create_block(size_t capacity, size_t offset) const {
        if(allocation_type_ == entity::AllocationType::DETACHABLE)
            return create_detachable_block(capacity, offset);
        else
            return create_regular_block(capacity, offset);
    }

    MemBlock* create_regular_block(size_t capacity, size_t offset) const {
        auto [ptr, ts] = Allocator::aligned_alloc(BlockType::alloc_size(capacity));
        new(ptr) MemBlock(capacity, offset, ts);
        return reinterpret_cast<BlockType*>(ptr);
    }

    MemBlock* create_detachable_block(size_t capacity, size_t offset) const {
        auto [ptr, ts] = Allocator::aligned_alloc(sizeof(MemBlock));
        auto* data = allocate_detachable_memory(capacity);
        new(ptr) MemBlock(data, capacity, offset, ts, true);
        return reinterpret_cast<BlockType*>(ptr);
    }

    void free_block(BlockType* block) const {
        ARCTICDB_TRACE(log::storage(), "Freeing block at address {:x}", uintptr_t(block));
        block->magic_.check();
        auto timestamp = block->timestamp_;
        block->~MemBlock();
        Allocator::free(std::make_pair(reinterpret_cast<uint8_t *>(block), timestamp));
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

    [[nodiscard]] const BlockType &last_block() const {
        util::check(!blocks_.empty(), "There should never be no blocks");
        return **blocks_.rbegin();
    }

    [[nodiscard]] bool no_blocks() const { return blocks_.empty(); }

    size_t bytes_ = 0;
    size_t regular_sized_until_ = 0;
#ifndef DEBUG_BUILD
    boost::container::small_vector<BlockType *, 1> blocks_;
    boost::container::small_vector<size_t, 1> block_offsets_;
#else
    std::vector<BlockType*> blocks_;
    std::vector<size_t> block_offsets_;
#endif
    entity::AllocationType allocation_type_ = entity::AllocationType::DYNAMIC;
};

constexpr size_t PageSize = 4096;
constexpr size_t BufferSize = MemBlock::raw_size(PageSize);
using ChunkedBuffer = ChunkedBufferImpl<BufferSize>;

template <size_t BlockSize>
std::vector<ChunkedBufferImpl<BlockSize>> split(const ChunkedBufferImpl<BlockSize>& input, size_t nbytes);

template <size_t BlockSize>
ChunkedBufferImpl<BlockSize> truncate(const ChunkedBufferImpl<BlockSize>& input, size_t start_byte, size_t end_byte);

inline void hash_buffer(const ChunkedBuffer& buffer, HashAccum& accum) {
    for(const auto& block : buffer.blocks()) {
        accum(block->data(), block->bytes());
    }
}
}
