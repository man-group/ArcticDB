/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/block.hpp>

#include <boost/iterator/iterator_facade.hpp>


namespace arcticdb {
using namespace arcticdb::entity;

template<typename TDT>
struct TypedBlockData {
    template <class ValueType>
    class TypedColumnBlockIterator :  public boost::iterator_facade<TypedColumnBlockIterator<ValueType>, ValueType, boost::random_access_traversal_tag> {
      public:
        TypedColumnBlockIterator(ValueType* ptr)
            :  ptr_(ptr) { }

        TypedColumnBlockIterator(const TypedColumnBlockIterator& other)
            : ptr_(other.ptr_) {}

        template <class OtherValue>
        explicit TypedColumnBlockIterator(const TypedColumnBlockIterator<OtherValue>& other)
            : ptr_(other.ptr_){}

        TypedColumnBlockIterator()
            :  ptr_(nullptr) { }

        TypedColumnBlockIterator& operator=(const TypedColumnBlockIterator& other) {
            ptr_ = other.ptr_;
            return *this;
        }

        template <class OtherValue>
        bool equal(const TypedColumnBlockIterator<OtherValue>& other) const {
            return ptr_ == other.ptr_;
        }

        ssize_t distance_to(const TypedColumnBlockIterator& other) const {
            return other.ptr_ - ptr_;
        }

        void increment(){
            ++ptr_;
        }

        void decrement(){
            --ptr_;
        }

        void advance(ptrdiff_t n){
            ptr_ += n;
        }

        ValueType& dereference() const {
            return *ptr_;
        }

        ValueType* ptr_;
    };

    using TypeDescriptorTag = TDT;
    using raw_type = typename TDT::DataTypeTag::raw_type;

    ARCTICDB_MOVE_COPY_DEFAULT(TypedBlockData)

    TypedBlockData(const raw_type *data, const shape_t *shapes, size_t nbytes, size_t row_count, const MemBlock *block) :
        data_(data),
        shapes_(shapes),
        nbytes_(nbytes),
        row_count_(row_count),
        block_(block)
        {}


    TypedBlockData(size_t nbytes, const shape_t* shapes) :
        data_(nullptr),
        shapes_(shapes),
        nbytes_(nbytes),
        row_count_(1u),
        block_(nullptr)
    {}

    std::size_t nbytes() const { return nbytes_; }
    std::size_t row_count() const { return row_count_; }
    TypeDescriptor type() const { return static_cast<TypeDescriptor>(TDT()); }
    const shape_t *shapes() const { return shapes_; }
    const raw_type *data() const { return data_; }
    const MemBlock *mem_block() const { return block_; }

    raw_type operator[](size_t pos) const {
        return reinterpret_cast<const raw_type*>(block_->data())[pos];
    }

    auto begin() const {
        return TypedColumnBlockIterator<const raw_type>(data_);
    }

    auto end() const {
        return TypedColumnBlockIterator<const raw_type>(data_ + row_count_);
    }

    size_t offset() const {
        return block_->offset_;
    }

    friend bool operator==(const TypedBlockData& left, const TypedBlockData& right) {
        return left.block_ == right.block_;
    }

private:
    const raw_type *data_;
    const shape_t *shapes_;
    size_t nbytes_;
    size_t row_count_;
    const MemBlock *block_;  // pointer to the parent memblock from which this was created from.
};

struct ColumnData {
/*
 * ColumnData is just a thin wrapper that helps in iteration over all the blocks in the column
 */

  public:
    template<typename TDT, bool constant>
    class ColumnDataIterator: public boost::iterator_facade<
            ColumnDataIterator<TDT, constant>,
            std::conditional_t<constant, typename TDT::DataTypeTag::raw_type const, typename TDT::DataTypeTag::raw_type>,
            boost::forward_traversal_tag
            > {
    using RawType = std::conditional_t<constant, const typename TDT::DataTypeTag::raw_type, typename TDT::DataTypeTag::raw_type>;
    public:
        ColumnDataIterator() = delete;

        // Used to construct [c]begin iterators
        explicit ColumnDataIterator(ColumnData* parent):
        parent_(parent)
        {
            increment_block();
        }

        // Used to construct [c]end iterators
        explicit ColumnDataIterator(ColumnData* parent, RawType* end_ptr):
                parent_(parent),
                ptr_(end_ptr) {}

        template <class OtherValue, bool OtherConst>
        ColumnDataIterator(ColumnDataIterator<OtherValue, OtherConst> const& other):
        parent_(other.parent_),
        opt_block_(other.opt_block_),
        remaining_values_in_block_(other.remaining_values_in_block_),
        ptr_(other.ptr_) {}
    private:
        friend class boost::iterator_core_access;
        template <class, bool> friend class ColumnDataIterator;

        void increment() {
            if (ARCTICDB_LIKELY(remaining_values_in_block_ > 0)) {
                ++ptr_;
                --remaining_values_in_block_;
            } else {
                increment_block();
            }
        }

        void increment_block() {
            opt_block_ = parent_->next<TDT>();
            if(ARCTICDB_LIKELY(opt_block_.has_value())) {
                remaining_values_in_block_ = opt_block_->row_count();
                if constexpr(constant) {
                    ptr_ = reinterpret_cast<RawType*>(opt_block_->data());
                } else {
                    ptr_ = const_cast<RawType*>(opt_block_->data());
                }
            }
        }

        template <typename OtherValue, bool OtherConst>
        bool equal(ColumnDataIterator<OtherValue, OtherConst> const& other) const {
            return parent_ == other.parent_ && ptr_ == other.ptr_;
        }

        RawType& dereference() const {
            return *ptr_;
        }

        ColumnData* parent_{nullptr};
        std::optional<TypedBlockData<TDT>> opt_block_{std::nullopt};
        std::size_t remaining_values_in_block_{0};
        RawType* ptr_{nullptr};
    };

    ColumnData(
        const ChunkedBuffer* data,
        const Buffer* shapes,
        const TypeDescriptor &type,
        const util::BitMagic* bit_vector) :
        data_(data),
        shapes_(shapes),
        pos_(0),
        shape_pos_(0),
        type_(type),
        bit_vector_(bit_vector){}

    ARCTICDB_MOVE_COPY_DEFAULT(ColumnData)

    template<typename TDT>
    ColumnDataIterator<TDT, false> begin() {
        return ColumnDataIterator<TDT, false>(this);
    }

    template<typename TDT>
    ColumnDataIterator<TDT, true> cbegin() {
        return ColumnDataIterator<TDT, true>(this);
    }

    template<typename TDT>
    ColumnDataIterator<TDT, false> end() {
        using RawType = typename TDT::DataTypeTag::raw_type;
        RawType* end_ptr{nullptr};
        if(!data_->blocks().empty()) {
            auto block = data_->blocks().at(num_blocks() - 1);
            auto typed_block_data = next_typed_block<TDT>(block);
            end_ptr = typed_block_data.data() + typed_block_data.row_count();
        }
        return ColumnDataIterator<TDT, false>(this, end_ptr);
    }

    template<typename TDT>
    ColumnDataIterator<TDT, true> cend() {
        using RawType = typename TDT::DataTypeTag::raw_type;
        const RawType* end_ptr{nullptr};
        if(!data_->blocks().empty()) {
            auto block = data_->blocks().at(num_blocks() - 1);
            auto typed_block_data = next_typed_block<TDT>(block);
            end_ptr = typed_block_data.data() + typed_block_data.row_count();
        }
        return ColumnDataIterator<TDT, true>(this, end_ptr);
    }

    TypeDescriptor type() const {
        return type_;
    }

    const ChunkedBuffer &buffer() const {
        return *data_;
    }

    const util::BitMagic* bit_vector() const {
        return bit_vector_;
    }

    ChunkedBuffer &buffer()  {
        return *const_cast<ChunkedBuffer*>(data_);
    }

    shape_t next_shape() {
        auto shape = *shapes_->ptr_cast<shape_t>(shape_pos_, sizeof(shape_t));
        shape_pos_ += sizeof(shape_t);
        return shape;
    }

    size_t num_blocks() const {
        return data_->blocks().size();
    }

    void reset() {
        pos_ = 0;
        shape_pos_ = 0;
    }

    template<typename TDT>
    std::optional<TypedBlockData<TDT>> next() {
        MemBlock* block = nullptr;
        do {
            if (pos_ == num_blocks())
                return std::nullopt;

             block = data_->blocks().at(pos_++);
        } while(!block);

        return next_typed_block<TDT>(block);
    }

    template<typename TDT>
    std::optional<TypedBlockData<TDT>>  last()  {
       if(data_->blocks().empty())
           return std::nullopt;

        pos_ = num_blocks() -1;
        auto block = data_->blocks().at(pos_);
        return next_typed_block<TDT>(block);
    }

    template<typename TDT>
    static TypedBlockData<TDT> make_typed_block(MemBlock* block) {
        if constexpr (TDT::DimensionTag::value != Dimension::Dim0) {
            util::raise_rte("Random access block in multi-dimentional column");
        }

        return TypedBlockData<TDT>{
            reinterpret_cast<const typename TDT::DataTypeTag::raw_type *>(block->data()),
            nullptr,
            block->bytes(),
            block->bytes() / get_type_size(TDT::DataTypeTag::data_type),
            block
        };
    }

    /// @brief Get non-owning pointer to the shapes array for the column
    [[nodiscard]] const Buffer* shapes() const noexcept;

  private:
    template<typename TDT>
    TypedBlockData<TDT> next_typed_block(MemBlock* block) {
        size_t num_elements = 0;
        const shape_t *shape_ptr = nullptr;

        constexpr auto dim = TDT::DimensionTag::value;
        if constexpr (dim == Dimension::Dim0) {
            num_elements = block->bytes() / get_type_size(type_.data_type());
        } else {
            if (shapes_->empty()) {
                num_elements = block->bytes() / get_type_size(type_.data_type());
            } else {
                shape_ptr = shapes_->ptr_cast<shape_t>(shape_pos_, sizeof(shape_t));
                size_t size = 0;
                constexpr auto raw_type_sz = sizeof(typename TDT::DataTypeTag::raw_type);
                // Blocks can contain empty tensors (empty arrays/matrices). They don't hold any data, however, they
                // have assigned shapes. In case of an empty tensor the corresponding entries in the shapes array must
                // be 0. The rationale is that the shapes array describes how many elements are there in a tensor. This
                // way we can distinguish between [] (empty array) and None (no array at all). We assume that no block,
                // besides the first, can start with empty tensors. This means that a block is exhausted when both are
                // conditions are satisfied:
                // i) The processed size becomes equal to the block size
                // ii) All zero-sized shapes are parsed (i.e. the shape of the current tensor is not 0)
                while (current_tensor_is_empty() || size < block->bytes()) {
                    if constexpr (dim == Dimension::Dim1) {
                        size += next_shape() * raw_type_sz;
                    } else {
                        const shape_t row_count = next_shape();
                        const shape_t column_count = next_shape();
                        util::check(
                            row_count > 0 || (row_count == 0 && column_count == 0),
                            "Tensor column count must be zero when the row count is 0");
                        size += row_count * column_count * raw_type_sz;
                    }
                    ++num_elements;
                }
                util::check(size == block->bytes(), "Element size vs block size overrun: {} > {}", size, block->bytes());
            }
        }

        return TypedBlockData<TDT>{
            reinterpret_cast<const typename TDT::DataTypeTag::raw_type *>(block->data()),
            shape_ptr,
            block->bytes(),
            num_elements,
            block
        };
    }

    /// @brief Check if the current tensor is of size 0
    /// @note The size of the current tensor is written in #shapes_ under #shapes_pos_
    [[nodiscard]] bool current_tensor_is_empty() const;

    const ChunkedBuffer* data_;
    const Buffer* shapes_;
    size_t pos_;
    size_t shape_pos_;
    TypeDescriptor type_;
    const util::BitMagic* bit_vector_;
};

}
