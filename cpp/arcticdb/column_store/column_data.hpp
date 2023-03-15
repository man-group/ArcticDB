/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once


#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/block.hpp>

#include <boost/iterator/iterator_facade.hpp>

#include <cstdint>

// Provides a typed block iterator over the contents of a column
namespace arcticdb {
using namespace arcticdb::entity;

template<typename TDT>
struct TypedBlockData {
    /*
     * Thin layer that helps create an iterator over the elements of the block taking the size of data and
     * shape into account. Maybe a bit too much overkill given how thin this is?
     */
    template <class ValueType>
    class TypedColumnBlockIterator :  public boost::iterator_facade<TypedColumnBlockIterator<ValueType>, ValueType, boost::random_access_traversal_tag> {
      public:
        TypedColumnBlockIterator(ValueType* ptr)
            :  ptr_(ptr) { }

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

    using raw_type = typename TDT::DataTypeTag::raw_type;

    ARCTICDB_MOVE_COPY_DEFAULT(TypedBlockData)

    TypedBlockData(const raw_type *data, const shape_t *shapes, size_t nbytes, size_t row_count, MemBlock *block) :
        data_(data),
        shapes_(shapes),
        nbytes_(nbytes),
        row_count_(row_count),
        block_(block)
        {}


    TypedBlockData(size_t nbytes, shape_t* shapes) :
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
    ColumnData(
        const ChunkedBuffer*data,
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
    std::optional<TypedBlockData<TDT>>  next() {
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

  private:
    template<typename TDT>
    TypedBlockData<TDT> next_typed_block(MemBlock* block) {
        size_t num_elements = 0;
        const shape_t *shape_ptr = nullptr;

        constexpr auto dim = TDT::DimensionTag::value;
        if constexpr (dim == Dimension::Dim0){
            num_elements = block->bytes() / get_type_size(type_.data_type());
        }  else {
            if (shapes_->empty()) {
                num_elements = block->bytes() / get_type_size(type_.data_type());
            } else {
                shape_ptr = shapes_->ptr_cast<shape_t>(shape_pos_, sizeof(shape_t));
                size_t size = 0;
                constexpr auto raw_type_sz = sizeof(typename TDT::DataTypeTag::raw_type);
                while (size < block->bytes()) {
                    if constexpr (dim == Dimension::Dim1)
                        size += next_shape() * raw_type_sz;
                    else
                        size += next_shape() * next_shape() * raw_type_sz;
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

    const ChunkedBuffer* data_;
    const Buffer* shapes_;
    size_t pos_;
    size_t shape_pos_;
    TypeDescriptor type_;
    const util::BitMagic* bit_vector_;
};

}