/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/block.hpp>

#include <boost/iterator/iterator_facade.hpp>

namespace arcticdb {
using namespace arcticdb::entity;

template<typename TDT>
struct TypedBlockData {
    template<class ValueType>
    class TypedColumnBlockIterator
        : public boost::iterator_facade<
                  TypedColumnBlockIterator<ValueType>, ValueType, boost::random_access_traversal_tag> {
      public:
        explicit TypedColumnBlockIterator(ValueType* ptr) : ptr_(ptr) {}

        TypedColumnBlockIterator(const TypedColumnBlockIterator& other) : ptr_(other.ptr_) {}

        template<class OtherValue>
        explicit TypedColumnBlockIterator(const TypedColumnBlockIterator<OtherValue>& other) : ptr_(other.ptr_) {}

        TypedColumnBlockIterator() : ptr_(nullptr) {}

        TypedColumnBlockIterator& operator=(const TypedColumnBlockIterator& other) {
            if (&other != this)
                ptr_ = other.ptr_;

            return *this;
        }

        template<class OtherValue>
        bool equal(const TypedColumnBlockIterator<OtherValue>& other) const {
            return ptr_ == other.ptr_;
        }

        ssize_t distance_to(const TypedColumnBlockIterator& other) const { return other.ptr_ - ptr_; }

        void increment() { ++ptr_; }

        void decrement() { --ptr_; }

        void advance(ptrdiff_t n) { ptr_ += n; }

        ValueType& dereference() const { return *ptr_; }

        ValueType* ptr_;
    };

    using TypeDescriptorTag = TDT;
    using raw_type = typename TDT::DataTypeTag::raw_type;

    ARCTICDB_MOVE_COPY_DEFAULT(TypedBlockData)

    TypedBlockData(
            const raw_type* data, const shape_t* shapes, size_t nbytes, size_t row_count, const IMemBlock* block
    ) :
        data_(data),
        shapes_(shapes),
        nbytes_(nbytes),
        row_count_(row_count),
        block_(block) {}

    TypedBlockData(size_t nbytes, const shape_t* shapes) :
        data_(nullptr),
        shapes_(shapes),
        nbytes_(nbytes),
        row_count_(1u),
        block_(nullptr) {}

    [[nodiscard]] std::size_t nbytes() const { return nbytes_; }

    [[nodiscard]] raw_type* release() { return reinterpret_cast<raw_type*>(const_cast<IMemBlock*>(block_)->release()); }

    [[nodiscard]] std::size_t row_count() const { return row_count_; }

    [[nodiscard]] TypeDescriptor type() const { return static_cast<TypeDescriptor>(TDT()); }

    [[nodiscard]] const shape_t* shapes() const { return shapes_; }

    [[nodiscard]] const raw_type* data() const { return data_; }

    [[nodiscard]] const IMemBlock* mem_block() const { return block_; }

    raw_type operator[](size_t pos) const { return reinterpret_cast<const raw_type*>(block_->data())[pos]; }

    auto begin() const { return TypedColumnBlockIterator<const raw_type>(data_); }

    auto end() const { return TypedColumnBlockIterator<const raw_type>(data_ + row_count_); }

    [[nodiscard]] size_t offset() const { return block_->offset(); }

    friend bool operator==(const TypedBlockData& left, const TypedBlockData& right) {
        return left.block_ == right.block_;
    }

  private:
    const raw_type* data_;
    const shape_t* shapes_;
    size_t nbytes_;
    size_t row_count_;
    const IMemBlock* block_; // pointer to the parent memblock from which this was created from.
};

enum class IteratorType { REGULAR, ENUMERATED };

enum class IteratorDensity { DENSE, SPARSE };

template<typename RawType>
struct Enumeration {
    ssize_t idx_{0};
    RawType* ptr_{nullptr};

    [[nodiscard]] inline ssize_t idx() const { return idx_; }

    inline RawType& value() {
        debug::check<ErrorCode::E_ASSERTION_FAILURE>(
                ptr_ != nullptr, "Dereferencing nullptr in enumerating ColumnDataIterator"
        );
        return *ptr_;
    };

    inline const RawType& value() const {
        debug::check<ErrorCode::E_ASSERTION_FAILURE>(
                ptr_ != nullptr, "Dereferencing nullptr in enumerating ColumnDataIterator"
        );
        return *ptr_;
    };
};

template<typename RawType>
struct PointerWrapper {
    RawType* ptr_{nullptr};
};

template<class T, IteratorType iterator_type>
using IteratorValueType_t =
        typename std::conditional_t<iterator_type == IteratorType::ENUMERATED, Enumeration<T>, PointerWrapper<T>>;

template<class T, IteratorType iterator_type, bool constant>
using IteratorReferenceType_t = typename std::conditional_t<
        iterator_type == IteratorType::ENUMERATED, std::conditional_t<constant, const Enumeration<T>, Enumeration<T>>,
        std::conditional_t<constant, const T, T>>;

/// Utility to create a TypedBlockData from a raw IMemBlock for Dim0 columns.
template<typename TDT>
TypedBlockData<TDT> make_typed_block(IMemBlock* block) {
    if constexpr (TDT::DimensionTag::value != Dimension::Dim0) {
        util::raise_rte("Random access block in multi-dimentional column");
    }

    return TypedBlockData<TDT>{
            reinterpret_cast<const typename TDT::DataTypeTag::raw_type*>(block->data()),
            nullptr,
            block->physical_bytes(),
            block->logical_size() / get_type_size(TDT::DataTypeTag::data_type),
            block
    };
}

} // namespace arcticdb
