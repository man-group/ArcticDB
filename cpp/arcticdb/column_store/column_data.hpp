/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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

struct ColumnData {
    /*
     * ColumnData is just a thin wrapper that helps in iteration over all the blocks in the column
     */
  public:
    template<typename RawType>
    struct Enumeration {
        ssize_t idx_{0};
        RawType* ptr_{nullptr};

        [[nodiscard]] inline ssize_t idx() const { return idx_; }

        inline RawType& value() {
            ARCTICDB_DEBUG_CHECK(
                    ErrorCode::E_ASSERTION_FAILURE,
                    ptr_ != nullptr,
                    "Dereferencing nullptr in enumerating ColumnDataIterator"
            );
            return *ptr_;
        };

        inline const RawType& value() const {
            ARCTICDB_DEBUG_CHECK(
                    ErrorCode::E_ASSERTION_FAILURE,
                    ptr_ != nullptr,
                    "Dereferencing nullptr in enumerating ColumnDataIterator"
            );
            return *ptr_;
        };
    };

    // Wrapper to keep code in iterator clean when differentiating between regular and enumerating iterators
    template<typename RawType>
    struct PointerWrapper {
        RawType* ptr_{nullptr};
    };

    template<class T, IteratorType iterator_type>
    using IteratorValueType_t =
            typename std::conditional_t<iterator_type == IteratorType::ENUMERATED, Enumeration<T>, PointerWrapper<T>>;

    template<class T, IteratorType iterator_type, bool constant>
    using IteratorReferenceType_t = typename std::conditional_t<
            iterator_type == IteratorType::ENUMERATED,
            std::conditional_t<constant, const Enumeration<T>, Enumeration<T>>,
            std::conditional_t<constant, const T, T>>;

    template<typename TDT, IteratorType iterator_type, IteratorDensity iterator_density, bool constant>
    class ColumnDataIterator;

    template<typename TDT, IteratorType iterator_type, IteratorDensity iterator_density, bool constant>
    using base_iterator_type = boost::iterator_facade<
            ColumnDataIterator<TDT, iterator_type, iterator_density, constant>,
            IteratorValueType_t<typename TDT::DataTypeTag::raw_type, iterator_type>, boost::forward_traversal_tag,
            IteratorReferenceType_t<typename TDT::DataTypeTag::raw_type, iterator_type, constant>&>;

    template<typename TDT, IteratorType iterator_type, IteratorDensity iterator_density, bool constant>
    class ColumnDataIterator : public base_iterator_type<TDT, iterator_type, iterator_density, constant> {
        using base_type = base_iterator_type<TDT, iterator_type, iterator_density, constant>;
        using RawType = typename TDT::DataTypeTag::raw_type;

      public:
        ColumnDataIterator() = delete;

        // Used to construct [c]begin iterators
        explicit ColumnDataIterator(const ColumnData* parent) : parent_(parent), block_pos_(0) {
            load_current_block();
            if constexpr (iterator_type == IteratorType::ENUMERATED && iterator_density == IteratorDensity::SPARSE) {
                // idx_ default-constructs to 0, which is correct for dense case
                data_.idx_ = parent_->bit_vector()->get_first();
            }
        }

        // Used to construct [c]end iterators
        explicit ColumnDataIterator(const ColumnData* parent, RawType* end_ptr) :
            parent_(parent),
            block_pos_(parent->num_blocks()) {
            data_.ptr_ = end_ptr;
        }

        // Construct an iterator pointing at element `in_block_offset` of block `block_pos`.
        // Does not support ENUMERATED SPARSE iterators.
        // TODO: Support enumerated sparse iterators. This will require efficiently caching an rs_index.
        ColumnDataIterator(const ColumnData* parent, size_t block_pos, size_t in_block_offset) :
            parent_(parent),
            block_pos_(block_pos) {
            load_current_block();
            remaining_values_in_block_ -= in_block_offset;
            data_.ptr_ += in_block_offset;
            if constexpr (iterator_type == IteratorType::ENUMERATED) {
                if constexpr (iterator_density == IteratorDensity::SPARSE) {
                    util::raise_rte("ColumnDataIterator at-position constructor not supported for SPARSE iteration");
                } else {
                    const size_t block_start_idx = parent_->buffer().block_byte_offset(block_pos) / sizeof(RawType);
                    data_.idx_ = static_cast<ssize_t>(block_start_idx + in_block_offset);
                }
            }
        }

        template<bool OtherConst>
        explicit ColumnDataIterator(const ColumnDataIterator<TDT, iterator_type, iterator_density, OtherConst>& other) :
            parent_(other.parent_),
            block_pos_(other.block_pos_),
            opt_block_(other.opt_block_),
            remaining_values_in_block_(other.remaining_values_in_block_),
            data_(other.data_) {}

        // Minimal accessors used by the search algorithms in column_algorithms.hpp.
        [[nodiscard]] const ColumnData* parent() const { return parent_; }
        [[nodiscard]] const std::optional<TypedBlockData<TDT>>& current_block() const { return opt_block_; }
        [[nodiscard]] const RawType* current_ptr() const { return data_.ptr_; }
        // Index of the block this iterator currently points into. For end iterators this is num_blocks.
        [[nodiscard]] size_t current_block_index() const { return block_pos_; }
        // TODO: Might be better to refactor storing the state as block_pos_ + in_block_offset_
        // Maybe not because current impl has a very efficient == which just compares a single pair of pointers
        // Although that has downsides as well (how do you iterate a column with external blocks pointing to the same
        // memory?!?)
        [[nodiscard]] size_t current_in_block_offset() const {
            if (remaining_values_in_block_ == 0) {
                // True only for end_ptrs
                return 0;
            }
            return data_.ptr_ - opt_block_->data();
        }

      private:
        friend class boost::iterator_core_access;

        void increment() {
            ++data_.ptr_;
            if (ARCTICDB_UNLIKELY(--remaining_values_in_block_ == 0)) {
                advance_block();
            }
            if constexpr (iterator_type == IteratorType::ENUMERATED) {
                if constexpr (iterator_density == IteratorDensity::SPARSE) {
                    data_.idx_ = parent_->bit_vector()->get_next(data_.idx_);
                } else {
                    ++data_.idx_;
                }
            }
        }

        void load_current_block() {
            opt_block_ = parent_->template typed_block_at_position<TDT>(block_pos_);
            if (ARCTICDB_LIKELY(opt_block_.has_value())) {
                remaining_values_in_block_ = opt_block_->row_count();
                data_.ptr_ = const_cast<typename TDT::DataTypeTag::raw_type*>(opt_block_->data());
            }
        }

        void advance_block() {
            ++block_pos_;
            load_current_block();
        }

        template<bool OtherConst>
        bool equal(const ColumnDataIterator<TDT, iterator_type, iterator_density, OtherConst>& other) const {
            ARCTICDB_DEBUG_CHECK(
                    ErrorCode::E_ASSERTION_FAILURE,
                    parent_ == other.parent_,
                    "ColumnDataIterator::equal called with different parent ColumnData*"
            );
            return data_.ptr_ == other.data_.ptr_;
        }

        typename base_type::reference dereference() const
        requires constant
        {
            if constexpr (iterator_type == IteratorType::ENUMERATED) {
                return data_;
            } else {
                ARCTICDB_DEBUG_CHECK(
                        ErrorCode::E_ASSERTION_FAILURE,
                        data_.ptr_ != nullptr,
                        "Dereferencing nullptr in ColumnDataIterator"
                );
                return *data_.ptr_;
            }
        }

        typename base_type::reference dereference() const
        requires(not constant)
        {
            if constexpr (iterator_type == IteratorType::ENUMERATED) {
                return *const_cast<typename base_type::value_type*>(&data_);
            } else {
                ARCTICDB_DEBUG_CHECK(
                        ErrorCode::E_ASSERTION_FAILURE,
                        data_.ptr_ != nullptr,
                        "Dereferencing nullptr in ColumnDataIterator"
                );
                return *data_.ptr_;
            }
        }

        const ColumnData* parent_{nullptr};
        size_t block_pos_{0};
        std::optional<TypedBlockData<TDT>> opt_block_{std::nullopt};
        std::size_t remaining_values_in_block_{0};
        typename base_type::value_type data_;
    };

    ColumnData(
            const ChunkedBuffer* data, const Buffer* shapes, const TypeDescriptor& type,
            const util::BitMagic* bit_vector
    ) :
        data_(data),
        shapes_(shapes),
        pos_(0),
        shape_pos_(0),
        type_(type),
        bit_vector_(bit_vector) {}

    ColumnData(const ChunkedBuffer* data, const TypeDescriptor& type) :
        data_(data),
        shapes_(nullptr),
        pos_(0),
        shape_pos_(0),
        type_(type),
        bit_vector_(nullptr) {}

    ARCTICDB_MOVE_COPY_DEFAULT(ColumnData)

    template<
            typename TDT, IteratorType iterator_type = IteratorType::REGULAR,
            IteratorDensity iterator_density = IteratorDensity::DENSE>
    ColumnDataIterator<TDT, iterator_type, iterator_density, false> begin() {
        return ColumnDataIterator<TDT, iterator_type, iterator_density, false>(this);
    }

    template<
            typename TDT, IteratorType iterator_type = IteratorType::REGULAR,
            IteratorDensity iterator_density = IteratorDensity::DENSE>
    ColumnDataIterator<TDT, iterator_type, iterator_density, true> cbegin() const {
        return ColumnDataIterator<TDT, iterator_type, iterator_density, true>(this);
    }

    template<
            typename TDT, IteratorType iterator_type = IteratorType::REGULAR,
            IteratorDensity iterator_density = IteratorDensity::DENSE>
    ColumnDataIterator<TDT, iterator_type, iterator_density, false> end() {
        using RawType = typename TDT::DataTypeTag::raw_type;
        RawType* end_ptr{nullptr};
        if (!data_->blocks().empty()) {
            auto block = data_->blocks().at(num_blocks() - 1);
            auto typed_block_data = make_typed_block<TDT>(block);
            end_ptr = const_cast<RawType*>(typed_block_data.data() + typed_block_data.row_count());
        }
        return ColumnDataIterator<TDT, iterator_type, iterator_density, false>(this, end_ptr);
    }

    template<
            typename TDT, IteratorType iterator_type = IteratorType::REGULAR,
            IteratorDensity iterator_density = IteratorDensity::DENSE>
    ColumnDataIterator<TDT, iterator_type, iterator_density, true> cend() const {
        using RawType = typename TDT::DataTypeTag::raw_type;
        RawType* end_ptr{nullptr};
        if (!data_->blocks().empty()) {
            auto block = data_->blocks().at(num_blocks() - 1);
            auto typed_block_data = make_typed_block<TDT>(block);
            end_ptr = const_cast<RawType*>(typed_block_data.data() + typed_block_data.row_count());
        }
        return ColumnDataIterator<TDT, iterator_type, iterator_density, true>(this, end_ptr);
    }

    // Returns a non-const DENSE iterator pointing at element `idx` of the column.
    // O(1) for single/regular blocks and O(log B) for irregular blocks.
    // TODO: add a separate sparse_iterator_at(size_t idx, OnMissing{BEFORE or AFTER}) -
    // will requre logic to cache rs_index.
    template<typename TDT, IteratorType iterator_type = IteratorType::REGULAR>
    ColumnDataIterator<TDT, iterator_type, IteratorDensity::DENSE, false> iterator_at(size_t idx) {
        using RawType = typename TDT::DataTypeTag::raw_type;
        const size_t total_rows = data_->bytes() / sizeof(RawType);
        util::check(
                idx < total_rows,
                "ColumnData::iterator_at: idx {} out of range for column with {} rows",
                idx,
                total_rows
        );
        auto resolved = data_->block_and_offset(idx * sizeof(RawType));
        size_t in_block_elem = resolved.offset_ / sizeof(RawType);
        return ColumnDataIterator<TDT, iterator_type, IteratorDensity::DENSE, false>(
                this, resolved.block_index_, in_block_elem
        );
    }

    // Const variant of iterator_at.
    template<typename TDT, IteratorType iterator_type = IteratorType::REGULAR>
    ColumnDataIterator<TDT, iterator_type, IteratorDensity::DENSE, true> citerator_at(size_t idx) const {
        using RawType = typename TDT::DataTypeTag::raw_type;
        const size_t total_rows = data_->bytes() / sizeof(RawType);
        util::check(
                idx < total_rows,
                "ColumnData::citerator_at: idx {} out of range for column with {} rows",
                idx,
                total_rows
        );
        auto resolved = data_->block_and_offset(idx * sizeof(RawType));
        size_t in_block_elem = resolved.offset_ / sizeof(RawType);
        return ColumnDataIterator<TDT, iterator_type, IteratorDensity::DENSE, true>(
                this, resolved.block_index_, in_block_elem
        );
    }

    [[nodiscard]] TypeDescriptor type() const { return type_; }

    [[nodiscard]] const ChunkedBuffer& buffer() const { return *data_; }

    [[nodiscard]] const util::BitMagic* bit_vector() const { return bit_vector_; }

    ChunkedBuffer& buffer() { return *const_cast<ChunkedBuffer*>(data_); }

    shape_t next_shape() {
        auto shape = *shapes_->ptr_cast<shape_t>(shape_pos_, sizeof(shape_t));
        shape_pos_ += sizeof(shape_t);
        return shape;
    }

    [[nodiscard]] size_t num_blocks() const { return data_->blocks().size(); }

    void reset() {
        pos_ = 0;
        shape_pos_ = 0;
    }

    template<typename TDT>
    std::optional<TypedBlockData<TDT>> next() {
        IMemBlock* block = nullptr;
        do {
            if (pos_ == num_blocks())
                return std::nullopt;

            block = data_->blocks().at(pos_++);
        } while (!block);

        return next_typed_block<TDT>(block);
    }

    template<typename TDT>
    std::optional<TypedBlockData<TDT>> typed_block_at_position(size_t pos) const {
        if (pos == num_blocks())
            return std::nullopt;

        auto block = data_->blocks().at(pos);
        util::check(block != nullptr, "Null block at position {} in typed_block_at_position", pos);
        return make_typed_block<TDT>(block);
    }

    template<typename TDT>
    std::optional<TypedBlockData<TDT>> last() {
        if (data_->blocks().empty())
            return std::nullopt;

        pos_ = num_blocks() - 1;
        auto block = data_->blocks().at(pos_);
        return next_typed_block<TDT>(block);
    }

    template<typename TDT>
    static TypedBlockData<TDT> make_typed_block(IMemBlock* block) {
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

    /// @brief Get non-owning pointer to the shapes array for the column
    [[nodiscard]] const Buffer* shapes() const noexcept;

  private:
    template<typename TDT>
    TypedBlockData<TDT> next_typed_block(IMemBlock* block) {
        size_t num_elements = 0;
        const shape_t* shape_ptr = nullptr;

        constexpr auto dim = TDT::DimensionTag::value;
        if constexpr (dim == Dimension::Dim0) {
            auto logical_size = block->logical_size();
            num_elements = logical_size / get_type_size(type_.data_type());
        } else {
            util::check(!buffer().has_extra_bytes_per_block(), "Can't have `extra_bytes_per_block` when using dim > 0");
            if (shapes_->empty()) {
                num_elements = block->logical_size() / get_type_size(type_.data_type());
            } else {
                shape_ptr = shapes_->ptr_cast<shape_t>(shape_pos_, sizeof(shape_t));
                size_t size = 0;
                constexpr auto raw_type_sz = sizeof(typename TDT::DataTypeTag::raw_type);
                // Blocks can contain empty tensors (empty arrays/matrices). They don't hold any data, however, they
                // have assigned shapes. In case of an empty tensor the corresponding entries in the shapes array must
                // be 0. The rationale is that the shapes array describes how many elements are there in a tensor. This
                // way we can distinguish between [] (empty array) and None (no array at all). We assume that no block,
                // besides the first, can start with empty tensors. This means that a block is exhausted when two
                // conditions are satisfied:
                // i) The processed size becomes equal to the block size
                // ii) All zero-sized shapes are parsed (i.e. the shape of the current tensor is not 0)
                while (current_tensor_is_empty() || size < block->logical_size()) {
                    if constexpr (dim == Dimension::Dim1) {
                        size += next_shape() * raw_type_sz;
                    } else {
                        const shape_t row_count = next_shape();
                        const shape_t column_count = next_shape();
                        util::check(
                                row_count > 0 || (row_count == 0 && column_count == 0),
                                "Tensor column count must be zero when the row count is 0"
                        );
                        size += row_count * column_count * raw_type_sz;
                    }
                    ++num_elements;
                }
                util::check(
                        size == block->logical_size(),
                        "Element size vs block size overrun: {} > {}",
                        size,
                        block->logical_size()
                );
            }
        }

        return TypedBlockData<TDT>{
                reinterpret_cast<const typename TDT::DataTypeTag::raw_type*>(block->data()),
                shape_ptr,
                block->physical_bytes(),
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

} // namespace arcticdb
