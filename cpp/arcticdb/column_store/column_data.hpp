/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/typed_block_data.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>

namespace arcticdb {
using namespace arcticdb::entity;

class Column;

/// Lightweight non-owning view over raw column buffers for block iteration, codec encoding,
/// and element-level iteration. Construct via ColumnData::from_column() when starting from
/// a Column, or directly from raw buffers (e.g. FieldCollection::column_data()).
struct ColumnData {
  public:
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
        explicit ColumnDataIterator(const ColumnData* parent) : parent_(parent) {
            increment_block();
            if constexpr (iterator_type == IteratorType::ENUMERATED && iterator_density == IteratorDensity::SPARSE) {
                // idx_ default-constructs to 0, which is correct for dense case
                data_.idx_ = parent_->bit_vector()->get_first();
            }
        }

        // Used to construct [c]end iterators
        explicit ColumnDataIterator(const ColumnData* parent, RawType* end_ptr) : parent_(parent) {
            data_.ptr_ = end_ptr;
        }

        template<bool OtherConst>
        explicit ColumnDataIterator(const ColumnDataIterator<TDT, iterator_type, iterator_density, OtherConst>& other) :
            parent_(other.parent_),
            block_pos_(other.block_pos_),
            opt_block_(other.opt_block_),
            remaining_values_in_block_(other.remaining_values_in_block_),
            data_(other.data_) {}

      private:
        friend class boost::iterator_core_access;

        void increment() {
            ++data_.ptr_;
            if (ARCTICDB_UNLIKELY(--remaining_values_in_block_ == 0)) {
                increment_block();
            }
            if constexpr (iterator_type == IteratorType::ENUMERATED) {
                if constexpr (iterator_density == IteratorDensity::SPARSE) {
                    data_.idx_ = parent_->bit_vector()->get_next(data_.idx_);
                } else {
                    ++data_.idx_;
                }
            }
        }

        void increment_block() {
            opt_block_ = parent_->typed_block_at_position<TDT>(block_pos_++);
            if (ARCTICDB_LIKELY(opt_block_.has_value())) {
                remaining_values_in_block_ = opt_block_->row_count();
                data_.ptr_ = const_cast<typename TDT::DataTypeTag::raw_type*>(opt_block_->data());
            }
        }

        template<bool OtherConst>
        bool equal(const ColumnDataIterator<TDT, iterator_type, iterator_density, OtherConst>& other) const {
            debug::check<ErrorCode::E_ASSERTION_FAILURE>(
                    parent_ == other.parent_, "ColumnDataIterator::equal called with different parent ColumnData*"
            );
            return data_.ptr_ == other.data_.ptr_;
        }

        typename base_type::reference dereference() const
        requires constant
        {
            if constexpr (iterator_type == IteratorType::ENUMERATED) {
                return data_;
            } else {
                debug::check<ErrorCode::E_ASSERTION_FAILURE>(
                        data_.ptr_ != nullptr, "Dereferencing nullptr in ColumnDataIterator"
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
                debug::check<ErrorCode::E_ASSERTION_FAILURE>(
                        data_.ptr_ != nullptr, "Dereferencing nullptr in ColumnDataIterator"
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
            auto typed_block_data = arcticdb::make_typed_block<TDT>(block);
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
            auto typed_block_data = arcticdb::make_typed_block<TDT>(block);
            end_ptr = const_cast<RawType*>(typed_block_data.data() + typed_block_data.row_count());
        }
        return ColumnDataIterator<TDT, iterator_type, iterator_density, true>(this, end_ptr);
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
        return arcticdb::make_typed_block<TDT>(block);
    }

    template<typename TDT>
    std::optional<TypedBlockData<TDT>> last() {
        if (data_->blocks().empty())
            return std::nullopt;

        pos_ = num_blocks() - 1;
        auto block = data_->blocks().at(pos_);
        return next_typed_block<TDT>(block);
    }

    // Keep backward compatibility — delegates to the free function
    template<typename TDT>
    static TypedBlockData<TDT> make_typed_block(IMemBlock* block) {
        return arcticdb::make_typed_block<TDT>(block);
    }

    /// @brief Get non-owning pointer to the shapes array for the column
    [[nodiscard]] const Buffer* shapes() const noexcept;

    /// Construct a ColumnData view from a Column's public interface
    static ColumnData from_column(const Column& col);

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

    [[nodiscard]] bool current_tensor_is_empty() const;

    const ChunkedBuffer* data_;
    const Buffer* shapes_;
    size_t pos_;
    size_t shape_pos_;
    TypeDescriptor type_;
    const util::BitMagic* bit_vector_;
};

} // namespace arcticdb
