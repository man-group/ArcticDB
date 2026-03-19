/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column_data_random_accessor.hpp>

namespace arcticdb {

// Not handling regular sized until case, only skips one if statement in block_and_offset, cannot avoid branching
// completely
enum class BlockStructure { SINGLE, REGULAR, IRREGULAR };

template<typename TDT>
struct IChunkedBufferRandomAccessor {
    template<class Base>
    struct Interface : Base {
        typename TDT::DataTypeTag::raw_type at(size_t idx) const { return folly::poly_call<0>(*this, idx); };
    };

    template<class T>
    using Members = folly::PolyMembers<&T::at>;
};

template<typename TDT>
using ChunkedBufferRandomAccessor = folly::Poly<IChunkedBufferRandomAccessor<TDT>>;

template<typename TDT>
class ChunkedBufferSingleBlockAccessor {
    using RawType = typename TDT::DataTypeTag::raw_type;

  public:
    ChunkedBufferSingleBlockAccessor(ColumnData* parent) {
        auto typed_block = parent->next<TDT>();
        // Cache the base pointer of the block, as typed_block->data() has an if statement for internal vs external
        base_ptr_ = reinterpret_cast<const RawType*>(typed_block->data());
    }

    RawType at(size_t idx) const { return *(base_ptr_ + idx); }

  private:
    const RawType* base_ptr_;
};

template<typename TDT>
class ChunkedBufferRegularBlocksAccessor {
    using RawType = typename TDT::DataTypeTag::raw_type;

  public:
    ChunkedBufferRegularBlocksAccessor(ColumnData* parent) {
        // Cache the base pointers of each block, as typed_block->data() has an if statement for internal vs external
        base_ptrs_.reserve(parent->num_blocks());
        while (auto typed_block = parent->next<TDT>()) {
            base_ptrs_.emplace_back(reinterpret_cast<const RawType*>(typed_block->data()));
        }
    }

    RawType at(size_t idx) const {
        // quot is the block index, rem is the offset within the block
        auto div = std::div(static_cast<long long>(idx), values_per_block_);
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                div.quot < static_cast<long long>(base_ptrs_.size()),
                "ColumnData::at called with out of bounds index"
        );
        return *(base_ptrs_[div.quot] + div.rem);
    }

  private:
    static constexpr auto values_per_block_ = BufferSize / sizeof(RawType);
    std::vector<const RawType*> base_ptrs_;
};

template<typename TDT>
class ChunkedBufferIrregularBlocksAccessor {
    using RawType = typename TDT::DataTypeTag::raw_type;

  public:
    ChunkedBufferIrregularBlocksAccessor(ColumnData* parent) : parent_(parent) {}

    RawType at(size_t idx) const {
        auto pos_bytes = idx * sizeof(RawType);
        auto block_and_offset = parent_->buffer().block_and_offset(pos_bytes);
        auto ptr = block_and_offset.block_->data();
        ptr += block_and_offset.offset_;
        return *reinterpret_cast<const RawType*>(ptr);
    }

  private:
    ColumnData* parent_;
};

template<typename TDT, BlockStructure block_structure>
class ColumnDataRandomAccessorSparse {
    using RawType = typename TDT::DataTypeTag::raw_type;

  public:
    ColumnDataRandomAccessorSparse(ColumnData* parent) : parent_(parent) {
        parent_->bit_vector()->build_rs_index(&(bit_index_));
        if constexpr (block_structure == BlockStructure::SINGLE) {
            chunked_buffer_random_accessor_ = ChunkedBufferSingleBlockAccessor<TDT>(parent);
        } else if constexpr (block_structure == BlockStructure::REGULAR) {
            chunked_buffer_random_accessor_ = ChunkedBufferRegularBlocksAccessor<TDT>(parent);
        } else {
            // Irregular
            chunked_buffer_random_accessor_ = ChunkedBufferIrregularBlocksAccessor<TDT>(parent);
        }
    }
    RawType at(size_t idx) const {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                parent_->bit_vector(),
                "ColumnData::at called with sparse true, but bit_vector_ == nullptr"
        );
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                parent_->bit_vector()->size() > idx,
                "ColumnData::at called with sparse true, but index is out of range"
        );
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                parent_->bit_vector()->get_bit(idx),
                "ColumnData::at called with sparse true, but selected bit is false"
        );
        // This is the same as using rank_corrected, but we always require the idx bit to be true, so do the -1
        // ourselves for efficiency
        auto physical_offset = parent_->bit_vector()->rank(idx, bit_index_) - 1;
        return chunked_buffer_random_accessor_.at(physical_offset);
    }

  private:
    ChunkedBufferRandomAccessor<TDT> chunked_buffer_random_accessor_;
    ColumnData* parent_;
    util::BitIndex bit_index_;
};

template<typename TDT, BlockStructure block_structure>
class ColumnDataRandomAccessorDense {
    using RawType = typename TDT::DataTypeTag::raw_type;

  public:
    ColumnDataRandomAccessorDense(ColumnData* parent) {
        if constexpr (block_structure == BlockStructure::SINGLE) {
            chunked_buffer_random_accessor_ = ChunkedBufferSingleBlockAccessor<TDT>(parent);
        } else if constexpr (block_structure == BlockStructure::REGULAR) {
            chunked_buffer_random_accessor_ = ChunkedBufferRegularBlocksAccessor<TDT>(parent);
        } else {
            // Irregular
            chunked_buffer_random_accessor_ = ChunkedBufferIrregularBlocksAccessor<TDT>(parent);
        }
    }
    RawType at(size_t idx) const { return chunked_buffer_random_accessor_.at(idx); }

  private:
    ChunkedBufferRandomAccessor<TDT> chunked_buffer_random_accessor_;
};

template<typename TDT>
requires(util::instantiation_of<TDT, TypeDescriptorTag> && (TDT::dimension() == Dimension::Dim0))
ColumnDataRandomAccessor<TDT> random_accessor(ColumnData* parent) {
    bool sparse = parent->bit_vector() != nullptr;
    if (parent->buffer().num_blocks() == 1) {
        if (sparse) {
            return ColumnDataRandomAccessorSparse<TDT, BlockStructure::SINGLE>(parent);
        } else {
            return ColumnDataRandomAccessorDense<TDT, BlockStructure::SINGLE>(parent);
        }
    } else if (parent->buffer().is_regular_sized()) {
        if (sparse) {
            return ColumnDataRandomAccessorSparse<TDT, BlockStructure::REGULAR>(parent);
        } else {
            return ColumnDataRandomAccessorDense<TDT, BlockStructure::REGULAR>(parent);
        }
    } else {
        if (sparse) {
            return ColumnDataRandomAccessorSparse<TDT, BlockStructure::IRREGULAR>(parent);
        } else {
            return ColumnDataRandomAccessorDense<TDT, BlockStructure::IRREGULAR>(parent);
        }
    }
}

#define INSTANTIATE_RANDOM_ACCESSORS(__T__)                                                                            \
    template class ColumnDataRandomAccessorSparse<                                                                     \
            TypeDescriptorTag<DataTypeTag<DataType::__T__>, DimensionTag<Dimension::Dim0>>,                            \
            BlockStructure::SINGLE>;                                                                                   \
    template class ColumnDataRandomAccessorSparse<                                                                     \
            TypeDescriptorTag<DataTypeTag<DataType::__T__>, DimensionTag<Dimension::Dim0>>,                            \
            BlockStructure::REGULAR>;                                                                                  \
    template class ColumnDataRandomAccessorSparse<                                                                     \
            TypeDescriptorTag<DataTypeTag<DataType::__T__>, DimensionTag<Dimension::Dim0>>,                            \
            BlockStructure::IRREGULAR>;                                                                                \
    template class ColumnDataRandomAccessorDense<                                                                      \
            TypeDescriptorTag<DataTypeTag<DataType::__T__>, DimensionTag<Dimension::Dim0>>,                            \
            BlockStructure::SINGLE>;                                                                                   \
    template class ColumnDataRandomAccessorDense<                                                                      \
            TypeDescriptorTag<DataTypeTag<DataType::__T__>, DimensionTag<Dimension::Dim0>>,                            \
            BlockStructure::REGULAR>;                                                                                  \
    template class ColumnDataRandomAccessorDense<                                                                      \
            TypeDescriptorTag<DataTypeTag<DataType::__T__>, DimensionTag<Dimension::Dim0>>,                            \
            BlockStructure::IRREGULAR>;                                                                                \
    template ColumnDataRandomAccessor<TypeDescriptorTag<DataTypeTag<DataType::__T__>, DimensionTag<Dimension::Dim0>>>  \
    random_accessor(ColumnData* parent);

INSTANTIATE_RANDOM_ACCESSORS(UINT8)
INSTANTIATE_RANDOM_ACCESSORS(UINT16)
INSTANTIATE_RANDOM_ACCESSORS(UINT32)
INSTANTIATE_RANDOM_ACCESSORS(UINT64)
INSTANTIATE_RANDOM_ACCESSORS(INT8)
INSTANTIATE_RANDOM_ACCESSORS(INT16)
INSTANTIATE_RANDOM_ACCESSORS(INT32)
INSTANTIATE_RANDOM_ACCESSORS(INT64)
INSTANTIATE_RANDOM_ACCESSORS(FLOAT32)
INSTANTIATE_RANDOM_ACCESSORS(FLOAT64)
INSTANTIATE_RANDOM_ACCESSORS(BOOL8)
INSTANTIATE_RANDOM_ACCESSORS(NANOSECONDS_UTC64)
INSTANTIATE_RANDOM_ACCESSORS(ASCII_FIXED64)
INSTANTIATE_RANDOM_ACCESSORS(ASCII_DYNAMIC64)
INSTANTIATE_RANDOM_ACCESSORS(UTF_FIXED64)
INSTANTIATE_RANDOM_ACCESSORS(UTF_DYNAMIC64)
INSTANTIATE_RANDOM_ACCESSORS(EMPTYVAL)
INSTANTIATE_RANDOM_ACCESSORS(BOOL_OBJECT8)
INSTANTIATE_RANDOM_ACCESSORS(UTF_DYNAMIC32)

#undef INSTANTIATE_RANDOM_ACCESSORS

} // namespace arcticdb