/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/column_data_random_accessor.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/statistics.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/concepts.hpp>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/util/flatten_utils.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/sparse_utils.hpp>
#include <arcticdb/util/type_traits.hpp>

// Compilation fails on Mac if cstdio is not included prior to folly/Function.h due to a missing definition of memalign
// in folly/Memory.h
#ifdef __APPLE__
#include <cstdio>
#endif
#include <concepts>
#include <numeric>
#include <optional>

namespace arcticdb {

using namespace arcticdb::entity;

struct JiveTable {
    explicit JiveTable(size_t num_rows) : orig_pos_(num_rows), sorted_pos_(num_rows) {}

    std::vector<uint32_t> orig_pos_;
    std::vector<uint32_t> sorted_pos_;
};

enum class ExtraBufferType : uint8_t { OFFSET, STRING, ARRAY, BITMAP };

// Specifies a way to index extra buffers.
// We can attach extra buffers to each offset and type. This is used for OutputFormat::ARROW to store the extra buffers
// required to construct a string buffer.
// For example if we have an arrow Column with 2 chunks containing the strings ["a", "bc"], ["ab", "ab"], we'd have:
// Column data in blocks:
// offset:0 -> [0, 1] (int32_t)
// offset:8 -> [0, 0] (int32_t)
// Extra buffers for the column:
// offset_bytes=0, type_=OFFSET -> [0, 1, 3] (int64_t)
// offset_bytes=8, type_=OFFSET -> [0, 2] (int64_t)
// offset_bytes=0, type_=STRING -> "abc"
// offset_bytes=8, type_=STRING -> "ab"
struct ExtraBufferIndex {
    size_t offset_bytes_;
    ExtraBufferType type_;
};

bool operator==(const ExtraBufferIndex& lhs, const ExtraBufferIndex& rhs);

struct ExtraBufferIndexHash {
    std::size_t operator()(const ExtraBufferIndex& index) const;
};

struct ExtraBufferContainer {
    mutable std::shared_mutex mutex_;
    std::unordered_map<ExtraBufferIndex, ChunkedBuffer, ExtraBufferIndexHash> buffers_;

    ChunkedBuffer& create_buffer(size_t offset, ExtraBufferType type, size_t size, AllocationType allocation_type);

    void set_buffer(size_t offset, ExtraBufferType type, ChunkedBuffer&& buffer);

    ChunkedBuffer& get_buffer(size_t offset, ExtraBufferType type) const;

    bool has_buffer(size_t offset, ExtraBufferType type) const;
};

template<typename T>
JiveTable create_jive_table(const Column& col);

void initialise_output_column(const Column& input_column, Column& output_column);

void initialise_output_column(const Column& left_input_column, const Column& right_input_column, Column& output_column);

void initialise_output_bitset(
        const Column& input_column, bool sparse_missing_value_output, util::BitSet& output_bitset
);

class Column {
  public:
    template<typename TDT, typename ValueType>
    class TypedColumnIterator
        : public boost::iterator_facade<
                  TypedColumnIterator<TDT, ValueType>, ValueType, boost::random_access_traversal_tag> {
        using RawType = std::decay_t<typename TDT::DataTypeTag::raw_type>;
        static constexpr size_t type_size = sizeof(RawType);

        ColumnData parent_;
        std::optional<TypedBlockData<TDT>> block_;
        typename TypedBlockData<TDT>::template TypedColumnBlockIterator<const ValueType> block_pos_;
        typename TypedBlockData<TDT>::template TypedColumnBlockIterator<const ValueType> block_end_;

        void set_block_range() {
            if (block_) {
                block_pos_ = std::begin(*block_);
                block_end_ = std::end(*block_);
            }
        }

        void set_next_block() {
            if (auto block = parent_.next<TDT>(); block)
                block_.emplace(std::move(*block));
            else
                block_ = std::nullopt;

            set_block_range();
        }

      public:
        TypedColumnIterator(const Column& col, bool begin) :
            parent_(col.data()),
            block_(begin ? parent_.next<TDT>() : std::nullopt) {
            if (begin)
                set_block_range();
        }

        template<class OtherValue>
        explicit TypedColumnIterator(const TypedColumnIterator<TDT, OtherValue>& other) :
            parent_(other.parent_),
            block_(other.block_),
            block_pos_(other.block_pos_),
            block_end_(other.block_end_) {}

        template<class OtherValue>
        bool equal(const TypedColumnIterator<TDT, OtherValue>& other) const {
            if (block_) {
                if (!other.block_)
                    return false;

                return *block_ == *other.block_ && block_pos_ == other.block_pos_;
            }
            return !other.block_;
        }

        ssize_t distance_to(const TypedColumnIterator& other) const { return other.get_offset() - get_offset(); }

        void increment() {
            ++block_pos_;
            if (block_pos_ == block_end_)
                set_next_block();
        }

        void decrement() {
            if (!block_) {
                block_ = parent_.last<TDT>();
                if (block_) {
                    block_pos_ = block_->begin();
                    std::advance(block_pos_, block_->row_count() - 1);
                }
            } else {
                --block_pos_;
                if (block_pos_ < block_->begin()) {
                    auto offset = block_->offset() - type_size;
                    set_offset(offset);
                }
            }
        }

        [[nodiscard]] ssize_t get_offset() const {
            if (!block_)
                return parent_.buffer().bytes() / type_size;

            const auto off = block_->offset();
            const auto dist = std::distance(std::begin(*block_), block_pos_);
            return off + dist;
        }

        void set_offset(size_t offset) {
            const auto bytes = offset * type_size;
            auto block_and_offset = parent_.buffer().block_and_offset(bytes);
            block_ = ColumnData::make_typed_block<TDT>(block_and_offset.block_);
            block_pos_ = std::begin(*block_);
            std::advance(block_pos_, block_and_offset.offset_ / type_size);
            block_end_ = std::end(block_.value());
        }

        void advance(ptrdiff_t n) {
            auto offset = get_offset();
            offset += n;
            set_offset(offset);
        }

        const ValueType& dereference() const { return *block_pos_; }
    };

    struct StringArrayData {
        ssize_t num_strings_;
        ssize_t string_size_;
        const char* data_;

        StringArrayData(ssize_t n, ssize_t s, const char* d) : num_strings_(n), string_size_(s), data_(d) {}
    };

    Column();

    Column(TypeDescriptor type);

    Column(TypeDescriptor type, Sparsity allow_sparse);

    Column(TypeDescriptor type, Sparsity allow_sparse, ChunkedBuffer&& buffer);

    Column(TypeDescriptor type, Sparsity allow_sparse, ChunkedBuffer&& buffer, Buffer&& shapes);

    Column(TypeDescriptor type, size_t expected_rows, AllocationType allocation_type, Sparsity allow_sparse,
           size_t extra_bytes_per_block = 0);

    ARCTICDB_MOVE_ONLY_DEFAULT(Column)

    friend bool operator==(const Column& left, const Column& right);
    friend bool operator!=(const Column& left, const Column& right);

    Column clone() const;

    bool empty() const;

    bool is_sparse() const;

    bool sparse_permitted() const;

    void set_statistics(FieldStatsImpl stats);

    bool has_statistics() const;

    FieldStatsImpl get_statistics() const;

    void backfill_sparse_map(ssize_t to_row);

    [[nodiscard]] util::BitMagic& sparse_map();
    [[nodiscard]] const util::BitMagic& sparse_map() const;
    [[nodiscard]] std::optional<util::BitMagic>& opt_sparse_map();
    [[nodiscard]] std::optional<util::BitMagic> opt_sparse_map() const;

    template<typename TagType>
    auto begin() const {
        using RawType = typename TagType::DataTypeTag::raw_type;
        return TypedColumnIterator<TagType, const RawType>(*this, true);
    }

    template<typename TagType>
    auto end() const {
        using RawType = typename TagType::DataTypeTag::raw_type;
        return TypedColumnIterator<TagType, const RawType>(*this, false);
    }

    template<typename T>
    requires std::integral<T> || std::floating_point<T>
    void set_scalar(ssize_t row_offset, T val) {
        util::check(
                sizeof(T) == get_type_size(type_.data_type()),
                "Type mismatch in set_scalar, expected {} byte scalar got {} byte scalar",
                get_type_size(type_.data_type()),
                sizeof(T)
        );

        auto prev_logical_row = last_logical_row_;
        last_logical_row_ = row_offset;
        ++last_physical_row_;

        if (row_offset != prev_logical_row + 1) {
            if (sparse_permitted()) {
                if (!sparse_map_) {
                    if (prev_logical_row != -1)
                        backfill_sparse_map(prev_logical_row);
                    else
                        (void)sparse_map();
                }
            } else {
                util::raise_rte("set_scalar expected row {}, actual {} ", prev_logical_row + 1, row_offset);
            }
        }

        if (is_sparse()) {
            ARCTICDB_TRACE(log::version(), "setting sparse bit at position {}", last_logical_row_);
            set_sparse_bit_for_row(last_logical_row_);
        }

        ARCTICDB_TRACE(log::version(), "Setting scalar {} at {} ({})", val, last_logical_row_, last_physical_row_);

        data_.ensure<T>();
        *data_.ptr_cast<T>(position_t(last_physical_row_), sizeof(T)) = val;
        data_.commit();

        util::check(last_physical_row_ + 1 == row_count(), "Row count calculation incorrect in set_scalar");
    }

    template<class T>
    void push_back(T val) {
        set_scalar(last_logical_row_ + 1, val);
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_external_block(ssize_t row_offset, T* val, size_t size) {
        util::check_arg(
                last_logical_row_ + 1 == row_offset,
                "set_external_block expected row {}, actual {} ",
                last_logical_row_ + 1,
                row_offset
        );
        auto bytes = sizeof(T) * size;
        const_cast<ChunkedBuffer&>(data_.buffer()).add_external_block(reinterpret_cast<const uint8_t*>(val), bytes);
        last_logical_row_ += static_cast<ssize_t>(size);
        last_physical_row_ = last_logical_row_;
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_sparse_block(ssize_t row_offset, T* ptr, size_t rows_to_write) {
        util::check(row_offset == 0, "Cannot write sparse column with existing data");
        auto new_buffer = util::scan_floating_point_to_sparse(ptr, rows_to_write, sparse_map());
        std::swap(data_.buffer(), new_buffer);
    }

    void set_sparse_block(ChunkedBuffer&& buffer, util::BitSet&& bitset);

    void set_sparse_block(ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset);

    ChunkedBuffer&& release_buffer();

    Buffer&& release_shapes();

    template<util::arithmetic_tensor TensorType>
    void set_array(ssize_t row_offset, TensorType& val) {
        using value_type = typename TensorType::value_type;
        ARCTICDB_SAMPLE(ColumnSetArray, RMTSF_Aggregate)
        magic_.check();
        util::check_arg(
                last_logical_row_ + 1 == row_offset,
                "set_array expected row {}, actual {} ",
                last_logical_row_ + 1,
                row_offset
        );
        data_.ensure_bytes(val.nbytes());
        shapes_.ensure<shape_t>(val.ndim());
        memcpy(shapes_.cursor(), val.shape(), val.ndim() * sizeof(shape_t));
        auto info = val.request();
        util::FlattenHelper flatten(val);
        auto data_ptr = reinterpret_cast<value_type*>(data_.cursor());
        flatten.flatten(data_ptr, reinterpret_cast<const value_type*>(info.ptr));
        update_offsets(val.nbytes());
        data_.commit();
        shapes_.commit();
        ++last_logical_row_;
    }

    void set_empty_array(ssize_t row_offset, int dimension_count);
    void set_type(TypeDescriptor td);
    ssize_t last_row() const;

    void check_magic() const;

    void unsparsify(size_t num_rows);
    void sparsify();

    void string_array_prologue(ssize_t row_offset, size_t num_strings);
    void string_array_epilogue(size_t num_strings);

    void set_string_array(
            ssize_t row_offset, size_t string_size, size_t num_strings, char* input, StringPool& string_pool
    );
    void set_string_list(ssize_t row_offset, const std::vector<std::string>& input, StringPool& string_pool);

    void append_sparse_map(const util::BitMagic& bv, position_t at_row);
    void append(const Column& other, position_t at_row);

    // Sorts the column by an external column's jive_table.
    // pre_allocated_space can be reused across different calls to sort_external to avoid unnecessary allocations.
    // It has to be the same size as the jive_table.
    void sort_external(const JiveTable& jive_table, std::vector<uint32_t>& pre_allocated_space);

    void mark_absent_rows(size_t num_rows);

    void default_initialize_rows(size_t start_pos, size_t num_rows, bool ensure_alloc);
    void default_initialize_rows(
            size_t start_pos, size_t num_rows, bool ensure_alloc, const std::optional<Value>& default_value
    );

    void set_row_data(size_t row_id);

    size_t get_physical_offset(size_t row) const;

    void set_sparse_map(util::BitSet&& bitset);

    std::optional<position_t> get_physical_row(position_t row) const;

    bool has_value_at(position_t row) const;

    void set_allow_sparse(Sparsity value);

    void set_shapes_buffer(size_t row_count);

    // The following two methods inflate (reduplicate) numpy string arrays that are potentially multi-dimensional,
    // i.e where the value is not a string but an array of strings
    void inflate_string_array(
            const TensorType<position_t>& string_refs, CursoredBuffer<ChunkedBuffer>& data,
            CursoredBuffer<Buffer>& shapes, boost::container::small_vector<position_t, 1>& offsets,
            const StringPool& string_pool
    );

    void inflate_string_arrays(const StringPool& string_pool);

    // Used when the column has been inflated externally, i.e. because it has be done
    // in a pipeline of tiled sub-segments
    void set_inflated(size_t inflated_count);

    bool is_inflated() const;

    void change_type(DataType target_type);

    void truncate_first_block(size_t row);

    void truncate_last_block(size_t row);

    void truncate_single_block(size_t start_offset, size_t end_offset);

    position_t row_count() const;

    std::optional<StringArrayData> string_array_at(position_t idx, const StringPool& string_pool);

    ChunkedBuffer::Iterator get_iterator() const;

    size_t bytes() const;

    ColumnData data() const;

    const uint8_t* ptr() const;

    uint8_t* ptr();

    TypeDescriptor type() const;

    size_t num_blocks() const;

    const shape_t* shape_ptr() const;

    void set_orig_type(const TypeDescriptor& desc);

    bool has_orig_type() const;

    const TypeDescriptor& orig_type() const;

    void compact_blocks();

    const auto& blocks() const { return data_.buffer().blocks(); }

    const auto& block_offsets() const { return data_.buffer().block_offsets(); }

    shape_t* allocate_shapes(std::size_t bytes);

    uint8_t* allocate_data(std::size_t bytes);

    void advance_data(std::size_t size);

    void advance_shapes(std::size_t size);

    template<typename T>
    std::optional<T> scalar_at(position_t row) const {
        auto physical_row = get_physical_row(row);
        if (!physical_row)
            return std::nullopt;

        return *data_.buffer().ptr_cast<T>(bytes_offset(*physical_row), sizeof(T));
    }

    // Copies all physical scalars to a std::vector<T>. This is useful if you require many random access operations
    // and you would like to avoid the overhead of computing the exact location every time.
    template<typename T>
    std::vector<T> clone_scalars_to_vector() const {
        auto values = std::vector<T>();
        values.reserve(row_count());
        const auto& buffer = data_.buffer();
        for (auto i = 0u; i < row_count(); ++i) {
            values.push_back(*buffer.ptr_cast<T>(i * item_size(), sizeof(T)));
        }
        return values;
    }

    // N.B. returning a value not a reference here, so it will need to be pre-checked when data is sparse or it
    // will likely 'splode.
    template<typename T>
    T& reference_at(position_t row) {
        util::check_arg(
                row < row_count(), "Scalar reference index {} out of bounds in column of size {}", row, row_count()
        );
        util::check_arg(is_scalar(), "get_reference requested on non-scalar column");
        return *data_.buffer().ptr_cast<T>(bytes_offset(row), sizeof(T));
    }

    template<typename T>
    std::optional<TensorType<T>> tensor_at(position_t idx) const {
        util::check_arg(idx < row_count(), "Tensor index out of bounds in column");
        util::check_arg(type_.dimension() != Dimension::Dim0, "tensor_at called on scalar column");
        const shape_t* shape_ptr = shape_index(idx);
        auto ndim = ssize_t(type_.dimension());
        return TensorType<T>(
                shape_ptr,
                ndim,
                type().data_type(),
                get_type_size(type().data_type()),
                reinterpret_cast<const T*>(
                        data_.buffer().ptr_cast<uint8_t>(bytes_offset(idx), calc_elements(shape_ptr, ndim))
                ),
                ndim
        );
    }

    template<typename T>
    const T* ptr_cast(position_t idx, size_t required_bytes) const {
        return data_.buffer().ptr_cast<T>(bytes_offset(idx), required_bytes);
    }

    template<typename T>
    T* ptr_cast(position_t idx, size_t required_bytes) {
        return const_cast<T*>(const_cast<const Column*>(this)->ptr_cast<T>(idx, required_bytes));
    }

    [[nodiscard]] ChunkedBuffer& buffer();

    uint8_t* bytes_at(size_t bytes, size_t required);

    const uint8_t* bytes_at(size_t bytes, size_t required) const;

    void assert_size(size_t bytes) const;

    template<typename T>
    std::optional<position_t> search_unsorted(T val) const {
        util::check_arg(is_scalar(), "Cannot index on multidimensional values");
        for (position_t i = 0; i < row_count(); ++i) {
            if (val == *ptr_cast<T>(i, sizeof(T)))
                return i;
        }
        return std::nullopt;
    }

    // Only works if column is of numeric type and is monotonically increasing
    // Returns the index such that if val were inserted before that index, the order would be preserved
    // By default returns the lowest index satisfying this property. If from_right=true, returns the highest such index
    // from (inclusive) and to (exclusive) can optionally be provided to search a subset of the rows in the column
    template<class T>
    requires std::integral<T> || std::floating_point<T>
    size_t search_sorted(
            T val, bool from_right = false, std::optional<int64_t> from = std::nullopt,
            std::optional<int64_t> to = std::nullopt
    ) const {
        // There will not necessarily be a unique answer for sparse columns
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                !is_sparse(), "Column::search_sorted not supported with sparse columns"
        );
        auto column_data = data();
        return details::visit_type(
                type().data_type(),
                [this, &column_data, val, from_right, &from, &to](auto type_desc_tag) -> int64_t {
                    using type_info = ScalarTypeInfo<decltype(type_desc_tag)>;
                    auto accessor = random_accessor<typename type_info::TDT>(&column_data);
                    if constexpr (std::is_same_v<T, typename type_info::RawType>) {
                        int64_t first = from.value_or(0);
                        const int64_t last = to.value_or(row_count());
                        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                                last >= first,
                                "Invalid input range for Column::search_sorted. First: {}, Last: {}",
                                first,
                                last
                        );
                        int64_t step;
                        int64_t count{last - first};
                        int64_t idx;
                        if (from_right) {
                            while (count > 0) {
                                idx = first;
                                step = count / 2;
                                idx = std::min(idx + step, last);
                                if (accessor.at(idx) <= val) {
                                    first = ++idx;
                                    count -= step + 1;
                                } else {
                                    count = step;
                                }
                            }
                        } else {
                            while (count > 0) {
                                idx = first;
                                step = count / 2;
                                idx = std::min(idx + step, last);
                                if (accessor.at(idx) < val) {
                                    first = ++idx;
                                    count -= step + 1;
                                } else {
                                    count = step;
                                }
                            }
                        }
                        return first;
                    } else {
                        // TODO: Could relax this requirement using something like has_valid_common_type
                        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                "Column::search_sorted requires input value to be of same type as column"
                        );
                        return {};
                    }
                }
        );
    }

    [[nodiscard]] static std::vector<std::shared_ptr<Column>> split(
            const std::shared_ptr<Column>& column, size_t num_rows
    );
    /// @brief Produces a new column containing only the data in range [start_row, end_row)
    /// @param[in] start_row Inclusive start of the row range
    /// @param[in] end_row Exclusive end of the row range
    [[nodiscard]] static std::shared_ptr<Column> truncate(
            const std::shared_ptr<Column>& column, size_t start_row, size_t end_row
    );

    void init_buffer();

    ChunkedBuffer& create_extra_buffer(
            size_t offset, ExtraBufferType type, size_t size, AllocationType allocation_type
    );

    ChunkedBuffer& get_extra_buffer(size_t offset, ExtraBufferType type) const;

    void set_extra_buffer(size_t offset, ExtraBufferType type, ChunkedBuffer&& buffer);

    bool has_extra_buffer(size_t offset, ExtraBufferType type) const;

  private:
    position_t last_offset() const;
    void update_offsets(size_t nbytes);
    bool is_scalar() const;
    const shape_t* shape_index(position_t idx) const;
    position_t bytes_offset(position_t idx) const;
    position_t scalar_offset(position_t idx) const;
    size_t item_size() const;
    size_t inflated_row_count() const;
    size_t num_shapes() const;
    void set_sparse_bit_for_row(size_t sparse_location);
    void regenerate_offsets() const;

    // Permutes the physical column storage based on the given sorted_pos.
    void physical_sort_external(std::vector<uint32_t>&& sorted_pos);

    // Members
    CursoredBuffer<ChunkedBuffer> data_;
    CursoredBuffer<Buffer> shapes_;

    mutable boost::container::small_vector<position_t, 1> offsets_;
    TypeDescriptor type_;
    std::optional<TypeDescriptor> orig_type_;
    ssize_t last_logical_row_ = -1;
    ssize_t last_physical_row_ = -1;

    bool inflated_ = false;
    Sparsity allow_sparse_ = Sparsity::NOT_PERMITTED;

    std::optional<util::BitMagic> sparse_map_;
    FieldStatsImpl stats_;

    std::unique_ptr<std::once_flag> init_buffer_ = std::make_unique<std::once_flag>();

    std::unique_ptr<ExtraBufferContainer> extra_buffers_;
    util::MagicNum<'D', 'C', 'o', 'l'> magic_;
};

template<typename TagType>
JiveTable create_jive_table(const Column& column) {
    JiveTable output(column.row_count());
    std::iota(std::begin(output.orig_pos_), std::end(output.orig_pos_), 0);

    // Calls to scalar_at are expensive, so we precompute them to speed up the sort compare function.
    auto column_data = column.data();
    auto accessor = random_accessor<TagType>(&column_data);
    std::sort(std::begin(output.orig_pos_), std::end(output.orig_pos_), [&](const auto& a, const auto& b) -> bool {
        return accessor.at(a) < accessor.at(b);
    });

    // Obtain the sorted_pos_ by reversing the orig_pos_ permutation
    for (auto i = 0u; i < output.orig_pos_.size(); ++i) {
        output.sorted_pos_[output.orig_pos_[i]] = i;
    }

    return output;
}

JiveTable create_jive_table(const std::vector<std::shared_ptr<Column>>& columns);

} // namespace arcticdb
