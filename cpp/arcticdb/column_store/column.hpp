/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/util/flatten_utils.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/sparse_utils.hpp>

#include <folly/container/Enumerate.h>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <numeric>
#include <optional>

namespace py = pybind11;

namespace arcticdb {

/// @cond

// this is needed to make templates of templates work
// since py::array_t has more than one template parameter
// (the rest are defaulted)
template< class T>
using py_array_t = py::array_t<T>;

/// @endcond

using namespace arcticdb::entity;

// N.B. this will not catch all the things that C++ considers to be narrowing conversions, because
// it doesn't take into account integral promotion, however we don't care about that for the
// purpose for which it is used in this file.
template <typename SourceType, typename TargetType>
constexpr bool is_narrowing_conversion() {
    if(sizeof(TargetType) < sizeof(SourceType))
        return true;

    if(sizeof(SourceType) == sizeof(TargetType) && std::is_integral_v<TargetType> && std::is_unsigned_v<SourceType> && std::is_signed_v<TargetType>) {
        return true;
    }

    return false;
}

struct JiveTable {
    explicit JiveTable(size_t num_rows) :
        orig_pos_(num_rows),
        sorted_pos_(num_rows) {
    }

    std::vector<uint32_t> orig_pos_;
    std::vector<uint32_t> sorted_pos_;
};

class Column;

template <typename T>
JiveTable create_jive_table(const Column& col);

class Column {

public:

    template<typename TDT, typename ValueType>
    class TypedColumnIterator :  public boost::iterator_facade<TypedColumnIterator<TDT, ValueType>, ValueType, boost::random_access_traversal_tag> {
        using RawType =  std::decay_t<typename TDT::DataTypeTag::raw_type>;
        static constexpr size_t type_size = sizeof(RawType);

        ColumnData  parent_;
        std::optional<TypedBlockData<TDT>> block_;
        typename TypedBlockData<TDT>::template TypedColumnBlockIterator<const ValueType> block_pos_;
        typename TypedBlockData<TDT>::template TypedColumnBlockIterator<const ValueType> block_end_;

        void set_block_range() {
            if(block_) {
                block_pos_ = std::begin(*block_);
                block_end_ = std::end(*block_);
            }
        }

        void set_next_block() {
            if(auto block  = parent_.next<TDT>(); block)
                block_.emplace(std::move(*block));
            else
                block_ = std::nullopt;

            set_block_range();
        }

    public:
        TypedColumnIterator(const Column& col, bool begin) :
        parent_(col.data()),
        block_(begin ? parent_.next<TDT>() : std::nullopt) {
            if(begin)
                set_block_range();
        }


        template <class OtherValue>
        explicit TypedColumnIterator(const TypedColumnIterator<TDT, OtherValue>& other) :
        parent_(other.parent_),
        block_(other.block_),
        block_pos_(other.block_pos_),
        block_end_(other.block_end_){ }

        template <class OtherValue>
        bool equal(const TypedColumnIterator<TDT, OtherValue>& other) const{
            if(block_) {
                if(!other.block_)
                    return false;

                return *block_ == *other.block_ && block_pos_ == other.block_pos_;
            }
            return !other.block_;
        }

        ssize_t distance_to(const TypedColumnIterator& other) const {
            return other.get_offset() - get_offset();
        }

        void increment(){
            ++block_pos_;
            if(block_pos_ == block_end_)
                set_next_block();
        }

        void decrement(){
            if(!block_) {
                block_ = parent_.last<TDT>();
                if(block_) {
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
            if(!block_)
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

        void advance(ptrdiff_t n){
            auto offset = get_offset();
            offset += n;
            set_offset(offset);
        }

        const ValueType& dereference() const {
            return *block_pos_;
        }
    };

    struct StringArrayData {
        ssize_t num_strings_;
        ssize_t string_size_;
        const char *data_;

        StringArrayData(ssize_t n, ssize_t s, const char *d) :
                num_strings_(n),
                string_size_(s),
                data_(d) {
        }
    };

    Column() : type_(null_type_descriptor()) {}

    Column(TypeDescriptor type, bool allow_sparse) :
        Column(type, 0, false, allow_sparse) {
    }

    Column(TypeDescriptor type, bool allow_sparse, ChunkedBuffer&& buffer) :
        data_(std::move(buffer)),
        type_(type),
        allow_sparse_(allow_sparse) {
    }

    Column(
        TypeDescriptor type,
        size_t expected_rows,
        bool presize,
        bool allow_sparse) :
            // Array types are stored on disk as flat sequences. The python layer cannot work with this. We need to pass
            // it pointers to an array type (at numpy arrays at the moment). When we allocate a column for an array we
            // need to allocate space for one pointer per row. This also affects how we handle arrays to python as well.
            // Check cpp/arcticdb/column_store/column_utils.hpp::array_at and cpp/arcticdb/column_store/column.hpp::Column
            data_(expected_rows * (type.dimension() == Dimension::Dim0 ? get_type_size(type.data_type()) : sizeof(void*)), presize),
            type_(type),
            allow_sparse_(allow_sparse){
        ARCTICDB_TRACE(log::inmem(), "Creating column with descriptor {}", type);
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(Column)

    friend bool operator==(const Column& left, const Column& right);
    friend bool operator!=(const Column& left, const Column& right);

    Column clone() const;

    bool is_sparse() const;

    bool sparse_permitted() const;

    void backfill_sparse_map(ssize_t to_row) {
        ARCTICDB_TRACE(log::version(), "Backfilling sparse map to position {}", to_row);
        sparse_map().set_range(0, bv_size(to_row), true);
    }

    std::optional<util::BitMagic>& opt_sparse_map() {
        return sparse_map_;
    }

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

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_scalar(ssize_t row_offset, T val) {
        util::check(sizeof(T) == get_type_size(type_.data_type()), "Type mismatch in set_scalar, expected {}",
                    get_type_size(type_.data_type()));

        auto prev_logical_row = last_logical_row_;
        last_logical_row_ = row_offset;
        ++last_physical_row_;

        if(row_offset != prev_logical_row + 1) {
            if(sparse_permitted()) {
                if(!sparse_map_) {
                    if(prev_logical_row != -1)
                        backfill_sparse_map(prev_logical_row);
                    else
                        (void)sparse_map();
                }
            } else {
                util::raise_rte("set_scalar expected row {}, actual {} ", prev_logical_row + 1, row_offset);
            }
        }

        if(is_sparse()) {
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
    inline void set_external_block(ssize_t row_offset, T *val, size_t size) {
        util::check_arg(last_logical_row_ + 1 == row_offset, "set_external_block expected row {}, actual {} ", last_logical_row_ + 1, row_offset);
        auto bytes = sizeof(T) * size;
        const_cast<ChunkedBuffer&>(data_.buffer()).add_external_block(reinterpret_cast<const uint8_t*>(val), bytes, data_.buffer().last_offset());
        last_logical_row_ += static_cast<ssize_t>(size);
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    inline void set_sparse_block(ssize_t row_offset, T *ptr, size_t rows_to_write) {
        util::check(row_offset == 0, "Cannot write sparse column with existing data");
        auto new_buffer = util::scan_floating_point_to_sparse(ptr, rows_to_write, sparse_map());
        std::swap(data_.buffer(), new_buffer);
    }

    inline void set_sparse_block(ChunkedBuffer&& buffer, util::BitSet&& bitset) {
        data_.buffer() = std::move(buffer);
        sparse_map_ = std::move(bitset);
    }

    inline void set_sparse_block(ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset) {
        data_.buffer() = std::move(buffer);
        shapes_.buffer() = std::move(shapes);
        sparse_map_ = std::move(bitset);
    }

    ChunkedBuffer&& release_buffer() {
        return std::move(data_.buffer());
    }

    Buffer&& release_shapes() {
        return std::move(shapes_.buffer());
    }

    template<class T, template<class> class Tensor, std::enable_if_t<
            std::is_integral_v<T> || std::is_floating_point_v<T>,
            int> = 0>
    void set_array(ssize_t row_offset, Tensor<T> &val) {
        ARCTICDB_SAMPLE(ColumnSetArray, RMTSF_Aggregate)
        magic_.check();
        util::check_arg(last_logical_row_ + 1 == row_offset, "set_array expected row {}, actual {} ", last_logical_row_ + 1, row_offset);
        data_.ensure_bytes(val.nbytes());
        shapes_.ensure<shape_t>(val.ndim());
        memcpy(shapes_.ptr(), val.shape(), val.ndim() * sizeof(shape_t));
        auto info = val.request();
        util::FlattenHelper flatten(val);
        auto data_ptr = reinterpret_cast<T*>(data_.ptr());
        flatten.flatten(data_ptr, reinterpret_cast<const T *>(info.ptr));
        update_offsets(val.nbytes());
        data_.commit();
        shapes_.commit();
        ++last_logical_row_;
    }

    template<class T, std::enable_if_t< std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_array(ssize_t row_offset, py::array_t<T>& val) {
        ARCTICDB_SAMPLE(ColumnSetArray, RMTSF_Aggregate)
        magic_.check();
        util::check_arg(last_logical_row_ + 1 == row_offset, "set_array expected row {}, actual {} ", last_logical_row_ + 1, row_offset);
        data_.ensure_bytes(val.nbytes());
        shapes_.ensure<shape_t>(val.ndim());
        memcpy(shapes_.ptr(), val.shape(), val.ndim() * sizeof(shape_t));
        auto info = val.request();
        util::FlattenHelper<T, py_array_t> flatten(val);
        auto data_ptr = reinterpret_cast<T*>(data_.ptr());
        flatten.flatten(data_ptr, reinterpret_cast<const T*>(info.ptr));
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

    void set_string_array(ssize_t row_offset,
                          size_t string_size,
                          size_t num_strings,
                          char *input,
                          StringPool &string_pool);
    void set_string_list(ssize_t row_offset,
                         const std::vector<std::string> &input,
                         StringPool &string_pool);

    void append_sparse_map(const util::BitMagic& bv, position_t at_row);
    void append(const Column& other, position_t at_row);

    void sort_external(const JiveTable& jive_table);

    void mark_absent_rows(size_t num_rows);

    void default_initialize_rows(size_t start_pos, size_t num_rows, bool ensure_alloc);

    void set_row_data(size_t row_id);

    size_t get_physical_offset(size_t row) const;

    void set_sparse_map(util::BitSet&& bitset);

    std::optional<position_t> get_physical_row(position_t row) const;

    bool has_value_at(position_t row) const;

    void set_allow_sparse(bool value);

    void set_shapes_buffer(size_t row_count);


    // The following two methods inflate (reduplicate) numpy string arrays that are potentially multi-dimensional,
    // i.e where the value is not a string but an array of strings
    void inflate_string_array(const TensorType<OffsetString::offset_t> &string_refs,
                              CursoredBuffer<ChunkedBuffer> &data,
                              CursoredBuffer<Buffer> &shapes,
                              std::vector<position_t> &offsets,
                              const StringPool &string_pool);

    void inflate_string_arrays(const StringPool &string_pool);

    // Used when the column has been inflated externally, i.e. because it has be done
    // in a pipeline of tiled sub-segments
    void set_inflated(size_t inflated_count);

    bool is_inflated() const;

    void change_type(DataType target_type);

    position_t row_count() const;

    std::optional<StringArrayData> string_array_at(position_t idx, const StringPool &string_pool) {
        util::check_arg(idx < row_count(), "String array index out of bounds in column");
        util::check_arg(type_.dimension() == Dimension::Dim1, "String array should always be one dimensional");
        if (!inflated_)
            inflate_string_arrays(string_pool);

        const shape_t *shape_ptr = shape_index(idx);
        auto num_strings = *shape_ptr;
        ssize_t string_size = offsets_[idx] / num_strings;
        return StringArrayData{num_strings, string_size, data_.ptr_cast<char>(bytes_offset(idx), num_strings * string_size)};
    }

    ChunkedBuffer::Iterator get_iterator() const {
        return {const_cast<ChunkedBuffer*>(&data_.buffer()), get_type_size(type_.data_type())};
    }

    size_t bytes() const {
        return data_.bytes();
    }

    ColumnData data() const {
        return ColumnData(&data_.buffer(), &shapes_.buffer(), type_, sparse_map_ ? &*sparse_map_ : nullptr);
    }

    const uint8_t* ptr() const {
        return data_.buffer().data();
    }

    uint8_t* ptr() {
        return data_.buffer().data();
    }

    TypeDescriptor type() const { return type_; }

    size_t num_blocks() const  {
        return data_.buffer().num_blocks();
    }

    const shape_t* shape_ptr() const {
        return shapes_.ptr_cast<shape_t>(0, num_shapes());
    }

    void set_orig_type(const TypeDescriptor& desc) {
        orig_type_ = desc;
    }

    bool has_orig_type() const {
        return static_cast<bool>(orig_type_);
    }

    const TypeDescriptor& orig_type() const  {
        return orig_type_.value();
    }

    void compact_blocks() {
        data_.compact_blocks();
    }

    const auto& blocks() const {
        return data_.buffer().blocks();
    }

    inline shape_t *allocate_shapes(std::size_t bytes) {
        shapes_.ensure_bytes(bytes);
        return reinterpret_cast<shape_t *>(shapes_.ptr());
    }

    inline uint8_t *allocate_data(std::size_t bytes) {
        util::check(bytes != 0, "Allocate data called with zero size");
        data_.ensure_bytes(bytes);
        return data_.ptr();
    }

    inline void advance_data(std::size_t size) {
        data_.advance(position_t(size));
    }

    inline void advance_shapes(std::size_t size) {
        shapes_.advance(position_t(size));
    }

    template<typename T>
    std::optional<T> scalar_at(position_t row) const {
        auto physical_row = get_physical_row(row);
        if(!physical_row)
            return std::nullopt;

        return *data_.buffer().ptr_cast<T>(bytes_offset(*physical_row), sizeof(T));
    }

    template<typename T>
    std::vector<std::optional<T>> clone_scalars_to_vector() const {
        std::vector<std::optional<T>> values(row_count(), std::nullopt);
        if (!is_sparse()){
            const auto& buffer = data_.buffer();
            for (auto i=0u; i<row_count(); ++i){
                values[i] = std::optional<T>(*buffer.ptr_cast<T>(i*item_size(), sizeof(T)));
            }
        }
        else{
            const auto& buffer = data_.buffer();
            const auto& sm = sparse_map();
            auto en = sm.first();
            auto en_end = sm.end();
            for (auto i=0u; en < en_end; ++en, ++i){
                values[*en] = std::optional<T>(*buffer.ptr_cast<T>(i*item_size(), sizeof(T)));
            }
        }
        return values;
    }

    // N.B. returning a value not a reference here, so it will need to be pre-checked when data is sparse or it
    // will likely 'splode.
    template<typename T>
    T& reference_at(position_t row)  {
        util::check_arg(row < row_count(), "Scalar reference index {} out of bounds in column of size {}", row, row_count());
        util::check_arg(is_scalar(), "get_reference requested on non-scalar column");
        return *data_.buffer().ptr_cast<T>(bytes_offset(row), sizeof(T));
    }

    template<typename T>
    std::optional<TensorType<T>> tensor_at(position_t idx) const {
        util::check_arg(idx < row_count(), "Tensor index out of bounds in column");
        util::check_arg(type_.dimension() != Dimension::Dim0, "tensor_at called on scalar column");
        const shape_t *shape_ptr = shape_index(idx);
        auto ndim = ssize_t(type_.dimension());
        return TensorType<T>(
                shape_ptr,
                ndim,
                type().data_type(),
                get_type_size(type().data_type()),
                reinterpret_cast<const T*>(data_.buffer().ptr_cast<uint8_t>(bytes_offset(idx), calc_elements(shape_ptr, ndim))),
                ndim);
    }

    template<typename T>
    const T *ptr_cast(position_t idx, size_t required_bytes) const {
        return data_.buffer().ptr_cast<T>(bytes_offset(idx), required_bytes);
    }

    template<typename T>
    T *ptr_cast(position_t idx, size_t required_bytes) {
        return const_cast<T*>(const_cast<const Column*>(this)->ptr_cast<T>(idx, required_bytes));
    }

    [[nodiscard]] auto& buffer() {
        return data_.buffer();
    }

    //TODO this will need to be more efficient - index each block?
    template<typename T>
    std::optional<position_t> index_of(T val) const {
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
    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    size_t search_sorted(T val, bool from_right=false) const {
        // There will not necessarily be a unique answer for sparse columns
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(!is_sparse(),
                                                        "Column::search_sorted not supported with sparse columns");
        std::optional<size_t> res;
        type().visit_tag([this, &res, val, from_right](auto type_desc_tag){
            using TDT = decltype(type_desc_tag);
            using RawType = typename TDT::DataTypeTag::raw_type;
            if constexpr(std::is_same_v<T, RawType>) {
                int64_t low{0};
                int64_t high{row_count() -1};
                while (!res.has_value()) {
                    auto mid{low + (high - low) / 2};
                    auto mid_value = *scalar_at<RawType>(mid);
                    if (val == mid_value) {
                        // At least one value in the column exactly matches the input val
                        // Search to the right/left for the last/first such value
                        if (from_right) {
                            while (++mid <= high && val == *scalar_at<RawType>(mid)) {}
                            res = mid;
                        } else {
                            while (--mid >= low && val == *scalar_at<RawType>(mid)) {}
                            res = mid + 1;
                        }
                    } else if (val > mid_value) {
                        if (mid + 1 <= high && val >= *scalar_at<RawType>(mid + 1)) {
                            // Narrow the search interval
                            low = mid + 1;
                        } else {
                            // val is less than the next value, so we have found the right interval
                            res = mid + 1;
                        }
                    } else { // val < mid_value
                        if (mid - 1 >= low && val <= *scalar_at<RawType>(mid + 1)) {
                            // Narrow the search interval
                            high = mid - 1;
                        } else {
                            // val is greater than the previous value, so we have found the right interval
                            res = mid;
                        }
                    }
                }
            } else {
                // TODO: Could relax this requirement using something like has_valid_common_type
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                        "Column::search_sorted requires input value to be of same type as column");
            }
        });
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                res.has_value(), "Column::search_sorted should always find an index");
        return *res;
    }

    static std::vector<std::shared_ptr<Column>> split(const std::shared_ptr<Column>& column, size_t num_rows);

    static std::shared_ptr<Column> truncate(const std::shared_ptr<Column>& column, size_t start_row, size_t end_row);

private:

    position_t last_offset() const;
    void update_offsets(size_t nbytes);
    bool is_scalar() const;
    const shape_t *shape_index(position_t idx) const;
    position_t bytes_offset(position_t idx) const;
    position_t scalar_offset(position_t idx) const;
    size_t item_size() const;
    size_t inflated_row_count() const;
    size_t num_shapes() const;
    void set_sparse_bit_for_row(size_t sparse_location);
    bool empty() const;
    void regenerate_offsets() const;
    void physical_sort_external(std::vector<uint32_t> &&sorted_pos, size_t physical_rows);

    [[nodiscard]] util::BitMagic& sparse_map();
    const util::BitMagic& sparse_map() const;

    // Members
    CursoredBuffer<ChunkedBuffer> data_;
    CursoredBuffer<Buffer> shapes_;

    mutable std::vector<position_t> offsets_;
    TypeDescriptor type_;
    std::optional<TypeDescriptor> orig_type_;
    ssize_t last_logical_row_ = -1;
    ssize_t last_physical_row_ = -1;

    bool inflated_ = false;
    bool allow_sparse_ = false;
    std::optional<util::BitMagic> sparse_map_;
    util::MagicNum<'D', 'C', 'o', 'l'> magic_;
}; //class Column

template <typename T>
JiveTable create_jive_table(const Column& col) {
    JiveTable output(col.row_count());
    std::iota(std::begin(output.orig_pos_), std::end(output.orig_pos_), 0);

    // Calls to scalar_at are expensive, so we precompute them to speed up the sort compare function.
    auto values = col.template clone_scalars_to_vector<T>();
    std::sort(std::begin(output.orig_pos_), std::end(output.orig_pos_),[&](const auto& a, const auto& b) -> bool {
        return values[a] < values[b];
    });

    // Obtain the sorted_pos_ by reversing the orig_pos_ permutation
    for (auto i=0u; i<output.orig_pos_.size(); ++i){
        output.sorted_pos_[output.orig_pos_[i]] = i;
    }

    return output;
}

} //namespace arcticdb
