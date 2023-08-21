/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/cursor.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/util/flatten_utils.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/util/sparse_utils.hpp>

#include <folly/Likely.h>
#include <folly/container/Enumerate.h>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <bitmagic/bm.h>
#include <bitmagic/bmserial.h>

#include <optional>
#include <numeric>


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
        sorted_pos_(num_rows),
        unsorted_rows_(bv_size(num_rows)) {
    }

    std::vector<uint32_t> orig_pos_;
    std::vector<uint32_t> sorted_pos_;
    util::BitSet unsorted_rows_;
    size_t num_unsorted_ = 0;
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
                block_pos_ = std::begin(block_.value());
                block_end_ = std::end(block_.value());
            }
        }

        void set_next_block() {
            if(auto block  = parent_.next<TDT>(); block)
                block_.emplace(std::move(block.value()));
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

                return block_.value() == other.block_.value() && block_pos_ == other.block_pos_;
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
                if (block_pos_ < block_.value().begin()) {
                    auto offset = block_.value().offset() - type_size;
                    set_offset(offset);
                }
            }
        }

        [[nodiscard]] ssize_t get_offset() const {
            if(!block_)
                return parent_.buffer().bytes() / type_size;

            const auto off = block_.value().offset();
            const auto dist = std::distance(std::begin(block_.value()), block_pos_);
            return off + dist;
        }

        void set_offset(size_t offset) {
            const auto bytes = offset * type_size;
            auto block_and_offset = parent_.buffer().block_and_offset(bytes);
            block_ = ColumnData::make_typed_block<TDT>(block_and_offset.block_);
            block_pos_ = std::begin(block_.value());
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
            data_(expected_rows * get_type_size(type.data_type()), presize),
            type_(type),
            allow_sparse_(allow_sparse){
        ARCTICDB_TRACE(log::inmem(), "Creating column with descriptor {}", type);
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(Column)

    Column clone() const {
        Column output;

        output.data_ = data_.clone();
        output.shapes_ = shapes_.clone();

        output.offsets_ = offsets_;
        output.type_ = type_;
        output.orig_type_ = orig_type_;
        output.last_logical_row_ = last_logical_row_;
        output.last_physical_row_ = last_physical_row_;
        output.inflated_ = inflated_;
        output.allow_sparse_ = allow_sparse_;
        output.sparse_map_ = sparse_map_;
        return output;
    }

    friend bool operator==(const Column& left, const Column& right) {
        if (left.row_count() != right.row_count())
            return false;

        if (left.type_ != right.type_)
            return false;

        return left.type_.visit_tag([&left, &right](auto l_impl) {
            using LeftType= std::decay_t<decltype(l_impl)>;
            using LeftRawType = typename LeftType::DataTypeTag::raw_type;

            return right.type_.visit_tag([&left, &right](auto r_impl) {
                using RightType= std::decay_t<decltype(r_impl)>;
                using RightRawType = typename RightType::DataTypeTag::raw_type;

                if constexpr(std::is_same_v < LeftRawType, RightRawType>) {
                    for (auto i = 0u; i < left.row_count(); ++i) {
                        auto left_val =left.scalar_at<LeftRawType>(i);
                        auto right_val =right.scalar_at<RightRawType>(i);
                        if (left_val != right_val)
                            return false;
                    }
                    return true;
                } else {
                    return false;
                }
            });
        });
    }

    friend bool operator!=(const Column& left, const Column& right) {
        return !(left == right);
    }

    bool is_sparse() const {
        if(last_logical_row_ != last_physical_row_) {
            util::check(static_cast<bool>(sparse_map_), "Expected sparse map in column with logical row {} and physical row {}", last_logical_row_, last_physical_row_);
            return true;
        }
        return false;
    }

    bool sparse_permitted() const {
        return allow_sparse_;
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

    void backfill_sparse_map(ssize_t to_row) {
        ARCTICDB_TRACE(log::version(), "Backfilling sparse map to position {}", to_row);
        sparse_map().set_range(0, bv_size(to_row), true);
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

    std::optional<util::BitMagic>& opt_sparse_map() {
        return sparse_map_;
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
        util::check(row_offset == 0, "Cannot write sparse column  with existing data");
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

    ssize_t last_row() const {
        return last_logical_row_;
    }

    void check_magic() const {
        magic_.check();
    }

    void unsparsify(size_t num_rows) {
        if(!sparse_map_)
            return;

        type_.visit_tag([that=this, num_rows] (const auto tdt) {
            using TagType = decltype(tdt);
            using RawType = typename TagType::DataTypeTag::raw_type;
            const auto dest_bytes = num_rows * sizeof(RawType);
            auto dest = ChunkedBuffer::presized(dest_bytes);
            util::default_initialize<TagType>(dest.data(), dest_bytes);
            util::expand_dense_buffer_using_bitmap<RawType>(that->sparse_map_.value(), that->data_.buffer().data(), dest.data());
            std::swap(dest, that->data_.buffer());
        });
        sparse_map_ = std::nullopt;
        last_logical_row_ = last_physical_row_ = static_cast<ssize_t>(num_rows) - 1;

        ARCTICDB_DEBUG(log::version(), "Unsparsify: last_logical_row_: {} last_physical_row_: {}", last_logical_row_, last_physical_row_);
    }

    void sparsify() {
        type().visit_tag([that=this](auto type_desc_tag) {
            using TDT = decltype(type_desc_tag);
            using RawType = typename TDT::DataTypeTag::raw_type;
            if constexpr(is_floating_point_type(TDT::DataTypeTag::data_type)) {
                auto raw_ptr =reinterpret_cast<const RawType*>(that->ptr());
                auto buffer = util::scan_floating_point_to_sparse(raw_ptr, that->row_count(), that->sparse_map());
                std::swap(that->data().buffer(), buffer);
                that->last_physical_row_ = that->sparse_map().count() - 1;
            }
        });
    }

    void string_array_prologue(ssize_t row_offset, size_t num_strings) {
        util::check_arg(last_logical_row_ + 1 == row_offset, "string_array_prologue expected row {}, actual {} ", last_logical_row_ + 1, row_offset);
        shapes_.ensure<shape_t>();
        auto shape_cursor = reinterpret_cast<shape_t *>(shapes_.ptr());
        *shape_cursor = shape_t(num_strings);
        data_.ensure<StringPool::offset_t>(num_strings);
    }

    void string_array_epilogue(size_t num_strings) {
        data_.commit();
        shapes_.commit();
        update_offsets(num_strings * sizeof(StringPool::offset_t));
        ++last_logical_row_;
    }

    void set_string_array(ssize_t row_offset,
                          size_t string_size,
                          size_t num_strings,
                          char *input,
                          StringPool &string_pool) {
        string_array_prologue(row_offset, num_strings);
        auto data_ptr = reinterpret_cast<StringPool::offset_t*>(data_.ptr());
        for (size_t i = 0; i < num_strings; ++i) {
            auto off = string_pool.get(std::string_view(input, string_size));
            *data_ptr++ = off.offset();
            input += string_size;
        }
        string_array_epilogue(num_strings);
    }

    void set_string_list(ssize_t row_offset, const std::vector<std::string> &input, StringPool &string_pool) {
        string_array_prologue(row_offset, input.size());
        auto data_ptr = reinterpret_cast<StringPool::offset_t *>(data_.ptr());
        for (const auto &str : input) {
            auto off = string_pool.get(str.data());
            *data_ptr++ = off.offset();
        }
        string_array_epilogue(input.size());
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

    void append_sparse_map(const util::BitMagic& bv, position_t at_row);

    void append(const Column& other, position_t at_row);

    void sort_external(const JiveTable& jive_table);

    void mark_absent_rows(size_t num_rows) {
        if(sparse_permitted()) {
            if(!sparse_map_){
                if (last_physical_row_ != -1)
                    backfill_sparse_map(last_physical_row_);
                else
                    sparse_map();
            }
            last_logical_row_ += static_cast<ssize_t>(num_rows);
        } else {
            util::check(last_logical_row_ == last_physical_row_, "Expected logical and physical rows to be equal in non-sparse column");
            default_initialize_rows(last_logical_row_ + 1, num_rows, true);
        }
    }

    void default_initialize_rows(size_t start_pos, size_t num_rows, bool ensure_alloc) {
        type_.visit_tag([that=this, start_pos, num_rows, ensure_alloc](auto tag) {
            using T= std::decay_t<decltype(tag)>;
            using RawType = typename T::DataTypeTag::raw_type;
            const auto bytes = (num_rows * sizeof(RawType));

            if(ensure_alloc)
                that->data_.ensure<uint8_t>(bytes);

            auto type_ptr = that->data_.ptr_cast<RawType>(start_pos, bytes);
            util::default_initialize<T>(reinterpret_cast<uint8_t*>(type_ptr), bytes);

            if(ensure_alloc)
                that->data_.commit();

            that->last_logical_row_ += static_cast<ssize_t>(num_rows);
            that->last_physical_row_ += static_cast<ssize_t>(num_rows);
        });
    }

    void set_row_data(size_t row_id) {
        last_logical_row_ = row_id;
        const auto last_stored_row = row_count() - 1;
        if(sparse_map_) {
            last_physical_row_ = sparse_map_.value().count() - 1;
        }
        else if (last_logical_row_ != last_stored_row) {
            last_physical_row_ = last_stored_row;
            backfill_sparse_map(last_stored_row);
        } else {
            last_physical_row_ = last_logical_row_;
        }
        ARCTICDB_TRACE(log::version(), "Set row data: last_logical_row_: {}, last_physical_row_: {}", last_logical_row_, last_physical_row_);
    }

    size_t get_physical_offset(size_t row) const {
        if(!is_sparse())
            return row;

        if(row == 0u)
            return 0u;

        // TODO: cache index
        std::unique_ptr<bm::bvector<>::rs_index_type> rs(new bm::bvector<>::rs_index_type());
        sparse_map().build_rs_index(rs.get());
        return sparse_map().count_to(bv_size(row - 1), *rs);
    }

    void set_sparse_map(util::BitSet&& bitset) {
        sparse_map_ = std::move(bitset);
    }

    std::optional<position_t> get_physical_row(position_t row) const {
        if(row > last_logical_row_) {
            if(sparse_permitted())
                return std::nullopt;
            else
                util::raise_rte("Scalar index {} out of bounds in column of size {}", row, row_count());
        }

        util::check_arg(is_scalar(), "get_scalar requested on non-scalar column");
        if(is_sparse() && !sparse_map().get_bit(bv_size(row)))
            return std::nullopt;

        return get_physical_offset(row);
    }

    template<typename T>
    std::optional<T> scalar_at(position_t row) const {
        auto physical_row = get_physical_row(row);
        if(!physical_row)
            return std::nullopt;

        return *data_.buffer().ptr_cast<T>(bytes_offset(physical_row.value()), sizeof(T));
    }

    bool has_value_at(position_t row) const {
        return !is_sparse() || sparse_map().get_bit(bv_size(row));
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
                reinterpret_cast<const T*>(data_.buffer().ptr_cast<uint8_t>(bytes_offset(idx), calc_elements(shape_ptr, ndim))));
    }

    void set_allow_sparse(bool value) {
        allow_sparse_ = value;
    }

    void set_shapes_buffer(size_t row_count) {
        CursoredBuffer<Buffer> shapes;
        shapes.ensure<shape_t>();
        shape_t rc(row_count);
        memcpy(shapes.ptr(), &rc, sizeof(shape_t));
        shapes.commit();
        swap(shapes_, shapes);
    }

    // The following two methods inflate (reduplicate) numpy string arrays that are potentially multi-dimensional,
    // i.e where the value is not a string but an array of strings
    void inflate_string_array(
            const TensorType<OffsetString::offset_t> &string_refs,
            CursoredBuffer<ChunkedBuffer> &data,
            CursoredBuffer<Buffer> &shapes,
            std::vector<position_t> &offsets,
            const StringPool &string_pool) {
        ssize_t max_size = 0;
        for (int i = 0; i < string_refs.size(); ++i)
            max_size = std::max(max_size, static_cast<ssize_t>(string_pool.get_const_view(string_refs.at(i)).size()));

        size_t data_size = static_cast<size_t>(max_size) * string_refs.size();
        data.ensure<uint8_t >(data_size);
        shapes.ensure<shape_t>();
        auto str_data = data.cursor();
        memset(str_data, 0, data_size);
        for (int i = 0; i < string_refs.size(); ++i) {
            auto str = string_pool.get_const_view(string_refs.at(i));
            memcpy(&str_data[i * max_size], str.data(), str.size());
        }

        offsets.push_back(data_size);
        shape_t s = string_refs.size();
        memcpy(shapes.cursor(), &s, sizeof(shape_t));
        data.commit();
        shapes.commit();
    }

    void inflate_string_arrays(const StringPool &string_pool) {
        util::check_arg(is_fixed_string_type(type().data_type()), "Can only inflate fixed string array types");
        util::check_arg(type().dimension() == Dimension::Dim1, "Fixed string inflation is for array types only");

        CursoredBuffer<ChunkedBuffer> data;
        CursoredBuffer<Buffer> shapes;
        std::vector<position_t> offsets;
        for (position_t row = 0; row < row_count(); ++row) {
          auto string_refs = tensor_at<OffsetString::offset_t>(row).value();
          inflate_string_array(string_refs, data, shapes, offsets, string_pool);
        }

        using std::swap;
        swap(shapes_, shapes);
        swap(offsets_, offsets);
        swap(data_, data);
        inflated_ = true;
    }

    // Used when the column has been inflated externally, i.e. because it has be done
    // in a pipeline of tiled sub-segments
    void set_inflated(size_t inflated_count) {
        set_shapes_buffer(inflated_count);
        inflated_ = true;
    }

    bool is_inflated() const {
        return inflated_;
    }

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

    template<typename T>
    const T *ptr_cast(position_t idx, size_t required_bytes) const {
        return data_.buffer().ptr_cast<T>(bytes_offset(idx), required_bytes);
    }

    template<typename T>
    T *ptr_cast(position_t idx, size_t required_bytes) {
        return const_cast<T*>(const_cast<const Column*>(this)->ptr_cast<T>(idx, required_bytes));
    }

    void change_type(DataType target_type) {
        util::check(shapes_.empty(), "Can't change type on multi-dimensional column with type {}", type_);
        if(type_.data_type() == target_type)
            return;

        CursoredBuffer<ChunkedBuffer> buf;
        for(const auto& block : data_.buffer().blocks()) {
            details::visit_type(type_.data_type(), [&buf, &block, type=type_, target_type] (auto&& source_dtt) {
                using source_raw_type = typename std::decay_t<decltype(source_dtt)>::raw_type;
                details::visit_type(target_type, [&buf, &block, &type, target_type] (auto&& target_dtt) {
                    using target_raw_type = typename std::decay_t<decltype(target_dtt)>::raw_type;

                    if constexpr (!is_narrowing_conversion<source_raw_type, target_raw_type>() &&
                            !std::is_same_v<source_raw_type, bool>) {
                        auto num_values = block->bytes() / sizeof(source_raw_type);
                        buf.ensure<target_raw_type>(num_values);
                        auto src = reinterpret_cast<const source_raw_type *>(block->data());
                        auto dest = reinterpret_cast<target_raw_type *>(buf.cursor());
                        for (auto i = 0u; i < num_values; ++i)
                            dest[i] = target_raw_type(src[i]);
                    }
                    else {
                        util::raise_rte("Cannot narrow column type from {} to {}", type, target_type);
                    }
                });
            });
        }
        type_ = TypeDescriptor{target_type, type_.dimension()};
        std::swap(data_, buf);
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

    ChunkedBuffer::Iterator get_iterator() const {
        return {const_cast<ChunkedBuffer*>(&data_.buffer()), get_type_size(type_.data_type())};
    }

    position_t row_count() const {
        if(!is_scalar()) {
            // TODO check with strings as well
            return num_shapes() / shape_t(type_.dimension());
        }

        if(is_sequence_type(type().data_type()) && inflated_ && is_fixed_string_type(type().data_type()))
            return inflated_row_count();

        return data_.bytes() / size_t(item_size());
    }

    size_t bytes() const {
        return data_.bytes();
    }

    ColumnData data() const {
        return ColumnData(&data_.buffer(), &shapes_.buffer(), type_, sparse_map_ ? &sparse_map_.value() : nullptr);
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

    void set_secondary_type(TypeDescriptor type) {
        secondary_type_ = std::move(type);
    }

    bool has_secondary_type() const {
        return secondary_type_.has_value();
    }

    const TypeDescriptor& secondary_type() const {
        util::check(secondary_type_.has_value(), "Requesting secondary type in a column that does not have one");
        return *secondary_type_;
    }

    void compact_blocks() {
        data_.compact_blocks();
    }

    const auto& blocks() const {
        return data_.buffer().blocks();
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
    position_t last_offset() const {
        return offsets_.empty() ? 0 : *offsets_.rbegin();
    }

    void update_offsets(size_t nbytes) {
        offsets_.push_back(last_offset() + nbytes);
    }

    bool is_scalar() const {
        return type().dimension() == Dimension(0);
    }

    const shape_t *shape_index(position_t idx) const {
        if (is_scalar())
            return nullptr;

        return shapes_.buffer().ptr_cast<shape_t>(idx * size_t(type_.dimension()) * sizeof(shape_t), sizeof(shape_t));
    }

    position_t bytes_offset(position_t idx) const {
        regenerate_offsets();
        util::check_arg(idx < row_count(),
                        "bytes_offset index {} out of bounds in column of size {}",
                        idx,
                        row_count());

        if (idx == 0)
            return 0;

        if (is_scalar())
            return scalar_offset(idx);

        util::check(size_t(idx - 1) < offsets_.size(), "Offset {} out of range, only have {}", idx - 1, offsets_.size());
        return offsets_[idx - 1];
    }

    position_t scalar_offset(position_t idx) const {
        return idx * item_size();
    }

    size_t item_size() const {
        if(is_sequence_type(type().data_type()) && inflated_ && is_fixed_string_type(type().data_type())) {
            return data_.bytes() / inflated_row_count();
        }

        return get_type_size(type().data_type());
    }

    size_t inflated_row_count() const {
        // using the content of shapes since the item size
        // from the datatype is no longer reliable
        return *reinterpret_cast<const size_t*>(shapes_.data());
    }

    size_t num_shapes() const  {
        return shapes_.bytes() / sizeof(shape_t);
    }

    void set_sparse_bit_for_row(size_t sparse_location) {
        sparse_map()[bv_size(sparse_location)] = true;
    }

    util::BitMagic& sparse_map() {
        if(!sparse_map_)
            sparse_map_ = std::make_optional<util::BitMagic>(0);

        return sparse_map_.value();
    }

    bool empty() const {
        return row_count() == 0;
    }

    const util::BitMagic& sparse_map() const {
        util::check(static_cast<bool>(sparse_map_), "Expected sparse map when it was not set");
        return sparse_map_.value();
    }

    void regenerate_offsets() const {
        if (ARCTICDB_LIKELY(is_scalar() || !offsets_.empty()))
            return;

        position_t pos = 0;
        for (position_t i = 0, j = i + position_t(type_.dimension()); j < position_t(num_shapes());
             i = j, j += position_t(type_.dimension())) {
            auto num_elements =
                    position_t(std::accumulate(shape_index(i), shape_index(j), shape_t(1), std::multiplies<>()));
            auto offset = num_elements * get_type_size(type_.data_type());
            offsets_.push_back(pos + offset);
            pos += offset;
        }
    }

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
    // For types that are arrays or matrices, the member type
    std::optional<TypeDescriptor> secondary_type_;
    util::MagicNum<'D', 'C', 'o', 'l'> magic_;
};

template <typename T>
JiveTable create_jive_table(const Column& col) {
    JiveTable output(col.row_count());
    std::iota(std::begin(output.orig_pos_), std::end(output.orig_pos_), 0);
    std::iota(std::begin(output.sorted_pos_), std::end(output.sorted_pos_), 0);

    std::sort(std::begin(output.orig_pos_), std::end(output.orig_pos_),[&](const auto& a, const auto& b) -> bool {
        return col.template scalar_at<T>(a) < col.template scalar_at<T>(b);
    });

    std::sort(std::begin(output.sorted_pos_), std::end(output.sorted_pos_),[&](const auto& a, const auto& b) -> bool {
        return output.orig_pos_[a] < output.orig_pos_[b];
    });

    for(auto pos : folly::enumerate(output.sorted_pos_)) {
        if(pos.index != *pos) {
            output.unsorted_rows_.set(bv_size(pos.index), true);
            ++output.num_unsorted_;
        }
    }

    return output;
}

} //namespace arcticdb
