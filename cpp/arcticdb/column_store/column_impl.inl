/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column.hpp>
namespace arcticdb {

template<typename T>
std::optional<T> Column::scalar_at(position_t row) const {
    auto physical_row = get_physical_row(row);
    if(!physical_row)
        return std::nullopt;
    return *data_.buffer().ptr_cast<T>(bytes_offset(*physical_row), sizeof(T));
}

template<typename T>
std::vector<T> Column::clone_scalars_to_vector() const {
    auto values = std::vector<T>();
    values.reserve(row_count());
    const auto& buffer = data_.buffer();
    for (auto i=0u; i<row_count(); ++i){
        values.push_back(*buffer.ptr_cast<T>(i*item_size(), sizeof(T)));
    }
    return values;
}

template<typename T>
requires std::integral<T> || std::floating_point<T>
void Column::set_scalar(ssize_t row_offset, T val) {
    util::check(
        sizeof(T) == get_type_size(type_.data_type()),
        "Type mismatch in set_scalar, expected {} byte scalar got {} byte scalar",
        get_type_size(type_.data_type()),
        sizeof(T)
    );

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
void Column::push_back(T val) {
    set_scalar(last_logical_row_ + 1, val);
}

template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int>>
inline void Column::set_external_block(ssize_t row_offset, T *val, size_t size) {
    util::check_arg(last_logical_row_ + 1 == row_offset, "set_external_block expected row {}, actual {} ", last_logical_row_ + 1, row_offset);
    auto bytes = sizeof(T) * size;
    const_cast<ChunkedBuffer&>(data_.buffer()).add_external_block(reinterpret_cast<const uint8_t*>(val), bytes, data_.buffer().last_offset());
    last_logical_row_ += static_cast<ssize_t>(size);
    last_physical_row_ = last_logical_row_;
}

template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int>>
inline void Column::set_sparse_block(ssize_t row_offset, T *ptr, size_t rows_to_write) {
    util::check(row_offset == 0, "Cannot write sparse column with existing data");
    auto new_buffer = util::scan_floating_point_to_sparse(ptr, rows_to_write, sparse_map());
    std::swap(data_.buffer(), new_buffer);
}

template<class T, template<class> class Tensor, std::enable_if_t<
        std::is_integral_v<T> || std::is_floating_point_v<T>,
        int>>
void Column::set_array(ssize_t row_offset, Tensor<T> &val) {
    ARCTICDB_SAMPLE(ColumnSetArray, RMTSF_Aggregate)
    magic_.check();
    util::check_arg(last_logical_row_ + 1 == row_offset, "set_array expected row {}, actual {} ", last_logical_row_ + 1, row_offset);
    data_.ensure_bytes(val.nbytes());
    shapes_.ensure<shape_t>(val.ndim());
    memcpy(shapes_.cursor(), val.shape(), val.ndim() * sizeof(shape_t));
    auto info = val.request();
    util::FlattenHelper flatten(val);
    auto data_ptr = reinterpret_cast<T*>(data_.cursor());
    flatten.flatten(data_ptr, reinterpret_cast<const T *>(info.ptr));
    update_offsets(val.nbytes());
    data_.commit();
    shapes_.commit();
    ++last_logical_row_;
}

template<class T, std::enable_if_t< std::is_integral_v<T> || std::is_floating_point_v<T>, int>>
void Column::set_array(ssize_t row_offset, py::array_t<T>& val) {
    ARCTICDB_SAMPLE(ColumnSetArray, RMTSF_Aggregate)
    magic_.check();
    util::check_arg(last_logical_row_ + 1 == row_offset, "set_array expected row {}, actual {} ", last_logical_row_ + 1, row_offset);
    data_.ensure_bytes(val.nbytes());
    shapes_.ensure<shape_t>(val.ndim());
    memcpy(shapes_.cursor(), val.shape(), val.ndim() * sizeof(shape_t));
    auto info = val.request();
    util::FlattenHelper<T, py_array_t> flatten(val);
    auto data_ptr = reinterpret_cast<T*>(data_.cursor());
    flatten.flatten(data_ptr, reinterpret_cast<const T*>(info.ptr));
    update_offsets(val.nbytes());
    data_.commit();
    shapes_.commit();
    ++last_logical_row_;
}

template<typename T>
T& Column::reference_at(position_t row)  {
    util::check_arg(row < row_count(), "Scalar reference index {} out of bounds in column of size {}", row, row_count());
    util::check_arg(is_scalar(), "get_reference requested on non-scalar column");
    return *data_.buffer().ptr_cast<T>(bytes_offset(row), sizeof(T));
}

template<typename T>
std::optional<TensorType<T>> Column::tensor_at(position_t idx) const {
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
const T *Column::ptr_cast(position_t idx, size_t required_bytes) const {
    return data_.buffer().ptr_cast<T>(bytes_offset(idx), required_bytes);
}

template<typename T>
T *Column::ptr_cast(position_t idx, size_t required_bytes) {
    return const_cast<T*>(const_cast<const Column*>(this)->ptr_cast<T>(idx, required_bytes));
}

template<typename T>
std::optional<position_t> Column::search_unsorted(T val) const {
    util::check_arg(is_scalar(), "Cannot index on multidimensional values");
    for (position_t i = 0; i < row_count(); ++i) {
        if (val == *ptr_cast<T>(i, sizeof(T)))
            return i;
    }
    return std::nullopt;
}

template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int>>
size_t Column::search_sorted(T val, bool from_right, std::optional<int64_t> from, std::optional<int64_t> to) const {
    // There will not necessarily be a unique answer for sparse columns
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(!is_sparse(),
                                                    "Column::search_sorted not supported with sparse columns");
    std::optional<size_t> res;
    auto column_data = data();
    details::visit_type(type().data_type(), [this, &res, &column_data, val, from_right, &from, &to](auto type_desc_tag) {
        using type_info = ScalarTypeInfo<decltype(type_desc_tag)>;
        auto accessor = random_accessor<typename type_info::TDT>(&column_data);
        if constexpr(std::is_same_v<T, typename type_info::RawType>) {
            int64_t low = from.value_or(0);
            int64_t high = to.value_or(row_count() - 1);
            while (!res.has_value()) {
                auto mid{low + (high - low) / 2};
                auto mid_value = accessor.at(mid);
                if (val == mid_value) {
                    // At least one value in the column exactly matches the input val
                    // Search to the right/left for the last/first such value
                    if (from_right) {
                        while (++mid <= high && val == accessor.at(mid)) {}
                        res = mid;
                    } else {
                        while (--mid >= low && val == accessor.at(mid)) {}
                        res = mid + 1;
                    }
                } else if (val > mid_value) {
                    if (mid + 1 <= high && val >= accessor.at(mid + 1)) {
                        // Narrow the search interval
                        low = mid + 1;
                    } else {
                        // val is less than the next value, so we have found the right interval
                        res = mid + 1;
                    }
                } else { // val < mid_value
                    if (mid - 1 >= low && val <= accessor.at(mid + 1)) {
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

template <
        typename input_tdt,
        typename functor>
requires std::is_invocable_r_v<void, functor, typename input_tdt::DataTypeTag::raw_type>
void Column::for_each(const Column& input_column, functor&& f) {
    auto input_data = input_column.data();
    std::for_each(input_data.cbegin<input_tdt>(), input_data.cend<input_tdt>(), std::forward<functor>(f));
}

template <
        typename input_tdt,
        typename functor>
requires std::is_invocable_r_v<void, functor, typename ColumnData::Enumeration<typename input_tdt::DataTypeTag::raw_type>>
void Column::for_each_enumerated(const Column& input_column, functor&& f) {
    auto input_data = input_column.data();
    if (input_column.is_sparse()) {
        std::for_each(input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>(), input_data.cend<input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>(),
                      std::forward<functor>(f));
    } else {
        std::for_each(input_data.cbegin<input_tdt, IteratorType::ENUMERATED, IteratorDensity::DENSE>(), input_data.cend<input_tdt, IteratorType::ENUMERATED, IteratorDensity::DENSE>(),
                      std::forward<functor>(f));
    }
}

template <
        typename input_tdt,
        typename output_tdt,
        typename functor>
requires std::is_invocable_r_v<typename output_tdt::DataTypeTag::raw_type, functor, typename input_tdt::DataTypeTag::raw_type>
void Column::transform(const Column& input_column, Column& output_column, functor&& f) {
    auto input_data = input_column.data();
    initialise_output_column(input_column, output_column);
    auto output_data = output_column.data();
    std::transform(
        input_data.cbegin<input_tdt>(),
        input_data.cend<input_tdt>(),
        output_data.begin<output_tdt>(),
        std::forward<functor>(f)
    );
}

template<
        typename left_input_tdt,
        typename right_input_tdt,
        typename output_tdt,
        typename functor>
requires std::is_invocable_r_v<
        typename output_tdt::DataTypeTag::raw_type,
        functor,
        typename left_input_tdt::DataTypeTag::raw_type,
        typename right_input_tdt::DataTypeTag::raw_type>
void Column::transform(const Column& left_input_column,
                      const Column& right_input_column,
                      Column& output_column,
                      functor&& f) {
    auto left_input_data = left_input_column.data();
    auto right_input_data = right_input_column.data();
    initialise_output_column(left_input_column, right_input_column, output_column);
    auto output_data = output_column.data();
    auto output_it = output_data.begin<output_tdt>();

    if (!left_input_column.is_sparse() && !right_input_column.is_sparse()) {
        // Both dense, use std::transform over the shorter column to avoid going out-of-bounds
        if (left_input_column.row_count() <= right_input_column.row_count()) {
            std::transform(left_input_data.cbegin<left_input_tdt>(),
                           left_input_data.cend<left_input_tdt>(),
                           right_input_data.cbegin<right_input_tdt>(),
                           output_it,
                           std::forward<functor>(f));
        } else {
            std::transform(right_input_data.cbegin<left_input_tdt>(),
                           right_input_data.cend<left_input_tdt>(),
                           left_input_data.cbegin<right_input_tdt>(),
                           output_it,
                           std::forward<functor>(f));
        }
    } else if (left_input_column.is_sparse() && right_input_column.is_sparse()) {
        auto left_it = left_input_data.cbegin<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        auto right_it = right_input_data.cbegin<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        auto end_bit = output_column.sparse_map().end();
        for (auto set_bit = output_column.sparse_map().first(); set_bit < end_bit; ++set_bit) {
            const auto idx = *set_bit;
            while (left_it->idx() != idx) {
                ++left_it;
            }
            while (right_it->idx() != idx) {
                ++right_it;
            }
            *output_it++ = f(left_it->value(), right_it->value());
        }
    } else if (left_input_column.is_sparse() && !right_input_column.is_sparse()) {
        // One sparse, one dense. Use the enumerating forward iterator over the sparse column as it is more efficient than random access
        auto right_accessor = random_accessor<right_input_tdt>(&right_input_data);
        const auto right_column_row_count = right_input_column.row_count();
        const auto left_input_data_cend = left_input_data.cend<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto left_it = left_input_data.cbegin<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
             left_it != left_input_data_cend && left_it->idx() < right_column_row_count;
             ++left_it) {
            *output_it++ = f(left_it->value(), right_accessor.at(left_it->idx()));
        }
    } else if (!left_input_column.is_sparse() && right_input_column.is_sparse()) {
        // One sparse, one dense. Use the enumerating forward iterator over the sparse column as it is more efficient than random access
        auto left_accessor = random_accessor<left_input_tdt>(&left_input_data);
        const auto left_column_row_count = left_input_column.row_count();
        const auto right_input_data_cend = right_input_data.cend<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto right_it = right_input_data.cbegin<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
             right_it != right_input_data_cend && right_it->idx() < left_column_row_count;
             ++right_it) {
            *output_it++ = f(left_accessor.at(right_it->idx()), right_it->value());
        }
    }
}

template <
        typename input_tdt,
        std::predicate<typename input_tdt::DataTypeTag::raw_type> functor>
void Column::transform(const Column& input_column,
                      util::BitSet& output_bitset,
                      bool sparse_missing_value_output,
                      functor&& f) {
    if (input_column.is_sparse()) {
        initialise_output_bitset(input_column, sparse_missing_value_output, output_bitset);
    } else {
        // This allows for empty/full result optimisations, technically bitsets are always dynamically sized
        output_bitset.resize(input_column.row_count());
    }
    util::BitSet::bulk_insert_iterator inserter(output_bitset);
    Column::for_each_enumerated<input_tdt>(input_column, [&inserter, f = std::forward<functor>(f)](auto enumerated_it) {
        if (f(enumerated_it.value())) {
            inserter = enumerated_it.idx();
        }
    });
    inserter.flush();
}

template <
        typename left_input_tdt,
        typename right_input_tdt,
        std::relation<typename left_input_tdt::DataTypeTag::raw_type, typename right_input_tdt::DataTypeTag::raw_type> functor>
void Column::transform(const Column& left_input_column,
                      const Column& right_input_column,
                      util::BitSet& output_bitset,
                      bool sparse_missing_value_output,
                      functor&& f) {
    auto left_input_data = left_input_column.data();
    auto right_input_data = right_input_column.data();
    util::check(left_input_column.last_row() == right_input_column.last_row(),
                "Mismatching logical column lengths in Column::transform");
    util::BitSet::bulk_insert_iterator inserter(output_bitset);

    if (!left_input_column.is_sparse() && !right_input_column.is_sparse()) {
        // Both dense, use std::for_each over the shorter column to avoid going out-of-bounds
        auto rows = std::max(left_input_column.last_row(), right_input_column.last_row()) + 1;
        output_bitset.resize(rows);
        if (sparse_missing_value_output && left_input_column.row_count() != right_input_column.row_count()) {
            // Dense columns of different lengths, and missing values should be on in the output bitset
            output_bitset.set_range(std::min(left_input_column.last_row(), right_input_column.last_row()) + 1, rows - 1);
        }
        auto pos = 0u;
        if (left_input_column.row_count() <= right_input_column.row_count()) {
            auto right_it = right_input_data.cbegin<right_input_tdt>();
            std::for_each(
                    left_input_data.cbegin<left_input_tdt>(),
                    left_input_data.cend<left_input_tdt>(),
                    [&right_it, &inserter, &pos, f = std::forward<functor>(f)](auto left_value) {
                        if (f(left_value, *right_it++)) {
                            inserter = pos;
                        }
                        ++pos;
                    });
        } else {
            auto left_it = left_input_data.cbegin<left_input_tdt>();
            std::for_each(
                    right_input_data.cbegin<right_input_tdt>(),
                    right_input_data.cend<right_input_tdt>(),
                    [&left_it, &inserter, &pos, f = std::forward<functor>(f)](auto right_value) {
                        if (f(*left_it++, right_value)) {
                            inserter = pos;
                        }
                        ++pos;
                    });
        }
    } else if (left_input_column.is_sparse() && right_input_column.is_sparse()) {
        // Both sparse, only check the intersection of on-bits from both sparse maps
        auto bits_to_check = left_input_column.sparse_map() & right_input_column.sparse_map();
        if (sparse_missing_value_output) {
            output_bitset = bits_to_check;
            output_bitset.flip();
        }
        // Both columns should have the same number of logical rows, so just use one of them
        output_bitset.resize(left_input_column.last_row() + 1);
        auto left_accessor = random_accessor<left_input_tdt>(&left_input_data);
        auto right_accessor = random_accessor<right_input_tdt>(&right_input_data);
        // TODO: experiment with more efficient bitset traversal methods
        // https://github.com/tlk00/BitMagic/tree/master/samples/bvsample25
        auto end_bit = bits_to_check.end();
        for (auto set_bit = bits_to_check.first(); set_bit < end_bit; ++set_bit) {
            if(f(left_accessor.at(*set_bit), right_accessor.at(*set_bit))) {
                inserter = *set_bit;
            }
        }
    } else if (left_input_column.is_sparse() && !right_input_column.is_sparse()) {
        // One sparse, one dense. Use the enumerating forward iterator over the sparse column as it is more efficient than random access
        initialise_output_bitset(left_input_column, sparse_missing_value_output, output_bitset);
        auto right_accessor = random_accessor<right_input_tdt>(&right_input_data);
        const auto right_column_row_count = right_input_column.row_count();
        const auto left_input_data_cend = left_input_data.cend<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto left_it = left_input_data.cbegin<left_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
             left_it != left_input_data_cend && left_it->idx() < right_column_row_count;
             ++left_it) {
            if(f(left_it->value(), right_accessor.at(left_it->idx()))) {
                inserter = left_it->idx();
            }
        }
    } else if (!left_input_column.is_sparse() && right_input_column.is_sparse()) {
        // One sparse, one dense. Use the enumerating forward iterator over the sparse column as it is more efficient than random access
        initialise_output_bitset(right_input_column, sparse_missing_value_output, output_bitset);
        auto left_accessor = random_accessor<left_input_tdt>(&left_input_data);
        const auto left_column_row_count = left_input_column.row_count();
        const auto right_input_data_cend = right_input_data.cend<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
        for (auto right_it = right_input_data.cbegin<right_input_tdt, IteratorType::ENUMERATED, IteratorDensity::SPARSE>();
             right_it != right_input_data_cend && right_it->idx() < left_column_row_count;
             ++right_it) {
            if(f(left_accessor.at(right_it->idx()), right_it->value())) {
                inserter = right_it->idx();
            }
        }
    }
    inserter.flush();
}

} // namespace arcticdb