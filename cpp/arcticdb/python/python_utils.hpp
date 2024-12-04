/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <pybind11/pybind11.h>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/data_error.hpp>
#include <arcticdb/entity/read_result.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/util/gil_safe_py_none.hpp>

namespace py = pybind11;

namespace arcticdb::python_util {

class ARCTICDB_VISIBILITY_HIDDEN PyRowRef : public py::tuple {
  PYBIND11_OBJECT_DEFAULT(PyRowRef, py::tuple, PyTuple_Check)

    explicit PyRowRef(const RowRef& row_ref) :
        py::tuple(row_ref.col_count()),
        row_ref_(row_ref) {
        // tuple is still mutable while ref count is 1
        py::list res;
        auto &segment = row_ref_.segment();
        segment.check_magic();
        auto row_pos = static_cast<position_t>(row_ref_.row_pos());
        for (position_t col = 0; col < position_t(segment.num_columns()); ++col) {
            visit_field(segment.column_descriptor(col), [this, &segment, &col, row_pos](auto impl) {
                using T= std::decay_t<decltype(impl)>;
                using RawType = typename T::DataTypeTag::raw_type;
                if constexpr (T::DimensionTag::value == Dimension::Dim0) {
                    if constexpr (T::DataTypeTag::data_type == DataType::ASCII_DYNAMIC64
                        || T::DataTypeTag::data_type == DataType::ASCII_FIXED64) {
                        set_col(col, segment.string_at(row_pos, col).value());
                    } else {
                        set_col(col, segment.scalar_at<RawType>(row_pos, col).value()); // TODO handle sparse
                    }
                } else {
                    // TODO handle utf too
                    if (T::DataTypeTag::data_type == DataType::ASCII_FIXED64) {
                        auto str_arr = segment.string_array_at(row_pos, col).value();
                        set_col(col, py::array(from_string_array(str_arr)));
                    } else if (T::DataTypeTag::data_type == DataType::ASCII_DYNAMIC64) {
                        auto string_refs = segment.tensor_at<entity::position_t>(row_pos, col).value();
                        std::vector<std::string_view> output;
                        for (ssize_t i = 0; i < string_refs.size(); ++i)
                            output.emplace_back(view_at(string_refs.at(i)));

                        set_col(col, output);
                    } else {
                        auto opt_tensor = segment.tensor_at<RawType>(row_pos, col);
                        if(opt_tensor.has_value()){
                            set_col(col, to_py_array(*opt_tensor));
                        }
                    }
                }
            });
        }
    }

  private:
    std::string_view view_at(entity::position_t o) {
        return row_ref_.segment().string_pool().get_view(o);
    }

    static py::buffer_info from_string_array(const Column::StringArrayData &data) {
        std::vector<ssize_t> shapes{data.num_strings_};
        std::vector<ssize_t> strides{data.string_size_};

        return py::buffer_info{
            (void *) data.data_,
            data.string_size_,
            std::string(fmt::format("{}{}", data.string_size_, 's')),
            ssize_t(Dimension::Dim1),
            shapes,
            strides
        };
    }
    template<class O>
    void set_col(std::size_t col, O &&o) const {
        (*this)[col] = std::forward<O>(o);
    }

    RowRef row_ref_;
};

enum IncrementRefCount {
    ON,
    OFF
};

inline void prefill_with_none(
    PyObject** ptr_dest,
    size_t num_rows,
    size_t sparse_count,
    SpinLock& spin_lock,
    IncrementRefCount inc_ref_count = IncrementRefCount::ON) {
    std::lock_guard lock(spin_lock);
    auto none = GilSafePyNone::instance();
    for (auto i = 0U; i < num_rows; ++i)
        *ptr_dest++ = none->ptr();

    if(inc_ref_count == IncrementRefCount::ON) {
        auto none_count = num_rows - sparse_count;
        for (auto j = 0U; j < none_count; ++j)
            Py_INCREF(none->ptr());
    }
    spin_lock.unlock();
}

template<typename Msg>
py::object pb_to_python(const Msg & out){
    std::string_view full_name = out.descriptor()->full_name();
    auto & name = out.descriptor()->name();
    std::string_view pkg_name = full_name.substr(0, full_name.size() - name.size());
    if(pkg_name[pkg_name.size()-1] == '.'){
        pkg_name = pkg_name.substr(0, pkg_name.size()-1);
    }

    auto py_pkg_obj = py::module::import(std::string(pkg_name).data());
    auto PyMsg = py_pkg_obj.attr(name.data());
    py::object res = PyMsg();
    std::string s;
    out.SerializeToString(&s);
    res.attr("ParseFromString")(py::bytes(s));
    return res;
}

template<typename Msg>
void pb_from_python(const py::object & py_msg, Msg & out){
    auto s = py_msg.attr("SerializeToString")().cast<std::string>();
    out.ParseFromString(s);
}

/**
 * Register __repr__ string representation by piggy backing on fmt::format
 * @tparam PyClass type of class binding
 * @param py_class class binding under construction
 * @return the reference passed in (to support fluent like api)
 */
template<class PyClass>
PyClass & add_repr(PyClass & py_class){
    py_class.def("__repr__",[](const typename PyClass::type & a){
        return fmt::format("{}", a);
    });
    return py_class;
}

inline py::object &pd_Timestamp() {
    static py::object T = py::module::import("pandas").attr("Timestamp");
    return T;
}

inline bool from_pd_timestamp(const py::object &o, timestamp &ts) {
    if (py::isinstance(o, pd_Timestamp())) {
        ts = o.attr("value").cast<timestamp>();
        return true;
    }
    // TODO manage absence of pandas
    return false;
}

inline py::object &dt_datetime() {
    static py::object T = py::module::import("datetime").attr("datetime");
    return T;
}

inline bool from_datetime(const py::object &o, timestamp &ts) {
    if (py::isinstance(o, dt_datetime())) {
        auto pd_ts = pd_Timestamp()(o);
        return from_pd_timestamp(pd_ts, ts);
    }
    return false;
}

inline py::object &np_datetime64() {
    static py::object T = py::module::import("numpy").attr("datetime64");
    return T;
}

inline bool from_dt64(const py::object &o, timestamp &ts) {
    if (py::isinstance(o, np_datetime64())) {
        // NOTE: this is safe as of Pandas < 2.0 because `datetime64` _always_ has been using nanosecond resolution,
        // i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other resolution.
        // Yet, this has changed in Pandas 2.0 and other resolution can be used,
        // i.e. Pandas >= 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`.
        // See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution
        // TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and do not
        // rely uniquely on the resolution-less 'M' specifier if it this doable.
        ts = o.attr("astype")("datetime64[ns]").attr("astype")("uint64").cast<timestamp>();
        return true;
    }
    return false;
}

inline timestamp py_convert_type(const py::object &convertible) {
    timestamp ts = 0;
    if (from_dt64(convertible, ts)) return ts;
    if (from_pd_timestamp(convertible, ts)) return ts;
    if (from_datetime(convertible, ts)) return ts;
    return convertible.cast<timestamp>();
}

class PyTimestampRange {
  public:
    PyTimestampRange(const py::object &start, const py::object &end) :
        start_(py_convert_type(start)), end_(py_convert_type(end)) {
        util::check_arg(start_ <= end_, "expected star <= end, actual {}, {}", start_, end_);
    }

    explicit operator entity::TimestampRange() const {
        return {start_, end_};
    }

    [[nodiscard]] timestamp start_nanos_utc() const { return start_; }
    [[nodiscard]] timestamp end_nanos_utc() const { return end_; }

  private:
    timestamp start_;
    timestamp end_;
};

inline py::list adapt_read_dfs(std::vector<std::variant<ReadResult, DataError>>&& r) {
    auto ret = std::move(r);
    py::list lst;
    for (auto &res: ret) {
        util::variant_match(
            res,
            [&lst] (ReadResult& read_result) {
                auto pynorm = python_util::pb_to_python(read_result.norm_meta);
                auto pyuser_meta = python_util::pb_to_python(read_result.user_meta);
                auto multi_key_meta = python_util::pb_to_python(read_result.multi_key_meta);
                lst.append(py::make_tuple(read_result.item, std::move(read_result.frame_data), pynorm, pyuser_meta, multi_key_meta,
                                          read_result.multi_keys));
            },
            [&lst] (DataError& data_error) {
                lst.append(data_error);
            }
        );
    }
    return lst;
}

// aggregations is a dict similar to that accepted by Pandas agg method
// The key-value pairs come in 2 forms:
// 1: key is the column name to aggregate, value is the aggregation operator. Output column name will be the same as input column name
// 2: key is the column name to output, value is a pair where the first element is the input column name, and the second element is the aggregation operator
// These 2 styles can be mixed and matched
inline std::vector<NamedAggregator> named_aggregators_from_dict(const std::unordered_map<std::string, std::variant<std::string, std::pair<std::string, std::string>>>& aggregations) {
    std::vector<NamedAggregator> named_aggregators;
    for (const auto& [output_column_name, var_agg_named_agg]: aggregations) {
        util::variant_match(
                var_agg_named_agg,
                [&named_aggregators, &output_column_name] (const std::string& agg_operator) {
                    named_aggregators.emplace_back(agg_operator, output_column_name, output_column_name);
                },
                [&named_aggregators, &output_column_name] (const std::pair<std::string, std::string>& input_col_and_agg) {
                    named_aggregators.emplace_back(input_col_and_agg.second, input_col_and_agg.first, output_column_name);
                }
        );
    }
    return named_aggregators;
}

inline auto pd_to_offset(std::string_view rule) {
    static py::object to_offset = py::module::import("pandas").attr("tseries").attr("frequencies").attr("to_offset");
    return to_offset(rule).attr("nanos").cast<timestamp>();
}

} // namespace arcticdb::python_util
