/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <fmt/format.h>

#include <pybind11/numpy.h>

//TODO this class is bogus and not part of the long-term plan. If it's only used for
// testing and the toolbox then move it, otherwise get rid of it

namespace  py = pybind11;

namespace arcticdb {

class TickReader {
public:

    TickReader() = default;

    void add_segment(SegmentInMemory &segment) {
        segment_ = std::move(segment);
    }

    [[nodiscard]] size_t row_count() const {
        return segment_.row_count();
    }

    std::string_view view_at(entity::position_t o) {
        return segment_.string_pool().get_view(o);
    }

    py::buffer_info from_string_array( const Column::StringArrayData& data) const
    {
        std::vector<ssize_t> shapes { data.num_strings_};
        std::vector<ssize_t> strides { data.string_size_ };

        return py::buffer_info {
                (void *) data.data_,
                data.string_size_,
                std::string(fmt::format("{}{}", data.string_size_, 's')),
                ssize_t(Dimension::Dim1),
                shapes,
                strides
        };
    }

    py::tuple at(size_t row) {
        py::list res;
        for (std::size_t  col= 0; col < segment_.num_columns(); ++col) {
            const auto& type_desc = segment_.column_descriptor(col).type();
            type_desc.visit_tag([&](auto && impl){
                using T= std::decay_t<decltype(impl)>;
                using RawType = typename T::DataTypeTag::raw_type;
                if constexpr (T::DimensionTag::value == Dimension::Dim0){
                    if constexpr (T::DataTypeTag::data_type == DataType::ASCII_DYNAMIC64 || T::DataTypeTag::data_type == DataType::ASCII_FIXED64)
                    {
                        auto str = segment_.string_at(row, col).value();
                        res.append(str);
                    } else {
                        RawType v = segment_.scalar_at<RawType>(row, col).value(); // TODO handle sparse
                        res.append(v);
                    }
                } else {
                    if (T::DataTypeTag::data_type == DataType::ASCII_FIXED64)
                    {
                        auto str_arr = segment_.string_array_at(row, col).value();
                        res.append(py::array(from_string_array(str_arr)));
                    }
                    else if (T::DataTypeTag::data_type == DataType::ASCII_DYNAMIC64)
                    {
                        auto string_refs = segment_.tensor_at<entity::position_t>(row, col).value();
                        std::vector<std::string_view > output;
                        for(ssize_t i = 0; i < string_refs.size(); ++i )
                            output.emplace_back(view_at(string_refs.at(i)));

                        res.append(output);
                    }
                    else
                        res.append(convert::to_py_array(segment_.tensor_at<RawType>(row, col).value()));
                }
            });
        }
        return py::tuple(res);
    }

private:
    //util::BitSet is_set;
    SegmentInMemory segment_;
};

}
