/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/pipeline/frame_data_wrapper.hpp>

#include <pybind11/pybind11.h>

namespace arcticdb::detail {

inline py::array array_at(const SegmentInMemory& frame, std::size_t col_pos, py::object& anchor)
{
    ARCTICDB_SAMPLE_DEFAULT(PythonOutputFrameArrayAt)
    if (frame.empty()) {
        return visit_field(frame.field(col_pos), [](auto&& tag) {
            using TypeTag = std::decay_t<decltype(tag)>;
            constexpr auto data_type = TypeTag::DataTypeTag::data_type;
            std::string dtype;
            py::array::ShapeContainer shapes{0};
            ssize_t esize = get_type_size(data_type);
            if constexpr (is_sequence_type(data_type)) {
                if (is_fixed_string_type(data_type)) {
                    dtype = data_type == DataType::ASCII_FIXED64 ? "<S0" : "<U0";
                    esize = 1;
                } else {
                    dtype = "O";
                }
            } else {
                constexpr auto dim = TypeTag::DimensionTag::value;
                util::check(dim == Dimension::Dim0, "Only scalars supported, {}", data_type);
                if constexpr (data_type == DataType::MICROS_UTC64) {
                    dtype = "datetime64[ns]";
                } else {
                    dtype = fmt::format("{}{:d}", get_dtype_specifier(data_type), esize);
                }
            }
            py::array::StridesContainer strides{esize};
            return py::array{py::dtype{dtype}, std::move(shapes), std::move(strides)};
        });
    }
    return visit_field(frame.field(col_pos), [&, frame = frame, col_pos = col_pos](auto&& tag) {
        using TypeTag = std::decay_t<decltype(tag)>;
        constexpr auto dt = TypeTag::DataTypeTag::data_type;
        auto& buffer = frame.column(col_pos).data().buffer();
        std::string dtype;
        std::vector<shape_t> shapes;
        shapes.push_back(frame.row_count());
        ssize_t esize = get_type_size(dt);
        if constexpr (is_sequence_type(dt)) {
            if (is_fixed_string_type(dt)) {
                esize = buffer.bytes() / frame.row_count();
                auto char_count = esize;
                if (dt == DataType::UTF_FIXED64) {
                    char_count /= UNICODE_WIDTH;
                }
                dtype = fmt::format((dt == DataType::ASCII_FIXED64 ? "<S{:d}" : "<U{:d}"), char_count);
            } else {
                dtype = "O";
            }
        } else {
            constexpr auto dim = TypeTag::DimensionTag::value;
            util::check(dim == Dimension::Dim0, "Only scalars supported, {}", frame.field(col_pos));
            if constexpr (dt == DataType::MICROS_UTC64) {
                dtype = "datetime64[ns]";
            } else {
                dtype = fmt::format("{}{:d}", get_dtype_specifier(dt), esize);
            }
        }
        std::vector<stride_t> strides;
        strides.push_back(esize);
        // Note how base is passed to the array to register the data owner.
        // It's especially important to keep the frame data object alive for as long as the array is alive
        // so that regular python ref counting logic handles the liveness
        return py::array(py::dtype{dtype}, std::move(shapes), std::move(strides), buffer.data(), anchor);
    });
}

inline std::shared_ptr<pipelines::FrameDataWrapper> initialize_array(const SegmentInMemory& frame, py::object& ref)
{
    auto output = std::make_shared<pipelines::FrameDataWrapper>(frame.fields().size());
    ARCTICDB_SAMPLE(InitializeArrays, 0);
    ARCTICDB_DEBUG(log::memory(), "Initializing arrays");
    util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    for (std::size_t c = 0; c < static_cast<size_t>(frame.fields().size()); ++c) {
        output->data_[c] = array_at(frame, c, ref);
    }
    util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    return output;
}

} // namespace arcticdb::detail
