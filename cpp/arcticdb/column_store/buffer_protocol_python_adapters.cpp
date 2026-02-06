/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/buffer_protocol_python_adapters.hpp>
#include <fmt/format.h>

namespace arcticdb::python_util {

py::buffer_info string_pool_as_buffer_info(const StringPool& pool) {
    return py::buffer_info{
            (void*)pool.data().blocks().at(0)->data(),
            1,
            py::format_descriptor<char>::format(),
            ssize_t(pool.data().blocks().at(0)->bytes())
    };
}

py::buffer_info from_string_array(const Column::StringArrayData& data) {
    std::vector<ssize_t> shapes{data.num_strings_};
    std::vector<ssize_t> strides{data.string_size_};

    return py::buffer_info{
            (void*)data.data_,
            data.string_size_,
            std::string(fmt::format("{}{}", data.string_size_, 's')),
            ssize_t(Dimension::Dim1),
            shapes,
            strides
    };
}

} // namespace arcticdb::python_util
