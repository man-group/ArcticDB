/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/read_result.hpp>

namespace arcticdb {

/**
 * @brief Adapt a C++ ReadResult to a Python ReadResult
 * @param ret The C++ ReadResult to adapt
 * @return A Python ReadResult
 */
inline auto adapt_read_df = [](ReadResult && ret) -> py::tuple{
    auto pynorm = python_util::pb_to_python(ret.norm_meta);
    auto pyuser_meta = python_util::pb_to_python(ret.user_meta);
    auto multi_key_meta = python_util::pb_to_python(ret.multi_key_meta);
    return py::make_tuple(ret.item, std::move(ret.frame_data), pynorm, pyuser_meta, multi_key_meta, ret.multi_keys);
};

/**
 * @brief Adapt a vector of C++ ReadResult to a Python list of ReadResult
 * @param ret The vector of C++ ReadResult to adapt
 * @return A Python list of ReadResult
 */
auto adapt_read_dfs = [](std::vector<ReadResult> && ret) -> py::list {
    py::list lst;
    for (auto &res: ret) {
        lst.append(adapt_read_df(std::move(res)));
    }
    return lst;
};

}