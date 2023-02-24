/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/read_result.hpp>

namespace arcticdb {

inline auto adapt_read_df = [](ReadResult && ret) -> py::tuple{
    auto pynorm = python_util::pb_to_python(ret.norm_meta);
    auto pyuser_meta = python_util::pb_to_python(ret.user_meta);
    auto multi_key_meta = python_util::pb_to_python(ret.multi_key_meta);
    return py::make_tuple(ret.item, std::move(ret.frame_data), pynorm, pyuser_meta, multi_key_meta, ret.multi_keys);
};

}