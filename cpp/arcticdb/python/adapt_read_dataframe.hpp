/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/read_result.hpp>
#include <arcticdb/python/python_handler_data.hpp>

namespace arcticdb {

inline py::tuple adapt_read_df(ReadResult&& ret, std::pair<std::any&, OutputFormat>* const handler) {
    if (handler) {
        apply_global_refcounts(handler->first, handler->second);
    }
    auto pynorm = python_util::pb_to_python(ret.norm_meta);
    auto pyuser_meta = util::variant_match(
            ret.user_meta,
            [](const arcticdb::proto::descriptors::UserDefinedMetadata& metadata) -> py::object {
                return python_util::pb_to_python(metadata);
            },
            [](const std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>& metadatas) -> py::object {
                py::list py_metadatas;
                for (const auto& metadata: metadatas) {
                    py_metadatas.append(python_util::pb_to_python(metadata));
                }
                return py_metadatas;
            });
    auto multi_key_meta = python_util::pb_to_python(ret.multi_key_meta);
    return py::make_tuple(ret.item, std::move(ret.frame_data), pynorm, pyuser_meta, multi_key_meta, ret.multi_keys);
};

}