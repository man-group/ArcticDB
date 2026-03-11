/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/string_pool_utils.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/python_handler_data.hpp>

namespace arcticdb {

inline PythonHandlerData& cast_handler_data(std::any& any) { return std::any_cast<PythonHandlerData&>(any); }

} // namespace arcticdb
