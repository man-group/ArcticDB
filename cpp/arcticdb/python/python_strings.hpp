/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <any>

#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/util/type_handler.hpp>
#include <arcticdb/python/python_handler_data.hpp>

namespace arcticdb {

struct ColumnMapping;
class Column;
class StringPool;

inline PythonHandlerData& cast_handler_data(std::any& any) { return std::any_cast<PythonHandlerData&>(any); }

void write_python_dynamic_strings_to_dest(
        PyObject** ptr_dest, const Column& source_column, const ColumnMapping& mapping, const StringPool& string_pool,
        const DecodePathData& shared_data, PythonHandlerData& handler_data
);

} // namespace arcticdb
