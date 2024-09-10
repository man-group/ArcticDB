/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <sparrow/arrow_interface/arrow_schema/smart_pointers.hpp>
#include <sparrow/arrow_interface/arrow_array/smart_pointers.hpp>

namespace arcticdb {

struct ArrowData {
    ArrowData(
        sparrow::arrow_array_unique_ptr &&data,
        sparrow::arrow_schema_unique_ptr &&schema
    ) :
        data_(std::move(data)),
        schema_(std::move(schema)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(ArrowData)

    sparrow::arrow_array_unique_ptr data_;
    sparrow::arrow_schema_unique_ptr schema_;
};

} // namespace arcticdb