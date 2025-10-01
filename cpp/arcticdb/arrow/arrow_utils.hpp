/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <memory>
#include <string_view>
#include <vector>

// Anything that transitively includes sparrow.array.hpp takes ages to build the (unused by us) std::format impl
// So avoid including sparrow in headers where possible until this is resolved
namespace sparrow {
class array;
class record_batch;
} // namespace sparrow

namespace arcticdb {

class SegmentInMemory;
class Column;

std::vector<sparrow::array> arrow_arrays_from_column(const Column& column, std::string_view name);

std::shared_ptr<std::vector<sparrow::record_batch>> segment_to_arrow_data(SegmentInMemory& segment);

} // namespace arcticdb