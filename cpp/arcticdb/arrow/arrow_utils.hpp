/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <sparrow/record_batch.hpp>
#include <vector>

namespace arcticdb {

class SegmentInMemory;
class Column;

std::vector<sparrow::array> arrow_arrays_from_column(const Column& column, std::string_view name);

std::shared_ptr<std::vector<sparrow::record_batch>> segment_to_arrow_data(SegmentInMemory& segment);

} // namespace arcticdb