/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <arcticdb/column_store/memory_segment.hpp>

#include <pybind11/numpy.h>

#include <cstddef>

namespace arcticdb::detail {

/**
 * Python-specific column utility: converts a column in SegmentInMemory to a NumPy array
 */
pybind11::array array_at(const SegmentInMemory& frame, size_t col_pos);

} // namespace arcticdb::detail
