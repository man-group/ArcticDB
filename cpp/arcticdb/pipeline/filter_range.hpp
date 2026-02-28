/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <variant>

#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>

namespace arcticdb {

// FilterRange: variant of no-filter, timestamp range, or row range.
// Used by the lazy read path (LazyRecordBatchIterator) and ReadQuery.
using FilterRange = std::variant<std::monostate, entity::IndexRange, pipelines::RowRange>;

} // namespace arcticdb
