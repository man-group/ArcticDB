/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>

#include <arcticdb/arrow/include_sparrow.hpp>

namespace arcticdb {

struct ArrowData {
    ArrowData(sparrow::record_batch&& record_batch) :
        record_batch_(std::move(record_batch)) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(ArrowData)

    sparrow::record_batch record_batch_;
};

} // namespace arcticdb