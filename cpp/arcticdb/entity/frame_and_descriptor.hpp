/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>

namespace arcticdb {

struct FrameAndDescriptor {
    SegmentInMemory frame_;
    TimeseriesDescriptor desc_;
    std::vector<AtomKey> keys_;
};

} // namespace arcticdb
