/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/stream/aggregator.hpp>

namespace arcticdb::stream {

void AggregationStats::reset() {
    nbytes = 0;
    count = 0;
    total_rows_ = 0;
    last_active_time_ = util::SysClock::coarse_nanos_since_epoch();
}

void AggregationStats::update(size_t num_bytes) {
    ++count;
    nbytes += num_bytes;
    if (total_rows_ == 0) {
        last_active_time_ = util::SysClock::coarse_nanos_since_epoch();
    }
    ++total_rows_;
}

void AggregationStats::update_many(size_t rows, size_t num_bytes) {
    ++count;
    nbytes += num_bytes;
    if (total_rows_ == 0) {
        last_active_time_ = util::SysClock::coarse_nanos_since_epoch();
    }
    total_rows_ += rows;
}

} // namespace arcticdb::stream
