/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
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

} //namespace arcticdb::stream
