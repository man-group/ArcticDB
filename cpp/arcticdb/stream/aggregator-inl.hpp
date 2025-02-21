/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_AGGREGATOR_H_
#error "This should only be included by aggregator.hpp"
#endif

namespace arcticdb::stream {

template<class Index, class Schema, class SegmentingPolicy, class DensityPolicy>
void Aggregator<Index, Schema, SegmentingPolicy, DensityPolicy>::end_row() {
    segment_.end_row();
    stats_.update(row_builder_.nbytes());
    if (segmenting_policy_(stats_)) {
        commit_impl(false);
    }
}

template<class Index, class Schema, class SegmentingPolicy, class DensityPolicy>
inline void Aggregator<Index, Schema, SegmentingPolicy, DensityPolicy>::commit_impl(bool final) {
    callback_(std::move(segment_));
    commits_count_++;
    if (final)
        return;

    segment_ = SegmentInMemory(
            schema_policy_.default_descriptor(),
            segmenting_policy_.expected_row_size(),
            AllocationType::DYNAMIC,
            DensityPolicy::allow_sparse
    );
    segment_.init_column_map();
    stats_.reset();
}

template<class Index, class Schema, class SegmentingPolicy, class DensityPolicy>
inline void Aggregator<Index, Schema, SegmentingPolicy, DensityPolicy>::commit() {
    if (ARCTICDB_LIKELY(segment_.row_count() > 0 || segment_.metadata()) || segment_.has_index_descriptor()) {
        commit_impl(false);
    }
}

template<class Index, class Schema, class SegmentingPolicy, class DensityPolicy>
inline void Aggregator<Index, Schema, SegmentingPolicy, DensityPolicy>::finalize() {
    if (ARCTICDB_LIKELY(segment_.row_count() > 0 || segment_.metadata()) || segment_.has_index_descriptor()) {
        commit_impl(true);
    }
}

template<class Index, class Schema, class SegmentingPolicy, class DensityPolicy>
inline void Aggregator<Index, Schema, SegmentingPolicy, DensityPolicy>::clear() {
    segment_.clear();
}

} // namespace arcticdb::stream