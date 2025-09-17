/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <folly/futures/Future.h>

namespace arcticdb::pipelines {
struct ReadQuery;
}

namespace arcticdb::pipelines::index {

struct IndexSegmentIterator;

struct IndexSegmentReader {
    const SegmentInMemory& seg() const { return seg_; }

    friend void swap(IndexSegmentReader& left, IndexSegmentReader& right) noexcept {
        using std::swap;

        swap(left.seg_, right.seg_);
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(IndexSegmentReader)

    explicit IndexSegmentReader(SegmentInMemory&& s);

    const Column& column(Fields field) const;

    SliceAndKey row(std::size_t i) const;

    size_t size() const;

    IndexSegmentIterator begin() const;

    IndexSegmentIterator end() const;

    IndexSegmentIterator last() const;

    bool empty() const;

    bool is_pickled() const;

    bool has_timestamp_index() const;

    bool bucketize_dynamic() const;

    SortedValue sorted() const { return tsd().sorted(); }

    void set_sorted(SortedValue sorted) { mutable_tsd().set_sorted(sorted); }

    const TimeseriesDescriptor& tsd() const { return seg_.index_descriptor(); }

    TimeseriesDescriptor& mutable_tsd() { return seg_.mutable_index_descriptor(); }

  private:
    mutable std::unordered_map<ColRange, std::shared_ptr<StreamDescriptor>, AxisRange::Hasher> descriptor_by_col_group_;
    SegmentInMemory seg_;
};

struct IndexSegmentIterator {
  public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = SliceAndKey;
    using difference_type = std::ptrdiff_t;
    using pointer = SliceAndKey*;
    using reference = SliceAndKey&;

    explicit IndexSegmentIterator(const IndexSegmentReader* reader) : reader_(reader) {}

    IndexSegmentIterator(const IndexSegmentReader* reader, difference_type diff) : reader_(reader), diff_(diff) {}

    IndexSegmentIterator& operator++() {
        ++diff_;
        return *this;
    }

    IndexSegmentIterator operator++(int) {
        IndexSegmentIterator tmp(reader_, diff_);
        ++*this;
        return tmp;
    }

    reference operator*() {
        value_ = reader_->row(diff_);
        return value_;
    }

    pointer operator->() {
        value_ = reader_->row(diff_);
        return &value_;
    }

    friend bool operator==(const IndexSegmentIterator& left, const IndexSegmentIterator& right) {
        return left.diff_ == right.diff_;
    }

    friend bool operator!=(const IndexSegmentIterator& left, const IndexSegmentIterator& right) {
        return !(left == right);
    }

  private:
    const IndexSegmentReader* reader_;
    difference_type diff_ = 0;
    SliceAndKey value_;
};

index::IndexSegmentReader get_index_reader(const AtomKey& prev_index, const std::shared_ptr<Store>& store);

folly::Future<IndexSegmentReader> async_get_index_reader(
        const AtomKey& prev_index, const std::shared_ptr<Store>& store
);

IndexRange get_index_segment_range(const AtomKey& prev_index, const std::shared_ptr<Store>& store);

void check_column_and_date_range_filterable(
        const IndexSegmentReader& index_segment_reader, const ReadQuery& read_query
);

} // namespace arcticdb::pipelines::index