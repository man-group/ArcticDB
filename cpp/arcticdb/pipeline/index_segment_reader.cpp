/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/pipeline/query.hpp>

using namespace arcticdb::entity;
using namespace arcticdb::stream;
using namespace arcticdb::proto::descriptors;

namespace arcticdb::pipelines::index {

IndexSegmentReader get_index_reader(const AtomKey &prev_index, const std::shared_ptr<Store> &store) {
    auto [key, seg] = store->read_sync(prev_index);
    return index::IndexSegmentReader{std::move(seg)};
}

folly::Future<IndexSegmentReader> async_get_index_reader(const AtomKey &prev_index, const std::shared_ptr<Store> &store) {
    return store->read(prev_index).thenValueInline([](std::pair<VariantKey, SegmentInMemory>&& key_seg) {
        return IndexSegmentReader{std::move(key_seg.second)};
    });
}

IndexSegmentReader::IndexSegmentReader(SegmentInMemory&& s) :
    seg_(std::move(s)) {
}

const Column &IndexSegmentReader::column(Fields field) const {
    return seg_.column(position_t(field));
}

bool IndexSegmentReader::empty() const {
    return seg_.empty();
}

IndexRange get_index_segment_range(
    const AtomKey& prev_index,
    const std::shared_ptr<Store>& store) {
    auto isr = get_index_reader(prev_index, store);
    return IndexRange{
        isr.begin()->key().start_index(),
        isr.last()->key().end_index()
    };
}

bool IndexSegmentReader::bucketize_dynamic() const {
    return tsd().column_groups();
}

SliceAndKey IndexSegmentReader::row(std::size_t r) const {
    auto i = static_cast<ssize_t>(r);
    auto key_type = key_type_from_segment<Fields>(seg_, i);
    auto sid = stream_id_from_segment<Fields>(seg_, i);

    auto k = entity::atom_key_builder()
            .gen_id(seg_.scalar_at<VersionId>(i, int(Fields::version_id)).value())
            .creation_ts(seg_.scalar_at<timestamp>(i, int(Fields::creation_ts)).value())
            .content_hash(seg_.scalar_at<uint64_t>(i, int(Fields::content_hash)).value())
            .start_index(index_start_from_segment<SegmentInMemory, Fields>(seg_, i))
            .end_index(index_end_from_segment<SegmentInMemory, Fields>(seg_, i))
            .build(std::move(sid), key_type);

    ColRange col_rg{column(index::Fields::start_col).scalar_at<std::size_t>(i).value(),
                    column(index::Fields::end_col).scalar_at<std::size_t>(i).value()};
    RowRange row_rg{column(index::Fields::start_row).scalar_at<std::size_t>(i).value(),
                    column(index::Fields::end_row).scalar_at<std::size_t>(i).value()};

    std::optional<size_t> hash_bucket;
    std::optional<uint64_t> num_buckets;
    if(bucketize_dynamic()) {
        hash_bucket = column(index::Fields::hash_bucket).scalar_at<std::size_t>(i).value();
        num_buckets = column(index::Fields::num_buckets).scalar_at<std::size_t>(i).value();
    }
    return {FrameSlice{col_rg, row_rg, hash_bucket, num_buckets}, std::move(k)};
}

size_t IndexSegmentReader::size() const {
    return seg_.row_count();
}

IndexSegmentIterator IndexSegmentReader::begin() const {
    return IndexSegmentIterator(this);
}

IndexSegmentIterator IndexSegmentReader::end() const {
    return {this, static_cast<int64_t>(size())};
}

IndexSegmentIterator IndexSegmentReader::last() const {
    return {this, static_cast<int64_t>(size() -1)};
}

bool IndexSegmentReader::is_pickled() const {
    return tsd().proto().normalization().input_type_case() == arcticdb::proto::descriptors::NormalizationMetadata::InputTypeCase::kMsgPackFrame;
}

bool IndexSegmentReader::has_timestamp_index() const {
    return tsd().index().type_ == IndexDescriptor::Type::TIMESTAMP;
}

void check_column_and_date_range_filterable(const pipelines::index::IndexSegmentReader& index_segment_reader, const ReadQuery& read_query) {
    util::check(!index_segment_reader.is_pickled()
                    || (!read_query.columns.has_value() && std::holds_alternative<std::monostate>(read_query.row_filter)),
                "The data for this symbol is pickled and does not support column stats, date_range, row_range, or column queries");
    util::check(index_segment_reader.has_timestamp_index() || !std::holds_alternative<IndexRange>(read_query.row_filter),
                "Cannot apply date range filter to symbol with non-timestamp index");
    sorting::check<ErrorCode::E_UNSORTED_DATA>(index_segment_reader.sorted() == SortedValue::UNKNOWN ||
                                                   index_segment_reader.sorted() == SortedValue::ASCENDING ||
                                                   !std::holds_alternative<IndexRange>(read_query.row_filter),
                                               "When filtering data using date_range, the symbol must be sorted in ascending order. ArcticDB believes it is not sorted in ascending order and cannot therefore filter the data using date_range.");
}

} // namespace  arcticdb::pipelines::index


