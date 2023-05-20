/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/variant.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/pipeline/index_fields.hpp>

using namespace arcticdb::entity;
using namespace arcticdb::stream;
using namespace arcticdb::proto::descriptors;

namespace arcticdb::pipelines::index {

IndexSegmentReader get_index_reader(const AtomKey &prev_index, const std::shared_ptr<Store> &store) {
    auto [key, seg] = store->read_sync(prev_index);
    return index::IndexSegmentReader{std::move(seg)};
}

IndexSegmentReader::IndexSegmentReader(SegmentInMemory&& s) : seg_(std::move(s)) {
    seg_.metadata()->UnpackTo(&tsd_.mutable_proto());
    if(seg_.has_index_fields()) {
        tsd_.mutable_fields() = seg_.detach_index_fields();
        tsd_.mutable_fields().regenerate_offsets();
    } else {
        TimeseriesDescriptor::Proto tsd;
        if(seg_.metadata()->UnpackTo(&tsd)) {
            tsd_.mutable_fields() = fields_from_proto(tsd.stream_descriptor());
        } else {
            util::raise_rte("Unable to unpack index fields");
        }
    }
    ARCTICDB_DEBUG(log::version(), "Decoded index segment descriptor: {}", tsd_.proto().DebugString());
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
    return tsd().proto().has_column_groups() && tsd().proto().column_groups().enabled();
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
    return tsd_.proto().normalization().input_type_case() == arcticdb::proto::descriptors::NormalizationMetadata::InputTypeCase::kMsgPackFrame;
}

bool IndexSegmentReader::has_timestamp_index() const {
    return tsd_.proto().stream_descriptor().index().kind() == arcticdb::proto::descriptors::IndexDescriptor::Type::IndexDescriptor_Type_TIMESTAMP;
}

} // namespace  arcticdb::pipelines::index


