/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/column_store/statistics.hpp>

#include <string>

namespace arcticdb {

struct FrameDescriptorImpl;

arcticdb::proto::descriptors::NormalizationMetadata make_timeseries_norm_meta(const StreamId& stream_id) {
    using namespace arcticdb::proto::descriptors;
    NormalizationMetadata norm_meta;
    NormalizationMetadata_PandasDataFrame pandas;
    auto id = std::get<StringId>(stream_id);
    pandas.mutable_common()->set_name(std::move(id));
    NormalizationMetadata_PandasIndex pandas_index;
    pandas_index.set_name("time");
    pandas_index.set_is_physically_stored(true);
    pandas.mutable_common()->mutable_index()->CopyFrom(pandas_index);
    norm_meta.mutable_df()->CopyFrom(pandas);
    return norm_meta;
}

arcticdb::proto::descriptors::NormalizationMetadata make_rowcount_norm_meta(const StreamId& stream_id) {
    using namespace arcticdb::proto::descriptors;
    NormalizationMetadata norm_meta;
    NormalizationMetadata_PandasDataFrame pandas;
    auto id = std::get<StringId>(stream_id);
    pandas.mutable_common()->set_name(std::move(id));
    NormalizationMetadata_PandasIndex pandas_index;
    pandas_index.set_is_physically_stored(true);
    pandas.mutable_common()->mutable_index()->CopyFrom(pandas_index);
    norm_meta.mutable_df()->CopyFrom(pandas);
    return norm_meta;
}

/**
 * Set the minimum defaults into norm_meta. Originally created to synthesize norm_meta for incomplete compaction.
 */
void ensure_timeseries_norm_meta(arcticdb::proto::descriptors::NormalizationMetadata& norm_meta, const StreamId& stream_id, bool set_tz) {
    if(norm_meta.input_type_case() == arcticdb::proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET) {
        norm_meta.CopyFrom(make_timeseries_norm_meta(stream_id));
    }

    if(set_tz && norm_meta.df().common().index().tz().empty())
        norm_meta.mutable_df()->mutable_common()->mutable_index()->set_tz("UTC");
}

void ensure_rowcount_norm_meta(arcticdb::proto::descriptors::NormalizationMetadata& norm_meta, const StreamId& stream_id) {
    if(norm_meta.input_type_case() == arcticdb::proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET) {
        norm_meta.CopyFrom(make_rowcount_norm_meta(stream_id));
    }
}

FrameDescriptorImpl frame_descriptor_from_proto(arcticdb::proto::descriptors::TimeSeriesDescriptor& tsd) {
    FrameDescriptorImpl output;
    output.column_groups_ = tsd.has_column_groups() && tsd.column_groups().enabled();
    output.total_rows_ = tsd.total_rows();
    return output;
}

inline SortedValue sorted_value_from_proto(arcticdb::proto::descriptors::SortedValue sorted_proto) {
    switch (sorted_proto) {
    case arcticdb::proto::descriptors::SortedValue::UNSORTED:
        return SortedValue::UNSORTED;
    case arcticdb::proto::descriptors::SortedValue::DESCENDING:
        return SortedValue::DESCENDING;
    case arcticdb::proto::descriptors::SortedValue::ASCENDING:
        return SortedValue::ASCENDING;
    default:
        return SortedValue::UNKNOWN;
    }
}
SegmentDescriptorImpl segment_descriptor_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& desc) {
    SegmentDescriptorImpl output;
    output.compressed_bytes_ = desc.out_bytes();
    output.uncompressed_bytes_ = desc.in_bytes();
    output.row_count_ = desc.row_count();
    output.index_ = IndexDescriptor(IndexDescriptor::Type(desc.index().kind()), desc.index().field_count());
    output.sorted_ = sorted_value_from_proto(desc.sorted());
    return output;
}

StreamId stream_id_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& desc) {
    return desc.id_case() == desc.kNumId ? StreamId(desc.num_id()) : StreamId(desc.str_id());
}


void field_stats_to_proto(const FieldStatsImpl& stats, arcticdb::proto::encoding::FieldStats& msg) {
    msg.set_min(stats.min_);
    msg.set_max(stats.max_);
    msg.set_unique_count(stats.unique_count_);
    msg.set_set(stats.set_);

    switch(stats.unique_count_precision_) {
    case UniqueCountType::PRECISE:
        msg.set_unique_count_precision(arcticdb::proto::encoding::FieldStats::PRECISE);
        break;
    case UniqueCountType::HYPERLOGLOG:
        msg.set_unique_count_precision(arcticdb::proto::encoding::FieldStats::HYPERLOGLOG);
        break;
    default:
        util::raise_rte("Unknown value in field stats unique count precision");
    }
}

void field_stats_from_proto(const arcticdb::proto::encoding::FieldStats& msg, FieldStatsImpl& stats) {
    stats.min_ = msg.min();
    stats.max_ = msg.max();
    stats.unique_count_ = msg.unique_count();
    stats.set_ = static_cast<uint8_t>(msg.set());

    switch(msg.unique_count_precision()) {
    case arcticdb::proto::encoding::FieldStats::PRECISE:
        stats.unique_count_precision_ = UniqueCountType::PRECISE;
        break;
    case arcticdb::proto::encoding::FieldStats::HYPERLOGLOG:
        stats.unique_count_precision_ = UniqueCountType::HYPERLOGLOG;
        break;
    default:
        util::raise_rte("Unknown unique count in field stats");
    }
}

FieldStatsImpl create_from_proto(const arcticdb::proto::encoding::FieldStats& msg) {
    FieldStatsImpl stats;
    field_stats_from_proto(msg, stats);
    return stats;
}
} //namespace arcticdb