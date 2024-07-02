/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>

#include <google/protobuf/text_format.h>
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

SegmentDescriptorImpl segment_descriptor_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& desc) {
    SegmentDescriptorImpl output;
    output.sorted_ = SortedValue(desc.sorted());
    output.compressed_bytes_ = desc.out_bytes();
    output.uncompressed_bytes_ = desc.in_bytes();
    output.row_count_ = desc.row_count();
    output.index_ = IndexDescriptor(IndexDescriptor::Type(desc.index().kind()), desc.index().field_count());
    return output;
}

StreamId stream_id_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& desc) {
    return desc.id_case() == desc.kNumId ? StreamId(desc.num_id()) : StreamId(desc.str_id());
}

} //namespace arcticdb