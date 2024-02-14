/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/types.hpp>

#include <google/protobuf/text_format.h>

#include <string>

namespace arcticdb {

inline arcticdb::proto::descriptors::NormalizationMetadata make_timeseries_norm_meta(const entity::StreamId& stream_id) {
    using namespace arcticdb::proto::descriptors;
    NormalizationMetadata norm_meta;
    NormalizationMetadata_PandasDataFrame pandas;
    auto id = std::get<entity::StringId>(stream_id);
    pandas.mutable_common()->set_name(std::move(id));
    NormalizationMetadata_PandasIndex pandas_index;
    pandas_index.set_name("time");
    pandas.mutable_common()->mutable_index()->CopyFrom(pandas_index);
    norm_meta.mutable_df()->CopyFrom(pandas);
    return norm_meta;
}

inline arcticdb::proto::descriptors::NormalizationMetadata make_rowcount_norm_meta(const entity::StreamId& stream_id) {
    using namespace arcticdb::proto::descriptors;
    NormalizationMetadata norm_meta;
    NormalizationMetadata_PandasDataFrame pandas;
    auto id = std::get<entity::StringId>(stream_id);
    pandas.mutable_common()->set_name(std::move(id));
    NormalizationMetadata_PandasIndex pandas_index;
    pandas_index.set_is_not_range_index(true);
    pandas.mutable_common()->mutable_index()->CopyFrom(pandas_index);
    norm_meta.mutable_df()->CopyFrom(pandas);
    return norm_meta;
}

/**
 * Set the minimum defaults into norm_meta. Originally created to synthesize norm_meta for incomplete compaction.
 */
inline void ensure_timeseries_norm_meta(arcticdb::proto::descriptors::NormalizationMetadata& norm_meta, const entity::StreamId& stream_id, bool set_tz) {
    if(norm_meta.input_type_case() == arcticdb::proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET) {
        norm_meta.CopyFrom(make_timeseries_norm_meta(stream_id));
    }

    if(set_tz && norm_meta.df().common().index().tz().empty())
        norm_meta.mutable_df()->mutable_common()->mutable_index()->set_tz("UTC");
}

inline void ensure_rowcount_norm_meta(arcticdb::proto::descriptors::NormalizationMetadata& norm_meta, const entity::StreamId& stream_id) {
    if(norm_meta.input_type_case() == arcticdb::proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET) {
        norm_meta.CopyFrom(make_rowcount_norm_meta(stream_id));
    }
}


} //namespace arcticdb