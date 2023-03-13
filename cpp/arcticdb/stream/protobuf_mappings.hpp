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
using namespace arcticdb::entity;



inline arcticdb::proto::descriptors::NormalizationMetadata make_timeseries_norm_meta(const StreamId& stream_id) {
    arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    arcticdb::proto::descriptors::NormalizationMetadata_PandasDataFrame pandas;
    auto id = std::get<StringId>(stream_id);
    pandas.mutable_common()->set_name(std::move(id));
    arcticdb::proto::descriptors::NormalizationMetadata_PandasIndex pandas_index;
    pandas_index.set_name("time");
    pandas.mutable_common()->mutable_index()->CopyFrom(pandas_index);
    norm_meta.mutable_df()->CopyFrom(pandas);
    return norm_meta;
}

} //namespace arcticdb