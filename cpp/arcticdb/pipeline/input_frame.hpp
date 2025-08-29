/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <sparrow/record_batch.hpp>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/util/type_traits.hpp>

namespace arcticdb::pipelines {

using namespace arcticdb::entity;

template <typename IndexT>
concept ValidIndex = util::any_of<
        std::remove_cvref_t<std::remove_pointer_t<std::decay_t<IndexT>>>,
        stream::TimeseriesIndex,
        stream::RowCountIndex,
        stream::TableIndex,
        stream::EmptyIndex>;

// This class originally wrapped numpy data, but with the addition of Arrow as an input format it is now a thin wrapper
// around a variant representing either numpy or Arrow input data
struct InputFrame {
  public:
    InputFrame();

    StreamDescriptor& desc();
    const StreamDescriptor& desc() const;
    void set_offset(ssize_t off) const;
    void set_sorted(SortedValue sorted);
    bool has_index() const;
    bool empty() const;
    timestamp index_value_at(size_t row);
    void set_index_range();

    mutable arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta;
    stream::Index index;
    IndexRange index_range;
    size_t num_rows = 0;
    mutable size_t offset = 0;

    std::optional<entity::NativeTensor> index_tensor;
    std::vector<entity::NativeTensor> field_tensors;
    std::optional<SegmentInMemory> seg;
        // TODO: Remove once a const view sparrow::record_batch is available
    // Until then, this is required to keep memory alive
    std::vector<sparrow::record_batch> record_batches;
  private:
    StreamDescriptor desc_;
};

} //namespace arcticdb::pipelines
