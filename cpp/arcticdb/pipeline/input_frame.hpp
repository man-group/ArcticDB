/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/util/type_traits.hpp>

namespace arcticdb::pipelines {

using namespace arcticdb::entity;

template<typename IndexT>
concept ValidIndex = util::any_of<
        std::remove_cvref_t<std::remove_pointer_t<std::decay_t<IndexT>>>, stream::TimeseriesIndex,
        stream::RowCountIndex, stream::TableIndex, stream::EmptyIndex>;

// This class originally wrapped numpy data, but with the addition of Arrow as an input format it is now a thin wrapper
// around a variant representing either numpy or Arrow input data
struct InputFrame {
  public:
    InputFrame();
    InputFrame(SegmentInMemory&& seg);
    InputFrame(
            StreamDescriptor&& desc, std::vector<entity::NativeTensor>&& field_tensors,
            std::optional<entity::NativeTensor>&& index_tensor = std::nullopt
    );
    void set_segment(SegmentInMemory&& seg);
    void set_from_tensors(
            StreamDescriptor&& desc, std::vector<entity::NativeTensor>&& field_tensors,
            std::optional<entity::NativeTensor>&& index_tensor
    );

    StreamDescriptor& desc();
    const StreamDescriptor& desc() const;
    // The descriptor of the input frame can differ than that for the timeseries descriptor in the index key for Arrow
    // at least if there are string columns, and potentially in other cases as more type support is added
    const StreamDescriptor& desc_for_tsd();
    void set_offset(ssize_t off) const;
    bool has_index() const;
    bool empty() const;
    timestamp index_value_at(size_t row);
    void set_index_range();
    void set_bucketize_dynamic(bool bucketize);
    bool has_segment() const;
    bool has_tensors() const;
    const std::optional<entity::NativeTensor>& opt_index_tensor() const;
    const std::vector<entity::NativeTensor>& field_tensors() const;
    const SegmentInMemory& segment() const;

    mutable arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta;
    stream::Index index;
    IndexRange index_range;
    size_t num_rows = 0;
    mutable size_t offset = 0;
    // This really doesn't belong here, hash-bucketing dynamic schema data is not a property of the frame
    mutable bool bucketize_dynamic = 0;

  private:
    struct InputTensors {
        std::optional<entity::NativeTensor> index_tensor;
        std::vector<entity::NativeTensor> field_tensors;
        StreamDescriptor desc;
    };
    struct InputSegment {
        SegmentInMemory seg;
        StreamDescriptor desc_for_tsd;
        explicit InputSegment(SegmentInMemory&& segment) : seg(std::move(segment)) {
            desc_for_tsd = seg.descriptor().clone();
            for (auto& field : desc_for_tsd.fields()) {
                if (field.type().data_type() == DataType::UTF_DYNAMIC32) {
                    field.mutable_type() = TypeDescriptor(DataType::UTF_DYNAMIC64, field.type().dimension());
                }
            }
        }
    };
    std::variant<InputTensors, InputSegment> input_data;
};

} // namespace arcticdb::pipelines
