/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include "column_store/column.hpp"
#include "entity/stream_descriptor.hpp"
#include <sparrow/record_batch.hpp>
#include <arcticdb/arrow/arrow_c_interface.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/util/type_traits.hpp>
#include <utility>

namespace arcticdb::pipelines {

using namespace arcticdb::entity;

template<typename IndexT>
concept ValidIndex = util::any_of<
        std::remove_cvref_t<std::remove_pointer_t<std::decay_t<IndexT>>>, stream::TimeseriesIndex,
        stream::RowCountIndex, stream::TableIndex, stream::EmptyIndex>;

// Whether to determine the sort order of a timeseries index column by walking it (an O(n) scan).
// Only applies to Arrow input for which sortedness is not known upfront. Pandas precomputes it on construction.
enum class SortednessScan {
    SKIP,            // do not scan; for Arrow, leave the index sort order as UNKNOWN
    SCAN_IF_UNKNOWN, // for Arrow, walk the index column to determine its sort order
};

// This class originally wrapped numpy data, but with the addition of Arrow as an input format it is now a thin wrapper
// around a variant representing either numpy or Arrow input data
struct InputFrame {
  public:
    using FieldData = std::variant<NativeTensor, Column>;

    InputFrame();

    template<ValidIndex Index, typename DescriptorT>
    requires std::same_as<std::decay_t<DescriptorT>, StreamDescriptor>
    InputFrame(
            DescriptorT&& desc, std::vector<NativeTensor>&& field_tensors, Index&& index,
            std::optional<NativeTensor>&& index_tensor
    ) :
        index(std::forward<Index>(index)) {
        if constexpr (std::same_as<Index, stream::TimeseriesIndex>) {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    index_tensor.has_value(), "Timeseries index tensor required"
            );
        }
        set_from_tensors(std::forward<DescriptorT>(desc), std::move(field_tensors), std::move(index_tensor));
    }

    // Index, when present, is unified into columns_[0]; data tensors follow.
    template<typename DescriptorT>
    requires std::same_as<std::decay_t<DescriptorT>, StreamDescriptor>
    void set_from_tensors(
            DescriptorT&& desc, std::vector<NativeTensor>&& field_tensors, std::optional<NativeTensor>&& index_tensor
    ) {
        desc_ = std::forward<DescriptorT>(desc);
        columns_.clear();
        columns_.reserve(field_tensors.size() + (index_tensor.has_value() ? 1 : 0));
        if (index_tensor.has_value()) {
            columns_.emplace_back(std::move(*index_tensor));
        }
        columns_.insert(
                columns_.end(),
                std::make_move_iterator(field_tensors.begin()),
                std::make_move_iterator(field_tensors.end())
        );
    };

    // With SortednessScan::SCAN_IF_UNKNOWN we do an O(n) verification of the sortedness of a timeseries index and
    // use it to populate SortedValue. Otherwise the sort order is left as SortedValue::UNKNOWN.
    void set_from_columns(
            std::vector<Column>&& cols, StreamDescriptor&& desc,
            std::vector<sparrow::record_batch>&& arrow_buffer_owners, SortednessScan sortedness_scan
    );
    StreamDescriptor& desc();
    const StreamDescriptor& desc() const;
    // The descriptor of the input frame can differ from that for the timeseries descriptor in the index key for
    // Arrow string columns. Namely InputFrame may store offsets as 32bit integers, whereas on storage we always store
    // 64bit offsets.
    StreamDescriptor compute_desc_for_tsd() const;
    void set_offset(ssize_t off) const;
    bool has_index() const;
    bool empty() const;
    timestamp index_value_at(size_t row) const;
    void set_index_range();
    void set_bucketize_dynamic(bool bucketize);
    size_t num_columns() const;
    const FieldData& field_data(size_t idx) const;
    const entity::NativeTensor& get_tensor(size_t idx) const;
    const Column& get_column(size_t idx) const;

    bool has_only_tensors() const; // TODO: Remove this once we support Arrow everywhere

    mutable arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta;
    stream::Index index;
    IndexRange index_range;
    size_t num_rows = 0;
    mutable size_t offset = 0;
    // This really doesn't belong here, hash-bucketing dynamic schema data is not a property of the frame
    mutable bool bucketize_dynamic = 0;

  private:
    std::vector<FieldData> columns_;
    std::vector<sparrow::record_batch> arrow_buffer_owners_;
    StreamDescriptor desc_;

    bool has_only_tensors_{true};
};

} // namespace arcticdb::pipelines
