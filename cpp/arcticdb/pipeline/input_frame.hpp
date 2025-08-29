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
#include <arcticdb/entity/types.hpp>
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
    InputFrame() :
        index(stream::empty_index()) {}

    std::optional<SegmentInMemory> seg;
    // TODO: Remove once a const view sparrow::record_batch is available
    // Until then, this is required to keep memory alive
    std::vector<sparrow::record_batch> record_batches;


    mutable arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta;
    stream::Index index;
    std::optional<entity::NativeTensor> index_tensor;
    std::vector<entity::NativeTensor> field_tensors;
    IndexRange index_range;
    size_t num_rows = 0;
    mutable size_t offset = 0;

    StreamDescriptor& desc() {
        if (seg.has_value()) {
            return seg->descriptor();
        } else {
            return desc_;
        }
    }

    const StreamDescriptor& desc() const {
        return const_cast<InputFrame*>(this)->desc();
    }

    void set_offset(ssize_t off) const {
        offset = off;
    }

    void set_sorted(SortedValue sorted) {
        switch (sorted) {
            case SortedValue::UNSORTED:desc().set_sorted(SortedValue::UNSORTED);break;
            case SortedValue::DESCENDING:desc().set_sorted(SortedValue::DESCENDING);break;
            case SortedValue::ASCENDING:desc().set_sorted(SortedValue::ASCENDING);break;
            default:desc().set_sorted(SortedValue::UNKNOWN);
        }
    }

    bool has_index() const { return desc().index().field_count() != 0ULL; }

    bool empty() const { return num_rows == 0; }

    timestamp index_value_at(size_t row) {
        util::check(has_index(), "InputFrame::index_value_at should only be called on timeseries data");
        if (seg.has_value()) {
            util::check(row < seg->row_count(), "Out of range row {} requsted in InputFrame::index_value_at with segment of length",
                        row, seg->row_count());
            const auto& index_column = seg->column(0);
            return *index_column.scalar_at<timestamp>(row);
        } else {
            util::check(index_tensor.has_value(), "InputFrame::index_value_at call with null index tensor");
            util::check(index_tensor->data_type() == DataType::NANOSECONDS_UTC64,
                        "Expected timestamp index in append, got type {}", index_tensor->data_type());
            return *index_tensor->ptr_cast<timestamp>(row);
        }
    }

    void set_index_range() {
        // Fill index range
        // Note RowCountIndex will normally have an index field count of 0
        if(num_rows == 0) {
            index_range.start_ = IndexValue{ NumericIndex{0} };
            index_range.end_ = IndexValue{ NumericIndex{0} };
        } else if (desc().index().field_count() == 1) {
            visit_field(desc().field(0), [&](auto &&tag) {
                using DT = std::decay_t<decltype(tag)>;
                using RawType = typename DT::DataTypeTag::raw_type;
                if constexpr (std::is_integral_v<RawType> || std::is_floating_point_v<RawType>) {
                    if (seg.has_value()) {
                        const auto& index_column = seg->column(0);
                        index_range.start_ = IndexValue(*index_column.scalar_at<timestamp>(0));
                        index_range.end_ = IndexValue(*index_column.scalar_at<timestamp>(num_rows - 1));
                    } else {
                        util::check(static_cast<bool>(index_tensor), "Got null index tensor in set_index_range");
                        util::check(index_tensor->nbytes() > 0, "Empty index tensor");
                        auto &tensor = index_tensor.value();
                        auto start_t = tensor.ptr_cast<RawType>(0);
                        auto end_t = tensor.ptr_cast<RawType>(static_cast<size_t>(tensor.shape(0) - 1));
                        index_range.start_  = IndexValue(static_cast<timestamp>(*start_t));
                        index_range.end_ = IndexValue(static_cast<timestamp>(*end_t));
                    }
                } else
                    throw std::runtime_error("Unsupported non-integral index type");
                });
        } else {
            index_range.start_ = IndexValue{ NumericIndex{0} };
            index_range.end_ = IndexValue{static_cast<timestamp>(num_rows) - 1};
        }
    }
  private:
    StreamDescriptor desc_;
};

} //namespace arcticdb::pipelines
