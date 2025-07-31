/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/util/simple_string_hash.hpp>

namespace arcticdb::pipelines {

std::pair<int64_t, int64_t> get_index_and_field_count(const arcticdb::pipelines::InputTensorFrame& frame) {
    return {frame.desc.index().field_count(), frame.desc.fields().size()};
}


SlicingPolicy get_slicing_policy(
    const WriteOptions& options,
    const arcticdb::pipelines::InputTensorFrame& frame) {
    if(frame.bucketize_dynamic) {
        const auto [index_count, field_count] = get_index_and_field_count(frame);
        const auto col_count = field_count - index_count;
        const auto num_buckets = std::min(static_cast<size_t>(std::ceil(double(col_count) / options.column_group_size)), options.max_num_buckets);
        return HashedSlicer(num_buckets, options.segment_row_size);
    }

    return FixedSlicer{options.column_group_size, options.segment_row_size};
}

SlicingPolicy get_slicing_policy(
        const WriteOptions& options,
        const SegmentInMemory& segment) {
    if(options.bucketize_dynamic) {
        const auto col_count = segment.num_columns();
        const auto num_buckets = std::min(static_cast<size_t>(std::ceil(double(col_count) / options.column_group_size)), options.max_num_buckets);
        return HashedSlicer(num_buckets, options.segment_row_size);
    }

    return FixedSlicer{options.column_group_size, options.segment_row_size};
}

std::vector<FrameSlice> slice(InputTensorFrame& frame, const SlicingPolicy& arg) {
    return util::variant_match(arg,
            [&frame](NoSlicing) -> std::vector<FrameSlice> {
                return {FrameSlice{std::make_shared<StreamDescriptor>(frame.desc),
                                   ColRange{frame.desc.index().field_count(), frame.desc.fields().size()},
                                   RowRange{0, frame.num_rows}}};
            },
            [&frame](const auto& slicer) {
                return slicer(frame);
            });
}

void add_index_fields(const arcticdb::pipelines::InputTensorFrame& frame, FieldCollection& current_fields) {
    for (auto i = 0u; i < frame.desc.index().field_count(); ++i) {
        const auto& field = frame.desc.fields(0);
        current_fields.add({field.type(), field.name()});
    }
}

std::pair<size_t, size_t> get_first_and_last_row(const arcticdb::pipelines::InputTensorFrame& frame) {
    return {frame.offset, frame.num_rows + frame.offset};
}

std::vector<FrameSlice> FixedSlicer::operator()(const arcticdb::pipelines::InputTensorFrame& frame) const {
    const auto [index_count, total_field_count] = get_index_and_field_count(frame);
    auto field_count = total_field_count - index_count;
    auto tensor_pos = std::begin(frame.field_tensors);
    auto fields_pos = std::begin(frame.desc.fields());
    std::advance(fields_pos, index_count);

    auto id = frame.desc.id();
    auto index = frame.desc.index();

    std::vector<FrameSlice> slices;
    slices.reserve(field_count / col_per_slice_ + std::size_t((field_count % col_per_slice_) == 0ULL));

    const auto [first_row, last_row] = get_first_and_last_row(frame);

    // order of the frame slices is used in the mark_index_slices impl. If slices are not grouped and ordered the same
    // way, one will need to modify the mark_index_slices method to use two passes instead of one
    auto col = index_count;
    do {
        auto tensor_next = tensor_pos;
        auto fields_next = fields_pos;
        auto distance = std::min(size_t(std::distance(tensor_pos, std::end(frame.field_tensors))), col_per_slice_);
        std::advance(tensor_next, distance);
        std::advance(fields_next, distance);

        // systematically writing the index in the column group
        // to avoid needlessly reading the first group just for the index
        auto current_fields = std::make_shared<FieldCollection>();
        add_index_fields(frame, *current_fields);

        for(auto field = fields_pos; field != fields_next; ++field) {
            current_fields->add({field->type(), field->name()});
        }


        auto desc = std::make_shared<StreamDescriptor>(id, index, current_fields);
        for (std::size_t r = first_row, end = last_row; r < end; r += row_per_slice_) {
            auto rdist = std::min(last_row-r, row_per_slice_);
            slices.push_back(FrameSlice(desc,
                                        ColRange{col, col+distance},
                                        RowRange{r, r+rdist}));
        }

        col += col_per_slice_;
        tensor_pos = tensor_next;
        fields_pos = fields_next;
    } while (tensor_pos!=std::end(frame.field_tensors));
    return slices;
}

std::vector<FrameSlice> HashedSlicer::operator()(const arcticdb::pipelines::InputTensorFrame& frame) const {
    std::vector<uint32_t> buckets;
    const auto [index_count, field_count] = get_index_and_field_count(frame);

    for(auto i = index_count; i < field_count; ++i)
        buckets.push_back(bucketize(frame.desc.field(i).name(), num_buckets_));

    std::vector<size_t> indices(buckets.size());
    std::iota(std::begin(indices), std::end(indices), index_count);
    std::sort(std::begin(indices), std::end(indices), [&buckets, index_count=index_count] (size_t left, size_t right) {
        return buckets[left - index_count] < buckets[right - index_count];
    });

    const auto [first_row, last_row] = get_first_and_last_row(frame);

    std::vector<FrameSlice> slices;
    auto start_pos = std::cbegin(indices);
    auto col = index_count;
    
    do {
        const auto current_bucket = buckets[*start_pos - index_count];
        const auto end_pos = std::find_if(start_pos, std::cend(indices), [&buckets, current_bucket, index_count=index_count] (size_t idx){
            return buckets[idx - index_count] != current_bucket;
        });
        const auto distance = std::distance(start_pos, end_pos);

        auto current_fields = std::make_shared<FieldCollection>();
        add_index_fields(frame, *current_fields);

        for(auto field = start_pos; field < end_pos; ++field) {
            const auto& f = frame.desc.field(*field);
            current_fields->add({f.type(), f.name()});
        }

        auto desc = std::make_shared<StreamDescriptor>(frame.desc.id(), frame.desc.index(), std::move(current_fields));
        
        for (std::size_t r = first_row, end = last_row; r < end; r += row_per_slice_) {
            auto rdist = std::min(last_row-r, row_per_slice_);
            slices.emplace_back(FrameSlice(desc,
                                        ColRange{col, col + distance},
                                        RowRange{r, r+rdist},
                                        current_bucket,
                                        num_buckets_,
                                        std::vector<size_t>(start_pos, end_pos)));
        }

        start_pos = end_pos;
        col += distance;
    } while(start_pos != std::cend(indices));

    return slices;
}
} //namespace arcticdb::pipelines
