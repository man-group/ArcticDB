/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <sparrow/array/array_data.hpp>
#include <sparrow/array.hpp>
#include <sparrow/arrow_interface/array_data_to_arrow_array_converters.hpp>

#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb {

sparrow::arrow_array_unique_ptr arrow_data_from_column(const Column& column) {
    return column.type().visit_tag([&](auto && impl) -> sparrow::arrow_array_unique_ptr {
        using TagType = std::decay_t<decltype(impl)>;
        using DataType = TagType::DataTypeTag;
        using RawType = DataType::raw_type;
        if constexpr (!is_sequence_type(DataType::data_type)) {
            sparrow::array_data data;
            data.type = sparrow::data_descriptor(sparrow::arrow_traits<RawType>::type_id);

            auto column_data = column.data();
            util::check(column_data.num_blocks() == 1, "Expected single block in arrow conversion");
            auto block = column_data.next<TagType>().value();
            sparrow::buffer<RawType> buffer(const_cast<RawType *>(block.data()), block.row_count());

            data.buffers.push_back(buffer);
            data.length = static_cast<std::int64_t>(block.row_count());
            data.offset = static_cast<std::int64_t>(0);
            data.child_data.emplace_back();

            return to_arrow_array_unique_ptr(std::move(data));
        } else {
            util::raise_rte("Sequence types not implemented");
        }
    });
};
/*
std::vector<sparrow::arrow_array_unique_ptr> segment_to_arrow_arrays(SegmentInMemory& segment) {
    std::vector<sparrow::arrow_array_unique_ptr> output;
    for(auto& column : segment.column()) {
        output.emplace_back(arrow_data_from_column(column));
    }
}
*/
} // namespace arcticdb