/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/tasks.hpp>

namespace arcticdb::async {

// N.B. Not the same as the filtered descriptor commonly used in allocate_frame, as the segment may not contain all the
// columns in the filter
StreamDescriptor get_filtered_descriptor(
        const StreamDescriptor& desc, const std::shared_ptr<std::unordered_set<std::string>>& filter_columns
) {
    // We assume here that filter_columns_ will always contain the index.
    auto index = stream::index_type_from_descriptor(desc);

    return util::variant_match(index, [&desc, &filter_columns](const auto& idx) {
        if (filter_columns) {
            FieldCollection fields;
            for (const auto& field : desc.fields()) {
                if (filter_columns->find(std::string{field.name()}) != std::cend(*filter_columns)) {
                    ARCTICDB_DEBUG(log::version(), "Field {} is required", field.name());
                    fields.add({field.type(), field.name()});
                } else {
                    ARCTICDB_DEBUG(log::version(), "Field {} is not required", field.name());
                }
            }

            return index_descriptor_from_range(desc.id(), idx, fields);
        } else {
            return index_descriptor_from_range(desc.id(), idx, desc.fields());
        }
    });
}

pipelines::SegmentAndSlice DecodeSliceTask::decode_into_slice(storage::KeySegmentPair&& key_segment_pair) {
    auto key = key_segment_pair.atom_key();
    auto& seg = *key_segment_pair.segment_ptr();
    ARCTICDB_DEBUG(log::storage(), "ReadAndDecodeAtomTask decoding segment of size {} with key {}", seg.size(), key);
    auto& hdr = seg.header();
    const auto& desc = seg.descriptor();
    auto descriptor = async::get_filtered_descriptor(desc, columns_to_decode_);
    ranges_and_key_.col_range_.second =
            ranges_and_key_.col_range_.first + (descriptor.field_count() - descriptor.index().field_count());
    ARCTICDB_TRACE(log::codec(), "Creating segment");
    SegmentInMemory segment_in_memory(std::move(descriptor));
    decode_into_memory_segment(seg, hdr, segment_in_memory, desc);
    segment_in_memory.set_row_data(std::max(segment_in_memory.row_count() - 1, ranges_and_key_.row_range().diff() - 1));
    return pipelines::SegmentAndSlice(std::move(ranges_and_key_), std::move(segment_in_memory));
}
} // namespace arcticdb::async