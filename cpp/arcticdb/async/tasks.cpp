/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/tasks.hpp>
#include <arcticdb/pipeline/read_frame.hpp>

namespace arcticdb::async {

// N.B. Not the same as the filtered descriptor commonly used in allocate_frame, as the segment may not contain all the columns in the filter
StreamDescriptor get_filtered_descriptor(const StreamDescriptor::Proto& desc,
    const std::shared_ptr<std::unordered_set<std::string>>& filter_columns)
{
    // We assume here that filter_columns_ will always contain the index.
    auto index = stream::index_type_from_descriptor(desc);

    return util::variant_match(index, [&desc, &filter_columns](const auto& idx) {
        if (filter_columns) {
            std::vector<FieldDescriptor::Proto> columns;
            for (const auto& field : desc.fields()) {
                if (filter_columns->find(field.name()) != std::cend(*filter_columns)) {
                    ARCTICDB_DEBUG(log::version(), "Field {} is required", field.name());
                    FieldDescriptor::Proto new_field;
                    new_field.CopyFrom(field);
                    columns.push_back(std::move(new_field));
                } else {
                    ARCTICDB_DEBUG(log::version(), "Field {} is not required", field.name());
                }
            }

            return StreamDescriptor{index_descriptor(StreamDescriptor::id_from_proto(desc), idx, std::move(columns))};
        } else {
            return StreamDescriptor{index_descriptor(StreamDescriptor::id_from_proto(desc), idx, desc.fields())};
        }
    });
}

pipelines::SliceAndKey DecodeSlicesTask::decode_into_slice(std::pair<Segment, pipelines::SliceAndKey>&& sk_pair) const
{
    auto [seg, sk] = std::move(sk_pair);
    ARCTICDB_DEBUG(log::storage(),
        "ReadAndDecodeAtomTask decoding segment of size {} with key {}",
        seg.total_segment_size(),
        variant_key_view(sk.key()));

    auto& hdr = seg.header();
    auto descriptor = async::get_filtered_descriptor(hdr.stream_descriptor(), filter_columns_);

    ARCTICDB_TRACE(log::codec(), "Creating segment");
    SegmentInMemory res(std::move(descriptor));

    decode(seg, hdr, res, hdr.stream_descriptor());
    sk.set_segment(std::move(res));
    return sk;
}

} //namespace arcticdb::async