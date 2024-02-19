/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>



namespace arcticdb {

using namespace arcticdb::storage;
using namespace arcticdb::entity;
using namespace arcticdb::stream;


std::unordered_map<entity::StreamId, size_t> get_num_version_entries(const std::shared_ptr<Store>& store, size_t batch_size)  {
    std::unordered_map<entity::StreamId, size_t> output;
    size_t max_blocks = ConfigsMap::instance()->get_int("VersionMap.MaxVersionBlocks", 5);
    store->iterate_type(entity::KeyType::VERSION, [&output, batch_size, max_blocks] (const VariantKey& key) {
        ++output[variant_key_id(key)];
        if (output.size() >= batch_size) {
            // remove half of them which are under max_blocks
            // otherwise memory would blow up for big libraries
            auto iter = output.begin();
            while(iter != output.end()) {
                auto copy = iter;
                iter++;
                if (copy->second < max_blocks) {
                    output.erase(copy);
                }
                if (output.size() <= batch_size / 2) {
                    break;
                }
            }
        }
    });
    return output;
}


FrameAndDescriptor frame_and_descriptor_from_segment(SegmentInMemory&& seg) {
    TimeseriesDescriptor tsd;
    auto& tsd_proto = tsd.mutable_proto();
    tsd_proto.set_total_rows(seg.row_count());
    const auto& seg_descriptor = seg.descriptor();
    tsd_proto.mutable_stream_descriptor()->CopyFrom(seg_descriptor.proto());
    if (seg.descriptor().index().type() == IndexDescriptor::ROWCOUNT)
        ensure_rowcount_norm_meta(*tsd_proto.mutable_normalization(), seg_descriptor.id());
    else
        ensure_timeseries_norm_meta(*tsd.mutable_proto().mutable_normalization(), seg_descriptor.id(), false);
    return { SegmentInMemory(std::move(seg)), tsd, {}, {} };
}

}