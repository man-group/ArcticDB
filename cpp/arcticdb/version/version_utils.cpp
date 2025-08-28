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


VariantKey write_multi_index_entry(
    std::shared_ptr<StreamSink> store,
    std::vector<AtomKey> &keys,
    const StreamId &stream_id,
    const py::object &metastruct,
    const py::object &user_meta,
    VersionId version_id
) {
    ARCTICDB_SAMPLE(WriteJournalEntry, 0)
    ARCTICDB_DEBUG(log::version(), "Version map writing multi key");
    VariantKey multi_key;

    IndexAggregator<RowCountIndex> multi_index_agg(stream_id, [&multi_key, &store, version_id, stream_id](auto &&segment) {
        multi_key = store->write_sync(KeyType::MULTI_KEY,
                                     version_id,  // version_id
                                     stream_id,
                                     NumericIndex{0},  // start_index
                                     NumericIndex{0},  // end_index
                                     std::forward<decltype(segment)>(segment));
    });

    for (const auto& key : keys) {
        multi_index_agg.add_key(key);
    }
    TimeseriesDescriptor timeseries_descriptor;

    if (!metastruct.is_none()) {
        arcticdb::proto::descriptors::UserDefinedMetadata multi_key_proto;
        python_util::pb_from_python(metastruct, multi_key_proto);
        timeseries_descriptor.set_multi_key_metadata(std::move(multi_key_proto));
    }
    if (!user_meta.is_none()) {
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        python_util::pb_from_python(user_meta, user_meta_proto);
        timeseries_descriptor.set_user_metadata(std::move(user_meta_proto));
    }
    multi_index_agg.set_timeseries_descriptor(timeseries_descriptor);
    multi_index_agg.commit();
    return multi_key;
}

std::unordered_map<StreamId, size_t> get_num_version_entries(const std::shared_ptr<Store>& store, size_t batch_size)  {
    std::unordered_map<StreamId, size_t> output;
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
    tsd.set_total_rows(seg.row_count());
    const auto& seg_descriptor = seg.descriptor();
    tsd.set_stream_descriptor(seg_descriptor);
    if (seg_descriptor.index().type() == IndexDescriptor::Type::ROWCOUNT)
        ensure_rowcount_norm_meta(*tsd_proto.mutable_normalization(), seg_descriptor.id());
    else
        ensure_timeseries_norm_meta(*tsd.mutable_proto().mutable_normalization(), seg_descriptor.id(), false);
    return { SegmentInMemory(std::move(seg)), tsd, {}};
}

}