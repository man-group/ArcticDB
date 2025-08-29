/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/storage/coalesced/multi_segment_header.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/file/mapped_file_storage.hpp>
#include <arcticdb/storage/single_file_storage.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/stream/piloted_clock.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/pipeline/write_frame.hpp>

namespace arcticdb {


size_t max_data_size(
    const std::vector<std::tuple<stream::StreamSink::PartialKey, SegmentInMemory, FrameSlice>>& items,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    EncodingVersion encoding_version) {
    auto max_file_size = 0UL;
    for(const auto& item : items) {
        const auto& [pk, seg, slice] = item;
        auto result = max_compressed_size_dispatch(seg, codec_opts, encoding_version);
        max_file_size += result.max_compressed_bytes_ + result.encoded_blocks_bytes_;
        const auto header_size = SegmentHeader::required_bytes(seg);
        max_file_size += header_size;
        ARCTICDB_DEBUG(log::codec(), "Adding max file size {} + {} + {}", result.max_compressed_bytes_, result.encoded_blocks_bytes_, header_size);
    }
    return max_file_size;
}

struct FileFooter {
    uint64_t index_offset_;
    uint64_t footer_offset_;
};

void write_dataframe_to_file_internal(
    const StreamId &stream_id,
    const std::shared_ptr<pipelines::InputTensorFrame> &frame,
    const std::string& path,
    const WriteOptions &options,
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    EncodingVersion encoding_version
) {
    ARCTICDB_SAMPLE(WriteDataFrameToFile, 0)
    py::gil_scoped_release release_gil;
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write_dataframe_to_file");
    frame->set_bucketize_dynamic(options.bucketize_dynamic);
    auto slicing = get_slicing_policy(options, *frame);
    auto partial_key = pipelines::TypedStreamVersion{frame->desc.id(), VersionId{0}, KeyType::TABLE_DATA};
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceFrame)
    auto slices = slice(*frame, slicing);
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceAndWrite)

    auto slice_and_rowcount = get_slice_and_rowcount(slices);
    auto key_seg_futs = folly::collect(folly::window(std::move(slice_and_rowcount),
         [frame, slicing, key = std::move(partial_key),
             sparsify_floats = options.sparsify_floats](auto &&slice) {
             return async::submit_cpu_task(pipelines::WriteToSegmentTask(
                 frame,
                 slice.first,
                 slicing,
                 get_partial_key_gen(frame, key),
                 slice.second,
                 frame->index,
                 sparsify_floats));
         },
         write_window_size())).via(&async::io_executor());
    auto segments = std::move(key_seg_futs).get();

    auto data_size = max_data_size(segments, codec_opts, encoding_version);
    ARCTICDB_DEBUG(log::version(), "Estimated max data size: {}", data_size);
    auto config = storage::file::pack_config(path, data_size, segments.size(), stream_id, stream::get_descriptor_from_index(frame->index), encoding_version, codec_opts);

    storage::LibraryPath lib_path{std::string{"file"}, fmt::format("{}", stream_id)};
    auto library = create_library(lib_path, storage::OpenMode::WRITE, {std::move(config)});
    auto store = std::make_shared<async::AsyncStore<PilotedClock>>(library, codec_opts, encoding_version);
    auto dedup_map = std::make_shared<DeDupMap>();
    size_t batch_size = ConfigsMap::instance()->get_int("FileWrite.BatchSize", 50);
    auto index_fut = folly::collect(folly::window(std::move(segments), [store, dedup_map] (auto key_seg) {
        return store->async_write(key_seg, dedup_map);
    }, batch_size)).via(&async::io_executor())
    .thenValue([&frame, stream_id, store] (auto&& slice_and_keys) {
        return index::write_index(frame, std::forward<decltype(slice_and_keys)>(slice_and_keys), IndexPartialKey{stream_id, VersionId{0}}, store);
    });
    // TODO include key size and key offset in max size calculation
    auto index_key = std::move(index_fut).get();
    auto serialized_key = to_serialized_key(index_key);
    auto single_file_store = library->get_single_file_storage().value();
    const auto offset = single_file_store->get_offset();
    single_file_store->write_raw(reinterpret_cast<const uint8_t*>(serialized_key.c_str()), serialized_key.size());
    single_file_store->finalize(storage::KeyData{offset, serialized_key.size()});
}

version_store::ReadVersionOutput read_dataframe_from_file_internal(
        const StreamId& stream_id,
        const std::string& path,
        const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options,
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        std::any& handler_data) {
    auto config = storage::file::pack_config(path, codec_opts);
    storage::LibraryPath lib_path{std::string{"file"}, fmt::format("{}", stream_id)};
    auto library = create_library(lib_path, storage::OpenMode::WRITE, {std::move(config)});
    auto store = std::make_shared<async::AsyncStore<PilotedClock>>(library, codec::default_lz4_codec(), EncodingVersion::V1);

    auto single_file_storage = library->get_single_file_storage().value();

    using namespace arcticdb::storage;
    const auto data_end = single_file_storage->get_bytes() - sizeof(KeyData);
    auto key_data = *reinterpret_cast<KeyData*>(single_file_storage->read_raw(data_end, sizeof(KeyData)));
    const auto key_offset = key_data.key_offset_;
    const auto key_size = key_data.key_size_;
    const auto serialized_key = single_file_storage->read_raw(key_offset, key_size);
    auto index_key = from_serialized_atom_key(serialized_key, KeyType::TABLE_INDEX);
    VersionedItem versioned_item(index_key);
    const auto header_offset = key_data.key_offset_ + key_data.key_size_;
    ARCTICDB_DEBUG(log::storage(), "Got header offset at {}", header_offset);
    single_file_storage->load_header(header_offset, data_end - header_offset);
    return version_store::read_frame_for_version(store, versioned_item, read_query, read_options, handler_data).get();
}
} //namespace arcticdb