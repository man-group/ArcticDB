#include <arcticdb/entity/types.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/storage/coalesced/multi_segment_header.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/util/optional_defaults.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/file/single_file_storage.hpp>
#include <arcticdb/storage/library.hpp>

namespace arcticdb {


size_t max_data_size(
    const std::vector<std::tuple<stream::StreamSink::PartialKey, SegmentInMemory, FrameSlice>>& items,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    EncodingVersion encoding_version) {
    auto max_file_size = 0UL;
    for(const auto& item : items) {
        const auto& [pk, seg, slice] = item;
        max_file_size += max_compressed_size_dispatch(seg, codec_opts, encoding_version).max_compressed_bytes_;
    }
    return max_file_size;
}
void write_dataframe_to_file_internal(
    const StreamId &stream_id,
    const std::shared_ptr<pipelines::InputTensorFrame> &frame,
    const WriteOptions &options,
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    EncodingVersion encoding_version
) {
    ARCTICDB_SAMPLE(WriteVersionedDataFrame, 0)
    py::gil_scoped_release release_gil;
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: write_dataframe_to_file");
    frame->set_bucketize_dynamic(options.bucketize_dynamic);
    //auto dedup_map = std::make_shared<DeDupMap>();
    auto slicing = get_slicing_policy(options, *frame);
    auto partial_key = pipelines::IndexPartialKey{frame->desc.id(), VersionId{0}};
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceFrame)
    auto slices = slice(*frame, slicing);
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceAndWrite)

    auto slice_and_rowcount = get_slice_and_rowcount(slices);
    const auto write_window = ConfigsMap::instance()->get_int("VersionStore.BatchWriteWindow",
                                                              2 * async::TaskScheduler::instance()->io_thread_count());
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
                                                     write_window)).via(&async::io_executor());
    auto segments = std::move(key_seg_futs).get();

    auto data_size = max_data_size(segments, codec_opts, encoding_version);

    data_size += max_compressed_size_dispatch(multi_segment_header.segment(),
                                              codec_opts,
                                              encoding_version).max_compressed_bytes_;

    auto config = storage::file::pack_config(path, data_size);
    LibraryPath lib_path{"file", fmt::format("{}", stream_id};
    auto library = create_library(ib_path, OpenMode::WRITE, {std::move(config)});
    LocalVersionedEngine versioned_engine{library};
}

} //namespace arcticdb