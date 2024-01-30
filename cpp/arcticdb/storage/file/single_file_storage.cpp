/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/storage/file/single_file_storage.hpp>

#include <arcticdb/log/log.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/storage/constants.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/serialized_key.hpp>

namespace arcticdb::storage::file {

SingleFileStorage::SingleFileStorage(const LibraryPath &lib, OpenMode mode, const Config &conf) :
    Storage(lib, mode),
    config_(conf) {
    init();
}

void SingleFileStorage::do_write_raw(uint8_t *data, size_t bytes) {
    memcpy(file_.data() + offset_, data, bytes);
    offset_ += bytes;
}

void SingleFileStorage::init() {
    if (config_.bytes() > 0) {
        multi_segment_header_.initalize(StreamId{0}, config_.items_count());
        auto data_size = config_.bytes() + max_compressed_size_dispatch(multi_segment_header_.segment(),
            config_.codec_opts(),
            EncodingVersion{
            static_cast<uint16_t>(config_.encoding_version())}).max_compressed_bytes_;
        StreamId id = config_.has_str_id() ? StreamId{} : NumericId{};
        data_size += entity::max_key_size(id, IndexDescriptor{config_.index()});
        file_.create_file(config_.path(), data_size);
    } else {
        file_.open_file(config_.path());
        const auto data_end = file_.bytes() - sizeof(uint64_t);
        auto index_offset = *reinterpret_cast<uint64_t*>(file_.data() + data_end);
        auto index_segment = Segment::from_bytes(file_.data() + index_offset, data_end - index_offset);
        auto header = decode_segment(std::move(index_segment));
        multi_segment_header_.set_segment(std::move(header));
    }
}

uint64_t SingleFileStorage::get_data_offset(const Segment& seg) {
    ARCTICDB_SAMPLE(SingleFileStorageGetOffset, 0)
    std::lock_guard<std::mutex> lock{offset_mutex_};
    offset_ += seg.total_segment_size();
    return offset_;
}

uint64_t SingleFileStorage::write_segment(Segment&& seg) {
    auto offset = get_data_offset(seg);
    auto* data = file_.data() + offset;
    const auto header_size = seg.segment_header_bytes_size();
    const auto segment_size = seg.total_segment_size(header_size);
    ARCTICDB_SUBSAMPLE(FileStorageMemCpy, 0)
    seg.write_to(data, header_size);
    return segment_size;
}

void SingleFileStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    ARCTICDB_SAMPLE(SingleFileStorageWriteValues, 0)
    auto key_values = std::move(kvs);
    kvs.broadcast([this] (auto key_seg) {
        const auto size = key_seg.segment().total_segment_size();
        const auto offset = write_segment(std::move(key_seg.segment()));
        multi_segment_header_.add_key_and_offset(key_seg.atom_key(), offset, size);
    });
}

void SingleFileStorage::do_update(Composite<KeySegmentPair>&&, UpdateOpts) {
    util::raise_rte("Update not implemented for file storages");
}

void SingleFileStorage::do_read(Composite<VariantKey>&&, const ReadVisitor&, storage::ReadKeyOpts) {
    ARCTICDB_SAMPLE(SingleFileStorageRead, 0)
}

bool SingleFileStorage::do_key_exists(const VariantKey&) {
    ARCTICDB_SAMPLE(SingleFileStorageKeyExists, 0)
    return true;
}

void SingleFileStorage::do_remove(Composite<VariantKey>&&, RemoveOpts) {
    util::raise_rte("Remove not implemented for file storages");
}

bool SingleFileStorage::do_fast_delete() {
    util::raise_rte("Fast delete not implemented for file storage - just delete the file");
}

void SingleFileStorage::do_iterate_type(KeyType, const IterateTypeVisitor&, const std::string&) {
   util::raise_rte("Iterate type not implemented for file storage");
}

SingleFileStorage::~SingleFileStorage() {
    multi_segment_header_.sort();
    auto header_segment = encode_dispatch(multi_segment_header_.detach_segment(), config_.codec_opts(), EncodingVersion{static_cast<uint16_t>(config_.encoding_version())});
    const auto offset = write_segment(std::move(header_segment));
    auto pos = reinterpret_cast<uint64_t*>(file_.data() + offset_);
    *pos = offset;
    offset_ += sizeof(uint64_t);
    file_.truncate(offset_);
}
} // namespace arcticdb::storage::lmdb
