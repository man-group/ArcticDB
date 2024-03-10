/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/storage/file/mapped_file_storage.hpp>

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

MappedFileStorage::MappedFileStorage(const LibraryPath &lib, OpenMode mode, Config conf) :
    SingleFileStorage(lib, mode),
    config_(std::move(conf)) {
    init();
}

void MappedFileStorage::do_write_raw(const uint8_t* data, size_t bytes) {
    ARCTICDB_DEBUG(log::storage(), "Writing {} bytes to mapped file storage at offset {}", bytes, offset_);
    memcpy(file_.data() + offset_, data, bytes);
    offset_ += bytes;
}

void MappedFileStorage::init() {
    ARCTICDB_DEBUG(log::storage(), "Creating file with config {}", config_.DebugString());
    if (config_.bytes() > 0) {
        ARCTICDB_DEBUG(log::storage(), "Creating new mapped file storage at path {}", config_.path());
        multi_segment_header_.initalize(StreamId{NumericId{0}}, config_.items_count());
        auto data_size = config_.bytes() + max_compressed_size_dispatch(multi_segment_header_.segment(),
            config_.codec_opts(),
            EncodingVersion{
            static_cast<uint16_t>(config_.encoding_version())}).max_compressed_bytes_;
        StreamId id = config_.has_str_id() ? StreamId{} : NumericId{};
        data_size += entity::max_key_size(id, IndexDescriptor{config_.index()});
        file_.create_file(config_.path(), data_size);
    } else {
        ARCTICDB_DEBUG(log::storage(), "Opening existing mapped file storage at path {}", config_.path());
        file_.open_file(config_.path());
    }
}

SegmentInMemory MappedFileStorage::read_segment(size_t offset, size_t bytes) const  {
    auto index_segment = Segment::from_bytes(file_.data() + offset, bytes);
    return decode_segment(std::move(index_segment));
}

void MappedFileStorage::do_load_header(size_t header_offset, size_t header_size) {
    auto header = read_segment(header_offset, header_size);
    ARCTICDB_DEBUG(log::storage(), "Loaded mapped file header of size {}", header.row_count());
    multi_segment_header_.set_segment(std::move(header));
}

uint64_t MappedFileStorage::get_data_offset(const Segment& seg, size_t header_size) {
    ARCTICDB_SAMPLE(MappedFileStorageGetOffset, 0)
    std::lock_guard lock{offset_mutex_};
    const auto segment_size = seg.total_segment_size(header_size);
    ARCTICDB_DEBUG(log::storage(), "Mapped file storage returning offset {} and adding {} bytes", offset_, segment_size);
    const auto previous_offset = offset_;
    offset_ += segment_size;
    return previous_offset;
}

uint64_t MappedFileStorage::write_segment(Segment&& seg) {
    auto segment = std::move(seg);
    const auto header_size = segment.segment_header_bytes_size();
    auto offset = get_data_offset(segment, header_size);
    auto* data = file_.data() + offset;
    ARCTICDB_SUBSAMPLE(FileStorageMemCpy, 0)
    ARCTICDB_DEBUG(log::storage(), "Mapped file storage writing segment of size {} at offset {}",  segment.total_segment_size(header_size), offset);
    segment.write_to(data, header_size);
    return offset;
}

void MappedFileStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    ARCTICDB_SAMPLE(MappedFileStorageWriteValues, 0)
    auto key_values = std::move(kvs);
    key_values.broadcast([this] (auto key_seg) {
        const auto size = key_seg.segment().total_segment_size();
        const auto offset = write_segment(std::move(key_seg.segment()));
        multi_segment_header_.add_key_and_offset(key_seg.atom_key(), offset, size);
    });
}

void MappedFileStorage::do_update(Composite<KeySegmentPair>&&, UpdateOpts) {
    util::raise_rte("Update not implemented for file storages");
}

void MappedFileStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, storage::ReadKeyOpts) {
    ARCTICDB_SAMPLE(MappedFileStorageRead, 0)
    auto keys = std::move(ks);
    keys.broadcast([&visitor, this] (const auto& key) {
        auto maybe_offset = multi_segment_header_.get_offset_for_key(to_atom(key));
        util::check(maybe_offset.has_value(), "Failed to find key {} in file", key);
        auto [offset, bytes] = std::move(maybe_offset.value());
        auto segment = Segment::from_bytes(file_.data() + offset, bytes);
        visitor(key, std::move(segment));
    });
}

bool MappedFileStorage::do_key_exists(const VariantKey& key) {
    ARCTICDB_SAMPLE(MappedFileStorageKeyExists, 0)
    return multi_segment_header_.get_offset_for_key(to_atom(key)) != std::nullopt;
}

void MappedFileStorage::do_remove(Composite<VariantKey>&&, RemoveOpts) {
    util::raise_rte("Remove not implemented for file storages");
}

bool MappedFileStorage::do_fast_delete() {
    util::raise_rte("Fast delete not implemented for file storage - just delete the file");
}

void MappedFileStorage::do_finalize(KeyData key_data)  {
    multi_segment_header_.sort();
    auto header_segment = encode_dispatch(multi_segment_header_.detach_segment(),
                                          config_.codec_opts(),
                                          EncodingVersion{static_cast<uint16_t>(config_.encoding_version())});
    write_segment(std::move(header_segment));
    auto pos = reinterpret_cast<KeyData*>(file_.data() + offset_);
    *pos = key_data;
    ARCTICDB_DEBUG(log::storage(), "Finalizing mapped file, writing key data {}", *pos);
    offset_ += sizeof(KeyData);
    file_.truncate(offset_);
}

uint8_t* MappedFileStorage::do_read_raw(size_t offset, size_t bytes) {
    util::check(offset + bytes <= file_.bytes(), "Can't read {} bytes from {} in file of size {},",
                                                 bytes, offset, file_.bytes());
    ARCTICDB_DEBUG(log::storage(), "Mapped file storage returning raw offset {} for {} bytes", offset, bytes);
    return file_.data() + offset;
}

void MappedFileStorage::do_iterate_type(KeyType, const IterateTypeVisitor&, const std::string&) {
   util::raise_rte("Iterate type not implemented for file storage");
}

size_t MappedFileStorage::do_get_offset() const {
    return offset_;
}

size_t MappedFileStorage::do_get_bytes() const {
    return file_.bytes();
}
} // namespace arcticdb::storage

