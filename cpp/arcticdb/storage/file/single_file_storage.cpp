/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/storage/file/single_file_storage.hpp>

#include <filesystem>

#include <arcticdb/log/log.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/storage/constants.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/util/format_bytes.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage_utils.hpp>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <folly/gen/Base.h>

namespace arcticdb::storage::file {

SingleFileStorage::SingleFileStorage(const LibraryPath &lib, OpenMode mode, const Config &conf) :

    multi_segment_header_{stream_id, segments.size()};
{

    
}

uint8_t* SingleFileStorage::get_data_offset(const Segment& seg) {
    ARCTICDB_SAMPLE(SingleFileStorageGetOffset, 0)
    std::lock_guard<std::mutex> lock{offset_mutex_};
    auto* data = file_.data() + offset_;
    offset_ += seg.total_segment_size();
    return data;
}

void SingleFileStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    ARCTICDB_SAMPLE(SingleFileStorageWriteValues, 0)
    auto key_values = std::move(kvs);
    kvs.broadcast([this] (auto key_seg) {
        auto data = get_data_offset(key_seg.segment());
        ARCTICDB_SUBSAMPLE(FileStorageMemCpy, 0)
        key_seg.segment().write_to(data, hdr_sz);
    });
}

void SingleFileStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) {
    util::raise_rte("Update not implemented for file storages")
}

void SingleFileStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, storage::ReadKeyOpts) {
    ARCTICDB_SAMPLE(SingleFileStorageRead, 0)
}

bool SingleFileStorage::do_key_exists(const VariantKey&key) {
    ARCTICDB_SAMPLE(SingleFileStorageKeyExists, 0)
    return true;
}

void SingleFileStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) {
    util::raise_rte("Remove not implemented for file storages");
}

bool SingleFileStorage::do_fast_delete() {
    util::raise_rte("Fast delete not implemented for file storage - just delete the file");
}

void SingleFileStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) {
   util::raise_rte("Iterate type not implemented for file storage");
}


SingleFileStorage::~SingleFileStorage() {
}
} // namespace arcticdb::storage::lmdb
