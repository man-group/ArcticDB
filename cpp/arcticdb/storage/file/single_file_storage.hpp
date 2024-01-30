/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/util/memory_mapped_file.hpp>
#include <arcticdb/storage/coalesced/multi_segment_header.hpp>

namespace fs = std::filesystem;

namespace arcticdb::storage::file {

class SingleFileStorage final : public Storage {
  public:
    using Config = arcticdb::proto::single_file_storage::Config;

    SingleFileStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

    ~SingleFileStorage();

  private:
    void do_write_raw(uint8_t* data, size_t bytes) final;

    void do_write(Composite<KeySegmentPair>&& kvs) final;

    void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) final;

    void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, storage::ReadKeyOpts opts) final;

    void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) final;

    bool do_supports_prefix_matching() const final {
        return false;
    };

    std::string do_key_path(const VariantKey&) const final { return {}; }

    inline bool do_fast_delete() final;

    void do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) final;

    bool do_key_exists(const VariantKey & key) final;

    uint64_t get_data_offset(const Segment& seg);

    uint64_t write_segment(Segment&& seg);

    void init();

private:
    std::mutex offset_mutex_;
    size_t offset_ = 0UL;
    Config config_;
    MemoryMappedFile file_;
    storage::MultiSegmentHeader multi_segment_header_;
};

inline arcticdb::proto::storage::VariantStorage pack_config(
        const std::string& path,
        size_t file_size,
        size_t items_count,
        const StreamId& id,
        IndexDescriptor index_desc,
        EncodingVersion encoding_version) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::single_file_storage::Config cfg;
    cfg.set_path(path);
    cfg.set_bytes(file_size);
    cfg.set_items_count(items_count);
    util::variant_match(id,
                            [&cfg] (const StringId& str) { cfg.set_str_id(str); },
                            [&cfg] (const NumericId& n) { cfg.set_num_id(n); });
    cfg.mutable_index()->CopyFrom(index_desc.proto()),
    cfg.set_encoding_version(static_cast<uint32_t>(encoding_version));
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

inline arcticdb::proto::storage::VariantStorage pack_config(const std::string& path) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::single_file_storage::Config cfg;
    cfg.set_path(path);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}
} //namespace arcticdb::storage::file
