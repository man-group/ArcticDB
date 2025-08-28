/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/single_file_storage.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/util/memory_mapped_file.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/storage/coalesced/multi_segment_header.hpp>

namespace fs = std::filesystem;

namespace arcticdb::storage::file {

class MappedFileStorage final : public SingleFileStorage {
  public:
    using Config = arcticdb::proto::mapped_file_storage::Config;

    MappedFileStorage(const LibraryPath &lib, OpenMode mode, Config conf);

    ~MappedFileStorage() override = default;

    std::string name() const final;

  private:
    void do_write_raw(const uint8_t* data, size_t bytes) override;

    void do_write(KeySegmentPair& key_seg) override;

    void do_write_if_none(KeySegmentPair& kv [[maybe_unused]]) final {
        storage::raise<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>("Atomic operations are only supported for s3 backend");
    };

    void do_update(KeySegmentPair& key_seg, UpdateOpts opts) override;

    void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, storage::ReadKeyOpts opts) override;

    KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts) final;

    void do_remove(VariantKey&& variant_key, RemoveOpts opts) override;

    void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) final;

    bool do_supports_prefix_matching() const override {
        return false;
    };

    SupportsAtomicWrites do_supports_atomic_writes() const final {
        return SupportsAtomicWrites::NO;
    }

    std::string do_key_path(const VariantKey&) const override { return {}; }

    bool do_fast_delete() override;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string &prefix) override;

    bool do_key_exists(const VariantKey & key) override;

    size_t do_get_offset() const override;

    void do_finalize(KeyData key_data) override;

    uint64_t get_data_offset(const Segment& seg);

    void do_load_header(size_t header_offset, size_t header_size) override;

    uint64_t write_segment(Segment& seg);

    uint8_t* do_read_raw(size_t offset, size_t bytes) override;

    size_t do_get_bytes() const override;

    void init();

    SegmentInMemory read_segment(size_t offset, size_t bytes) const;

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
        const IndexDescriptorImpl& index_desc,
        EncodingVersion encoding_version,
        const arcticdb::proto::encoding::VariantCodec& codec_opts) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::mapped_file_storage::Config cfg;
    cfg.set_path(path);
    cfg.set_bytes(file_size);
    cfg.set_items_count(items_count);
    util::variant_match(id,
                            [&cfg] (const StringId& str) { cfg.set_str_id(str); },
                            [&cfg] (const NumericId& n) { cfg.set_num_id(n); });
    cfg.mutable_index()->CopyFrom(index_descriptor_to_proto(index_desc)),
    cfg.set_encoding_version(static_cast<uint32_t>(encoding_version));
    cfg.mutable_codec_opts()->CopyFrom(codec_opts);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

inline arcticdb::proto::storage::VariantStorage pack_config(
    const std::string& path,
    const arcticdb::proto::encoding::VariantCodec& codec_opts) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::mapped_file_storage::Config cfg;
    cfg.set_path(path);
    cfg.mutable_codec_opts()->CopyFrom(codec_opts);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}
} //namespace arcticdb::storage::file
