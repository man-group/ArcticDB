/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/storage/lmdb/lmdb_client_interface.hpp>
#include <arcticdb/storage/lmdb/lmdb.hpp>
#include <filesystem>

namespace fs = std::filesystem;

namespace arcticdb::storage::lmdb {

struct LmdbInstance {
    ::lmdb::env env_;
    std::unordered_map<std::string, std::unique_ptr<::lmdb::dbi>> dbi_by_key_type_;
};

class LmdbStorage final : public Storage {
  public:
    using Config = arcticdb::proto::lmdb_storage::Config;
    static void reset_warning_counter();

    LmdbStorage(const LibraryPath& lib, OpenMode mode, const Config& conf);
    LmdbStorage(LmdbStorage&& other) noexcept;
    ~LmdbStorage() override;

    std::string name() const final;

  private:
    void do_write(KeySegmentPair& key_seg) final;

    void do_write_if_none(KeySegmentPair& kv [[maybe_unused]]) final {
        storage::raise<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>("Atomic operations are only supported for s3 backend"
        );
    };

    void do_update(KeySegmentPair& key_seg, UpdateOpts opts) final;

    void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, storage::ReadKeyOpts opts) final;

    KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts) final;

    void do_remove(VariantKey&& variant_key, RemoveOpts opts) final;

    void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) final;

    bool do_supports_prefix_matching() const final { return false; };

    SupportsAtomicWrites do_supports_atomic_writes() const final { return SupportsAtomicWrites::NO; }

    inline bool do_fast_delete() final;

    void cleanup() override;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix)
            final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_is_path_valid(std::string_view path) const final;

    ::lmdb::env& env();

    ::lmdb::dbi& get_dbi(const std::string& db_name);

    std::string do_key_path(const VariantKey&) const final { return {}; };

    void warn_if_lmdb_already_open();

    // _internal methods assume the write mutex is already held
    void do_write_internal(KeySegmentPair& key_seg, ::lmdb::txn& txn);
    boost::container::small_vector<VariantKey, 1> do_remove_internal(
            std::span<VariantKey> variant_key, ::lmdb::txn& txn, RemoveOpts opts
    );
    std::unique_ptr<std::mutex> write_mutex_;
    std::shared_ptr<LmdbInstance> lmdb_instance_;

    std::filesystem::path lib_dir_;

    std::unique_ptr<LmdbClientWrapper> lmdb_client_;

    // For log warning only
    // Number of times an LMDB path has been opened. See also reinit_lmdb_warning.
    // Opening an LMDB env over the same path twice in the same process is unsafe, so we warn the user about it.
    inline static std::unordered_map<std::string, uint64_t> times_path_opened;
};

inline arcticdb::proto::storage::VariantStorage pack_config(const std::string& path) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::lmdb_storage::Config cfg;
    cfg.set_path(path);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

} // namespace arcticdb::storage::lmdb
