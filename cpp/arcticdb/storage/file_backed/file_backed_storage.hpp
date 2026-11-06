/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/file_backed/posix_interface.hpp>

#include <filesystem>
#include <memory>

namespace arcticdb::storage::file_backed {

class FileBackedStorage final : public Storage {
  public:
    using Config = arcticdb::proto::file_storage::Config;

    FileBackedStorage(const LibraryPath& library_path, OpenMode mode, const Config& conf);
    ~FileBackedStorage() override;

    bool supports_object_size_calculation() const override { return true; }
    std::string name() const override;

  private:
    void do_write(KeySegmentPair& key_seg) override;
    void do_write_if_none(KeySegmentPair& kv) override;
    void do_update(KeySegmentPair& key_seg, UpdateOpts opts) override;
    void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) override;
    KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts opts) override;
    void do_remove(VariantKey&& variant_key, RemoveOpts opts) override;
    void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) override;
    bool do_key_exists(const VariantKey& key) override;
    bool do_supports_prefix_matching() const override { return false; };
    SupportsAtomicWrites do_supports_atomic_writes() const override {
        // TODO: I don't fully understand what this is for. It almost seems like dead code.
        // Nothing in the arcticdb core uses the ReliableStorageLock, but perhaps it could?
        // We should be able to ensure atomic writes on NFS.
        return SupportsAtomicWrites::YES;
    };
    bool do_fast_delete() override;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix)
            override;
    void do_visit_object_sizes(
            [[maybe_unused]] KeyType key_type, [[maybe_unused]] const std::string& prefix,
            [[maybe_unused]] const ObjectSizesVisitor& visitor
    ) override;
    [[nodiscard]] std::string do_key_path(const VariantKey& key) const override;
    [[nodiscard]] const std::set<char>& do_unsupported_symbol_chars() const override;
    [[nodiscard]] const std::set<char>& do_unsupported_library_chars() const override;
    [[nodiscard]] std::optional<char> do_verify_library_suffix(std::string_view) const override;

  private:
    std::string generate_random_name() const;

  private:
    std::filesystem::path storage_dir_;
    // TODO: Need to collect the process umask and chmod newly created directories if needed.
    mode_t new_dir_mode_;
    std::unique_ptr<PosixInterface> posix_;
};

} // namespace arcticdb::storage::file_backed
