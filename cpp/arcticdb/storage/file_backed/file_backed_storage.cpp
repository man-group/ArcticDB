/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "storage/storage.hpp"
#include <arcticdb/storage/file_backed/file_backed_storage.hpp>
#include <filesystem>
#include <linux/limits.h>

// TODO: Windows will be an entirely separate implementation. Some of the Storage class implementations will be
//       platform independent, so perhaps it is prudent to introduce some separate TUs per platform.
#ifndef _WIN32
#include <sys/stat.h>
#endif

namespace fs = std::filesystem;

namespace {}

namespace arcticdb::storage::file_backed {

FileBackedStorage::FileBackedStorage(const LibraryPath& library_path, OpenMode mode, const Config& conf) :
    Storage(library_path, mode) {
    const auto lib_path_str = library_path.to_delim_path(fs::path::preferred_separator);
    const fs::path root_path = conf.dir_path();
    storage_dir_ = root_path / lib_path_str;

    // TODO: Should this use a safer primitive? Library creation is a rare event, but I think I will eventually
    // have the primitive I need.
    if (!fs::exists(storage_dir_)) {
        util::check_arg(
                mode > OpenMode::READ, "Missing dir {} for lib={}. mode={}", storage_dir_.c_str(), lib_path_str, mode
        );

        fs::create_directories(storage_dir_);
    }
}

FileBackedStorage::~FileBackedStorage() = default;

std::string FileBackedStorage::name() const { return fmt::format("file_backed_storage-{}", storage_dir_.c_str()); }

// TODO: Remove all these maybe unused attributes.
void FileBackedStorage::do_write(KeySegmentPair& key_seg [[maybe_unused]]) {}
void FileBackedStorage::do_write_if_none(KeySegmentPair& kv [[maybe_unused]]) {}
void FileBackedStorage::do_update(KeySegmentPair& key_seg [[maybe_unused]], UpdateOpts opts [[maybe_unused]]) {}
void FileBackedStorage::do_read(
        VariantKey&& variant_key [[maybe_unused]], const ReadVisitor& visitor [[maybe_unused]],
        ReadKeyOpts opts [[maybe_unused]]
) {}
KeySegmentPair FileBackedStorage::do_read(
        VariantKey&& variant_key [[maybe_unused]], ReadKeyOpts opts [[maybe_unused]]
) {
    throw std::runtime_error("do_read not implemented");
}
void FileBackedStorage::do_remove(VariantKey&& variant_key [[maybe_unused]], RemoveOpts opts [[maybe_unused]]) {}
void FileBackedStorage::do_remove(
        std::span<VariantKey> variant_keys [[maybe_unused]], RemoveOpts opts [[maybe_unused]]
) {}
bool FileBackedStorage::do_key_exists(const VariantKey& key [[maybe_unused]]) {
    throw std::runtime_error("do_key_exists not implemented");
}
bool FileBackedStorage::do_fast_delete() { throw std::runtime_error("do_fast_delete not implemented"); }

bool FileBackedStorage::do_iterate_type_until_match(
        KeyType key_type [[maybe_unused]], const IterateTypePredicate& visitor [[maybe_unused]],
        const std::string& prefix [[maybe_unused]]
) {
    throw std::runtime_error("do_iterate_type_until_match not implemented");
}
void FileBackedStorage::do_visit_object_sizes(
        [[maybe_unused]] KeyType key_type, [[maybe_unused]] const std::string& prefix,
        [[maybe_unused]] const ObjectSizesVisitor& visitor
) {}
[[nodiscard]] std::string FileBackedStorage::do_key_path(const VariantKey& key [[maybe_unused]]) const {
    throw std::runtime_error("do_key_path not implemented");
}
[[nodiscard]] const std::set<char>& FileBackedStorage::do_unsupported_symbol_chars() const {
    // TODO: Revisit this, it's wrong. Compare with LMDB, think carefully about Windows.
    return GLOBALLY_UNSUPPORTED_CHARS;
}
[[nodiscard]] const std::set<char>& FileBackedStorage::do_unsupported_library_chars() const {
    // TODO: Revisit this, it's wrong. Compare with LMDB, think carefully about Windows.
    return GLOBALLY_UNSUPPORTED_CHARS;
}
[[nodiscard]] std::optional<char> FileBackedStorage::do_verify_library_suffix(std::string_view) const {
    throw std::runtime_error("do_verify_library_suffix not implemented");
}

} // namespace arcticdb::storage::file_backed
