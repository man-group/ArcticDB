/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "util/constructors.hpp"
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/storage/file_backed/file_backed_storage.hpp>
#include <arcticdb/storage/file_backed/real_posix.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>

#include <asm-generic/errno.h>
#include <fmt/format.h>

#include <filesystem>
#include <random>
#include <sstream>
#include <system_error>
#include <unistd.h>

// TODO: Windows will be an entirely separate implementation. Some of the Storage class implementations will be
//       platform independent, so perhaps it is prudent to introduce some separate TUs per platform.
// TODO: Because of Windows, all my usages of c_str() are bad. They return wchar_t*.
#ifndef _WIN32
#include <fcntl.h>
#include <linux/limits.h>
#include <sys/stat.h>
#endif

namespace fs = std::filesystem;

namespace {

static const mode_t NEW_FILE_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;                     // 0644
static const mode_t DIR_MODE_MASK = S_ISUID | S_ISGID | S_ISVTX | S_IRWXU | S_IRWXG | S_IRWXO; // 07777

class FdCloser {
  public:
    FdCloser(int fd) noexcept : fd_(fd) {}
    ~FdCloser() { ::close(fd_); }

    ARCTICDB_NO_MOVE_OR_COPY(FdCloser);

  private:
    int fd_;
};

void write_all(int fd, std::span<const std::uint8_t> buffer) {
    // TODO: Use mock?
    const size_t buffer_size = buffer.size();
    size_t written = 0;
    while (written < buffer_size) {
        const ssize_t n = ::write(fd, buffer.data() + written, buffer_size - written);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            throw std::system_error(errno, std::generic_category(), "write failed");
        }
        written += static_cast<size_t>(n);
    }
}

void read_all(int fd, std::span<std::uint8_t> buffer) {
    // TODO: Use mock?
    size_t buffer_size = buffer.size();
    size_t read = 0;
    while (read < buffer_size) {
        const ssize_t n = ::read(fd, buffer.data(), buffer_size - read);
        if (n > 0)
            read += static_cast<size_t>(n);
        else if (n == 0)
            throw std::runtime_error(fmt::format("Unexpected EOF after {} of {} bytes", read, buffer_size));
        else if (errno != EINTR)
            throw std::system_error(errno, std::generic_category(), "read failed");
    }
}

void refresh_directory(const fs::path& dir) {
    int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd >= 0)
        ::close(fd);
}

/**
 * Open all directories from `dir` up to `subpath`, inclusive. This forces NFS to purge caches and
 * eliminates spurious ENOENT responses for files, which actually exist.
 * Note that we use real syscalls here. Testing should not be happening at this level.
 */
void refresh_ancestors(const fs::path& dir, const fs::path& subpath) {
    fs::path to_check = subpath;
    assert(to_check.has_parent_path());
    do {
        // TODO: The stopping condition here can be relaxed. The only reason we traverse parents one by one
        //       is because of the possibility that they are **all missing**. If one of them successfully opens,
        //       we can stop.
        to_check = to_check.parent_path();
        refresh_directory(to_check);
    } while (to_check != dir);
}

bool exists(
        arcticdb::storage::file_backed::PosixInterface& posix_, const fs::path& dir, const fs::path& subpath,
        bool allow_refresh = true
) {
    struct stat key_st;
    fs::path to_check = dir / subpath;
    if (posix_.stat(to_check.c_str(), &key_st) == 0)
        return true;

    if (errno != ENOENT)
        throw std::system_error(
                errno, std::generic_category(), fmt::format("do_key_exists() failed to stat {}", to_check.c_str())
        );

    if (allow_refresh) {
        refresh_ancestors(dir, to_check);
        return exists(posix_, dir, subpath, false);
    }

    return false;
}

} // namespace

namespace arcticdb::storage::file_backed {

FileBackedStorage::FileBackedStorage(const LibraryPath& library_path, OpenMode mode, const Config& conf) :
    Storage(library_path, mode),
    posix_(std::make_unique<RealPosix>()) {
    const auto lib_path_str = library_path.to_delim_path(fs::path::preferred_separator);
    const fs::path root_path = conf.dir_path();
    storage_dir_ = root_path / lib_path_str;

    // TODO: Should this use a safer primitive? Library creation is a rare event, but I think I will eventually
    // have the primitive I need.
    if (!fs::exists(storage_dir_)) {
        util::check_arg(
                mode > OpenMode::READ, "Missing dir {} for lib={}. mode={}", storage_dir_.c_str(), lib_path_str, mode
        );

        // TODO: It's important to set sensible permissions on these directories, so that group users have read access.
        fs::create_directories(storage_dir_);
    }
    struct stat st;
    if (::stat(storage_dir_.c_str(), &st) == 0) {
        new_dir_mode_ = st.st_mode & DIR_MODE_MASK;
    } else
        throw std::system_error(
                errno, std::generic_category(), fmt::format("Could not fstat library directory", storage_dir_.c_str())
        );
}

FileBackedStorage::~FileBackedStorage() = default;

std::string FileBackedStorage::name() const { return fmt::format("file_backed_storage-{}", storage_dir_.c_str()); }

// TODO: Remove all these maybe unused attributes.
// TODO: Need a bucketizer. For now I'll dump everything in the storage dir.
// TODO: There are some bare libc function calls in here which should be moved behind the PosixInterface.

void FileBackedStorage::do_write(KeySegmentPair& key_seg) {
    // TODO: The conversion to file name needs careful vetting.
    std::string maybe_safe_file_name_for_key = to_tokenized_key(key_seg.variant_key());
    if (maybe_safe_file_name_for_key.find('\0') != std::string::npos) {
        throw std::runtime_error(
                fmt::format("to_tokenized_key generated invalid file name! key: {}", key_seg.variant_key())
        );
    }
    Segment& seg = *key_seg.segment_ptr();
    // bool is_overwrite = std::holds_alternative<RefKey>(key_seg.variant_key());
    // TODO: With bucketizer, may have to create new directories with O_CREAT. Essential to set the directory
    // permissions.
    int dir_fd = posix_->open(storage_dir_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0);
    if (dir_fd < 0)
        throw std::system_error(
                errno,
                std::generic_category(),
                fmt::format("do_write() failed to open directory {}", storage_dir_.c_str())
        );

    const std::string tmp_name = generate_random_name();
    const fs::path tmp_file_path = storage_dir_ / tmp_name;
    {
        // TODO: Remembering to do this every time and only after checking < 0 seems error prone. Wrap file descriptors?
        FdCloser close_dir_fd(dir_fd);
        int tmp_key_fd =
                posix_->openat(dir_fd, tmp_name.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, NEW_FILE_MODE);
        if (tmp_key_fd < 0) {
            throw std::system_error(
                    errno,
                    std::generic_category(),
                    fmt::format("do_write() failed to create file {}", tmp_file_path.c_str())
            );
        }

        struct stat tmp_st;
        {
            FdCloser close_key_fd(tmp_key_fd);
            size_t segment_size = seg.calculate_size();
            std::vector<std::uint8_t> buffer(segment_size);
            seg.write_to(buffer.data());
            try {
                write_all(tmp_key_fd, buffer);
            } catch (...) {
                ::unlinkat(dir_fd, tmp_file_path.c_str(), 0);
                throw;
            }

            posix_->fsync(tmp_key_fd);
            posix_->fstat(tmp_key_fd, &tmp_st);
            // scope closes tmp_key_fd
        }
        // Makes the file durable for local file systems. No-op on NFS.
        posix_->fsync(dir_fd);
        // scope closes dir_fd
    }
    fs::path destination = storage_dir_ / maybe_safe_file_name_for_key;
    if (posix_->rename(tmp_file_path.c_str(), destination.c_str())) {
        const int err = errno;
        ::unlink(tmp_file_path.c_str());
        throw std::system_error(
                err,
                std::generic_category(),
                fmt::format("Failed to move {} to {}", tmp_file_path.c_str(), destination.c_str())
        );
    }
}
void FileBackedStorage::do_write_if_none(KeySegmentPair& kv [[maybe_unused]]) {
    throw std::runtime_error("do_write_if_none() not implemented");
}
void FileBackedStorage::do_update(KeySegmentPair& key_seg [[maybe_unused]], UpdateOpts opts [[maybe_unused]]) {
    throw std::runtime_error("do_update() not implemented");
}
void FileBackedStorage::do_read(
        VariantKey&& variant_key [[maybe_unused]], const ReadVisitor& visitor [[maybe_unused]],
        ReadKeyOpts opts [[maybe_unused]]
) {
    throw std::runtime_error("do_read with visitor not implemented");
}
KeySegmentPair FileBackedStorage::do_read(VariantKey&& variant_key, ReadKeyOpts) {
    // TODO: The conversion to file name needs careful vetting.
    std::string maybe_safe_file_name_for_key = to_tokenized_key(variant_key);
    fs::path dest_path = storage_dir_ / maybe_safe_file_name_for_key;
    int fd = posix_->open(dest_path.c_str(), O_RDONLY | O_CLOEXEC, 0);
    if (fd < 0) {
        if (errno == ENOENT) {
            refresh_ancestors(storage_dir_, dest_path);
            fd = posix_->open(dest_path.c_str(), O_RDONLY | O_CLOEXEC, 0);
            if (fd < 0)
                throw KeyNotFoundException(variant_key);
        } else { // TODO: Retry once on ESTALE too, without refresh
            throw KeyNotFoundException(variant_key);
        }
    }
    struct stat st;
    posix_->fstat(fd, &st);
    auto buffer = std::make_shared<Buffer>(st.st_size);
    read_all(fd, std::span<std::uint8_t>(buffer->data(), st.st_size));
    return {std::move(variant_key), Segment::from_buffer(buffer)};
}

void FileBackedStorage::do_remove(VariantKey&& variant_key [[maybe_unused]], RemoveOpts opts [[maybe_unused]]) {
    throw std::runtime_error("do_remove() not implemented");
}
void FileBackedStorage::do_remove(
        std::span<VariantKey> variant_keys [[maybe_unused]], RemoveOpts opts [[maybe_unused]]
) {
    throw std::runtime_error("do_remove() not implemented");
}

bool FileBackedStorage::do_key_exists(const VariantKey& key) {
    // TODO: The conversion to file name needs careful vetting.
    std::string maybe_safe_file_name_for_key = to_tokenized_key(key);
    return exists(*posix_, storage_dir_, maybe_safe_file_name_for_key);
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
) {
    throw std::runtime_error("do_visit_object_sizes() not implemented");
}

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
    // Shamelessly lifted off LMDB backend. The concept is the same.
#ifdef _WIN32
    // Windows directory names cannot end in '.' or whitespace.
    if (!path.empty() && (path.back() == '.' || std::isspace(path.back()))) {
        return path.back();
    }
#endif
    return std::nullopt;
}

std::string FileBackedStorage::generate_random_name() const {
    // TODO: Is this enough randomness? Seems legit to me. PID + TID + 32-bit random value ensures
    // no clashes with local processes. For other machines to clash, they need to accidentally get the same
    // PID, TID *and* they need to generate the same random value. The seed is also distinct every time.
    static thread_local std::mt19937 gen{std::random_device{}()};
    std::ostringstream ss;
    std::uint32_t random_number = gen();
    ss << ".tmp" << getpid() << gettid() << random_number;
    return ss.str();
}

} // namespace arcticdb::storage::file_backed
