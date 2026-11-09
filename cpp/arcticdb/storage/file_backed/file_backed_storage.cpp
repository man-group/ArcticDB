/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "entity/variant_key.hpp"
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/storage/file_backed/file_backed_storage.hpp>
#include <arcticdb/storage/file_backed/posix_interface.hpp>
#include <arcticdb/storage/file_backed/real_posix.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/util/constructors.hpp>

#include <fmt/format.h>

#include <filesystem>
#include <random>
#include <sstream>
#include <system_error>
#include <unistd.h>

// TODO: Windows will be an entirely separate implementation. Some of the Storage class implementations will be
//       platform independent, so perhaps it is prudent to introduce some separate TUs per platform.
// TODO: Because of Windows, all my usages of c_str() are bad. They return wchar_t*.
// TODO: The usages of `to_tokenized_key` need to be reconsidered. They are not compatible with Windows.
//       due to the use of '*' as separator.
#ifndef _WIN32
#include <fcntl.h>
#include <linux/limits.h>
#include <sys/stat.h>
#endif

namespace fs = std::filesystem;

namespace {

static const mode_t NEW_FILE_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;                     // 0644
static const mode_t DIR_MODE_MASK = S_ISUID | S_ISGID | S_ISVTX | S_IRWXU | S_IRWXG | S_IRWXO; // 07777
static constexpr std::string_view TMP_FILE_PREFIX(".tmp");
static constexpr std::string_view SILLY_RENAME_FILE_PREFIX(".nfs");

using arcticdb::storage::file_backed::PosixInterface;

std::string generate_tmp_file_name() {
    // TODO: Is this enough randomness? Seems legit to me. PID + TID + 32-bit random value ensures
    // no clashes with local processes. For other machines to clash, they need to accidentally get the same
    // PID, TID *and* they need to generate the same random value. The seed is also distinct every time.
    // TODO: On second thought, a timestamp + random value should be a lot more reliable collision-wise.
    static thread_local std::mt19937 gen{std::random_device{}()};
    std::ostringstream ss;
    std::uint32_t random_number = gen();
    ss << TMP_FILE_PREFIX << getpid() << gettid() << random_number;
    return ss.str();
}

bool is_tmp_file_name(std::string_view file_name) noexcept { return file_name.starts_with(TMP_FILE_PREFIX); }
bool is_silly_rename(std::string_view file_name) noexcept { return file_name.starts_with(SILLY_RENAME_FILE_PREFIX); }

bool skip_during_listing(std::string_view file_name) noexcept {
    return is_tmp_file_name(file_name) || is_silly_rename(file_name);
}

// TODO: pseudodir is not a real term.
bool is_pseudodir(std::string_view file_name) noexcept { return file_name == "." || file_name == ".."; }

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

class File {
  public:
    // TODO: storage_dir sneaked in here only because of the retry. Maybe the retry wrapper can absorb this concern.
    File(PosixInterface& posix, const fs::path& storage_dir, const fs::path& pathname, int flags, mode_t mode) :
        posix_(posix) {
        fd_ = posix.open(pathname.c_str(), flags, mode);
        // TODO: Use a retry wrapper
        if (fd_ < 0) {
            if (errno == ENOENT) {
                refresh_ancestors(storage_dir, pathname);
                fd_ = posix.open(pathname.c_str(), flags, mode);
            }
        }
        if (fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), fmt::format("Failed to open {}", pathname.c_str()));
        }
    }

    File(PosixInterface& posix, const File& dir_fd, const fs::path& pathname, int flags, mode_t mode) : posix_(posix) {
        fd_ = posix.openat(dir_fd.fd_, pathname.c_str(), flags, mode);
        // TODO Note that there is no retry here on ENOENT. It's awkward to refresh the ancestor directories, since we'd
        // have to close the fd and reopen it. Technically doable from within here. Do it later.
        if (fd_ < 0)
            throw std::system_error(
                    errno, std::generic_category(), fmt::format("Failed to openat {}", pathname.c_str())
            );
    }

    ~File() {
        // TODO: Check if there are any exceptions in flight. If anything goes wrong in here, we can't throw. If nothing
        // in flight, we can safely throw.
        if (needs_flush_) {
            posix_.fsync(fd_);
            // TODO: Not sure why I chose to stat the file too. Can't find documentation which says it's necessary.
            struct stat tmp_st;
            posix_.fstat(fd_, &tmp_st);
        }

        // TODO: Check this, and decide if safe to throw.
        [[maybe_unused]] int res = ::close(fd_);
    }

    ARCTICDB_NO_MOVE_OR_COPY(File);

    void write_all(PosixInterface& posix, std::span<const std::uint8_t> buffer) {
        const size_t buffer_size = buffer.size();
        size_t written = 0;
        while (written < buffer_size) {
            const ssize_t n = posix.write(fd_, buffer.data() + written, buffer_size - written);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                throw std::system_error(errno, std::generic_category(), "write failed");
            }
            written += static_cast<size_t>(n);
        }

        needs_flush_ = true;
    }

    std::shared_ptr<arcticdb::Buffer> read_all() const {
        struct stat st;
        posix_.fstat(fd_, &st);
        const size_t buffer_size = st.st_size;
        auto buffer = std::make_shared<arcticdb::Buffer>(buffer_size);
        size_t read = 0;
        while (read < buffer_size) {
            const ssize_t n = posix_.read(fd_, buffer->data(), buffer_size - read);
            if (n > 0)
                read += static_cast<size_t>(n);
            else if (n == 0)
                throw std::runtime_error(fmt::format("Unexpected EOF after {} of {} bytes", read, buffer_size));
            else if (errno != EINTR)
                throw std::system_error(errno, std::generic_category(), "read failed");
        }

        return buffer;
    }

    void sync() const {
        // TODO: Can this error? Yes... EINTR at the least should be retried. Then there's the usual ones
        // which write can return.
        int res = posix_.fsync(fd_);
        if (res < 0) {
            if (errno == EINTR) {
                // Try one more time? TODO: Quick retries should be wrapped in a retry() that takes a lambda.
                res = posix_.fsync(fd_);
            }
        }

        if (res < 0)
            throw std::system_error(errno, std::generic_category(), "fsync failed");
    }

    void mkdir(const fs::path& pathname, mode_t mode) const {
        int res = posix_.mkdirat(fd_, pathname.c_str(), mode);
        if (res < 0) {
            if (errno != EEXIST)
                throw std::system_error(
                        errno, std::generic_category(), fmt::format("failed to create subdir {}", pathname.c_str())
                );
        }
    }

    // Ignore errors intentionally. This should only be used in exception handlers, where we can't throw any more
    // errors.
    void unlinkat_best_effort(const fs::path& pathname) const { ::unlinkat(fd_, pathname.c_str(), 0); }

  private:
    PosixInterface& posix_;
    int fd_;
    bool needs_flush_ = false;
};

class DirCloser {
  public:
    DirCloser(DIR* dirp) noexcept : dirp_(dirp) {}
    ~DirCloser() { ::closedir(dirp_); }

    ARCTICDB_NO_MOVE_OR_COPY(DirCloser);

  private:
    DIR* dirp_;
};

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

void FileBackedStorage::do_write(KeySegmentPair& key_seg) {
    std::string maybe_safe_file_name_for_key = to_tokenized_key(key_seg.variant_key());
    Segment& seg = *key_seg.segment_ptr();
    // TODO: With bucketizer, may have to create new directories with O_CREAT. Essential to set the directory
    // permissions.

    const std::string tmp_file_name = generate_tmp_file_name();
    const std::string key_name = entity::key_type_long_name(variant_key_type(key_seg.variant_key()));
    const fs::path tmp_file_path = storage_dir_ / key_name / tmp_file_name;
    {
        File storage_dir_fd(*posix_, storage_dir_, storage_dir_, O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0);
        storage_dir_fd.mkdir(key_name, new_dir_mode_);
        File key_type_dir_fd(*posix_, storage_dir_fd, key_name, O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0);
        try {
            {
                File tmp_key_fd(
                        *posix_, key_type_dir_fd, tmp_file_name, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, NEW_FILE_MODE
                );
                size_t segment_size = seg.calculate_size();
                std::vector<std::uint8_t> buffer(segment_size);
                seg.write_to(buffer.data());
                tmp_key_fd.write_all(*posix_, buffer);
            }
            // After data is written, and file is closed, fsync on dir makes the file durable for local file systems.
            // No-op on NFS.
            // TODO: Maybe fsync() could be absorbed into the File class, if the tmp file's write signalled the
            //       containing directory that it needs to flush too. i.e. there is a chain of custody.
            storage_dir_fd.sync();
        } catch (...) {
            storage_dir_fd.unlinkat_best_effort(tmp_file_path);
            throw;
        }
    }
    // All file descriptors closed before rename.
    fs::path destination = storage_dir_ / key_name / maybe_safe_file_name_for_key;
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
    std::shared_ptr<Buffer> buffer;
    try {
        const std::string maybe_safe_file_name_for_key = to_tokenized_key(variant_key);
        const fs::path dest_path =
                storage_dir_ / entity::key_type_long_name(variant_key_type(variant_key)) / maybe_safe_file_name_for_key;
        File file(*posix_, storage_dir_, dest_path, O_RDONLY | O_CLOEXEC, 0);
        buffer = file.read_all();
    } catch (const std::system_error& e) {
        throw KeyNotFoundException(variant_key);
    }
    return {std::move(variant_key), Segment::from_buffer(buffer)};
}

void FileBackedStorage::do_remove(VariantKey&& variant_key, RemoveOpts opts) {
    const std::string maybe_safe_file_name_for_key = to_tokenized_key(variant_key);
    const fs::path dest_path =
            storage_dir_ / entity::key_type_long_name(variant_key_type(variant_key)) / maybe_safe_file_name_for_key;
    int res = posix_->unlink(dest_path.c_str());
    if (res) {
        if (errno == ENOENT) {
            if (!opts.ignores_missing_key_)
                throw KeyNotFoundException(variant_key);
            else
                log::storage().warn("Failed to delete segment for key {}", variant_key_view(variant_key));
        }
        // TODO: Should we retry on some of the errors? e.g. EBUSY should be ignored -> silly rename.
    }
}

void FileBackedStorage::do_remove(
        std::span<VariantKey> variant_keys [[maybe_unused]], RemoveOpts opts [[maybe_unused]]
) {
    // TODO: Factor out and reuse this code
    std::vector<VariantKey> failed_deletes;
    for (const VariantKey& variant_key : variant_keys) {
        const std::string maybe_safe_file_name_for_key = to_tokenized_key(variant_key);
        const fs::path dest_path =
                storage_dir_ / entity::key_type_long_name(variant_key_type(variant_key)) / maybe_safe_file_name_for_key;
        int res = posix_->unlink(dest_path.c_str());
        if (res) {
            if (errno == ENOENT) {
                if (!opts.ignores_missing_key_)
                    failed_deletes.emplace_back(variant_key);
                else
                    log::storage().warn("Failed to delete segment for key {}", variant_key_view(variant_key));
            }
            // TODO: Should we retry on some of the errors? e.g. EBUSY should be ignored -> silly rename.
            //       I think the real bug here is that we just report back that the key is *missing*. It may exist
            //       and we just be failing to delete. That needs to be reported.
        }
    }

    if (!failed_deletes.empty())
        throw KeyNotFoundException(std::move(failed_deletes));
}

bool FileBackedStorage::do_key_exists(const VariantKey& key) {
    const fs::path subpath = fs::path(entity::key_type_long_name(variant_key_type(key))) / to_tokenized_key(key);
    return exists(*posix_, storage_dir_, subpath);
}

bool FileBackedStorage::do_fast_delete() { throw std::runtime_error("do_fast_delete not implemented"); }

bool FileBackedStorage::do_iterate_type_until_match(
        KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix
) {
    const fs::path keys_dir = storage_dir_ / entity::key_type_long_name(key_type);
    DIR* dirp = posix_->opendir(keys_dir.c_str());
    if (!dirp)
        return false;

    DirCloser close_dir(dirp);
    struct dirent* dir_entry = nullptr;
    while ((dir_entry = posix_->readdir(dirp))) {
        std::string_view file_name(dir_entry->d_name);
        if (!file_name.starts_with(prefix) || skip_during_listing(file_name) || is_pseudodir(file_name))
            continue;

        VariantKey key{variant_key_from_bytes(
                reinterpret_cast<uint8_t*>(dir_entry->d_name), strlen(dir_entry->d_name), key_type
        )};
        if (variant_key_type(key) == key_type && visitor(std::move(key)))
            return true;
    }

    return false;
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

} // namespace arcticdb::storage::file_backed
