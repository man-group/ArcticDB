/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

// TODO: Windows will be an entirely separate implementation. Some of the Storage class implementations will be
//       platform independent, so perhaps it is prudent to introduce some separate TUs per platform.
// TODO: Because of Windows, all my usages of c_str() are bad. They return wchar_t*.
// TODO: The usages of `to_tokenized_key` need to be reconsidered. They are not compatible with Windows.
//       due to the use of '*' as separator.

#ifdef __linux__

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/file_backed/file_backed_storage.hpp>
#include <arcticdb/storage/file_backed/posix_interface.hpp>
#include <arcticdb/storage/file_backed/real_posix.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/error_code.hpp>

#include <boost/iterator/iterator_facade.hpp>
#include <fmt/format.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <ranges>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>

namespace fs = std::filesystem;

namespace {

static const mode_t NEW_FILE_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;                     // 0644
static const mode_t DIR_MODE_MASK = S_ISUID | S_ISGID | S_ISVTX | S_IRWXU | S_IRWXG | S_IRWXO; // 07777
static constexpr std::string_view TMP_FILE_PREFIX(".tmp");
static constexpr std::string_view SILLY_RENAME_FILE_PREFIX(".nfs");

using namespace arcticdb;
using storage::file_backed::PosixInterface;

// TODO: Now that everything in here is built on raise<>, it makes sense for arguments to be Args...&&
void raise_io_error(int error_code, std::string_view message) {
    switch (error_code) {
    case EACCES:
    case EPERM:
    case EROFS:
        storage::raise<ErrorCode::E_PERMISSION>("{}: {}", message, std::strerror(error_code));
        break;
    case ENOSPC:
    case EDQUOT:
        storage::raise<ErrorCode::E_QUOTA_EXCEEDED>(message);
        break;
    case ENOENT:
        throw storage::KeyNotFoundException(std::string{message});
        break;
    case ESTALE:
        storage::raise<ErrorCode::E_STALE_FILE_DESCRIPTOR>(message);
        break;
    case ENOTDIR:
    default:
        storage::raise<ErrorCode::E_UNEXPECTED_FILE_STORAGE_ERROR>("{}: {}", message, std::strerror(error_code));
        break;
    }
}

// File name, which is unlikely to collide across consecutive function calls. The pid is useful because a
// forked process would inherit this particular generator's seed and produce an identical sequence of values.
std::string generate_tmp_file_name() {
    static thread_local std::mt19937_64 gen{std::random_device{}()};
    return fmt::format("{}_{}_{}", TMP_FILE_PREFIX, getpid(), gen());
}

fs::path get_key_subpath(const VariantKey& key) {
    return fs::path(entity::key_type_long_name(variant_key_type(key))) / to_tokenized_key(key);
}

bool is_tmp_file_name(std::string_view file_name) noexcept { return file_name.starts_with(TMP_FILE_PREFIX); }
bool is_silly_rename(std::string_view file_name) noexcept { return file_name.starts_with(SILLY_RENAME_FILE_PREFIX); }

bool is_dot_dir(std::string_view file_name) noexcept { return file_name == "." || file_name == ".."; }

bool is_skipped_name(std::string_view file_name) noexcept {
    return is_tmp_file_name(file_name) || is_silly_rename(file_name) || is_dot_dir(file_name);
}

void refresh_directory(PosixInterface& posix, const fs::path& dir) {
    int fd = posix.open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0);
    if (fd >= 0)
        (void)posix.close(fd);
}

/**
 * Open all directories from `dir` up to `subpath`, inclusive. This forces NFS to purge caches and
 * eliminates spurious ENOENT responses for files, which actually exist.
 * Note that we use real syscalls here. Testing should not be happening at this level.
 */
void refresh_ancestors(PosixInterface& posix, const fs::path& dir, const fs::path& subpath) {
    fs::path to_check = subpath;
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            to_check.has_parent_path() && dir.is_absolute() && subpath.is_absolute(),
            "Expected dir {} and subpath {} to be absolute paths, and dir should be a parent of subpath",
            dir.c_str(),
            subpath.c_str()
    );
    while (to_check != dir) {
        // TODO: The stopping condition here can be relaxed. The only reason we traverse parents one by one
        //       is because of the possibility that they are **all missing**. If one of them successfully opens,
        //       we can stop.
        to_check = to_check.parent_path();
        refresh_directory(posix, to_check);
    }
}

bool exists(PosixInterface& posix, const fs::path& dir, const fs::path& subpath, bool allow_refresh = true) {
    struct stat key_st;
    fs::path to_check = dir / subpath;
    if (posix.stat(to_check.c_str(), &key_st) == 0)
        return true;

    if (errno != ENOENT)
        raise_io_error(errno, fmt::format("do_key_exists() failed to stat {}", to_check.c_str()));

    if (allow_refresh) {
        refresh_ancestors(posix, dir, to_check);
        return exists(posix, dir, subpath, false);
    }

    return false;
}

class FdHolder {
  protected:
    // TODO: storage_dir sneaked in here only because of the retry. Maybe the retry wrapper can absorb this concern.
    FdHolder(PosixInterface& posix, const fs::path& storage_dir, const fs::path& pathname, int flags, mode_t mode) :
        posix_(posix) {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                (flags & O_CLOEXEC) != 0,
                "For fork safety, mode O_CLOEXEC should always be set"
        );
        fd_ = posix.open(pathname.c_str(), flags, mode);
        if (fd_ < 0) {
            if (errno == ENOENT) {
                // Only occurs without O_CREAT
                refresh_ancestors(posix, storage_dir, pathname);
                fd_ = posix.open(pathname.c_str(), flags, mode);
            }
        }
        if (fd_ < 0) {
            raise_io_error(errno, fmt::format("Failed to open {}", pathname.c_str()));
        }
    }

    FdHolder(PosixInterface& posix, const FdHolder& dir_fd, const fs::path& pathname, int flags, mode_t mode) :
        posix_(posix) {
        fd_ = posix.openat(dir_fd.fd_, pathname.c_str(), flags, mode);
        // TODO Note that there is no retry here on ENOENT. It's awkward to refresh the ancestor directories, since we'd
        // have to close the fd and reopen it. Technically doable from within here. Do it later.
        if (fd_ < 0)
            raise_io_error(errno, fmt::format("Failed to openat {}", pathname.c_str()));
    }

    ~FdHolder() {
        if (fd_ >= 0)
            close_swallow_errors();
    }

    ARCTICDB_NO_MOVE_OR_COPY(FdHolder);

  public:
    int fd() const noexcept { return fd_; }

    void close() {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE, fd_ >= 0, "Closing a file descriptor which was already closed"
        );
        int true_fd = fd_;
        fd_ = -1;
        if (posix_.close(true_fd) != 0)
            raise_io_error(errno, "Failed to close file descriptor");
    }

    // Ignore errors intentionally. This should only be used in exception handlers, where we can't throw any more
    // errors.
    void close_swallow_errors() {
        int res = posix_.close(fd_);
        if (res != 0)
            log::storage().warn("Failed to close file: {}", std::strerror(errno));
        fd_ = -1;
    }

    void sync() const {
        int res = posix_.fsync(fd_);
        if (res != 0) {
            if (errno == EINTR) {
                // Try one more time? TODO: Quick retries should be wrapped in a retry() that takes a lambda.
                res = posix_.fsync(fd_);
            }
        }

        if (res != 0)
            raise_io_error(errno, "fsync failed");
    }

  protected:
    PosixInterface& posix_;
    int fd_ = -1;
};

class Directory : public FdHolder {
  public:
    Directory(
            PosixInterface& posix, const fs::path& storage_dir, const fs::path& pathname,
            int flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC, mode_t mode = 0
    ) :
        FdHolder(posix, storage_dir, pathname, flags, mode) {}

    Directory(
            PosixInterface& posix, const Directory& dir_fd, const fs::path& pathname,
            int flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC, mode_t mode = 0
    ) :
        FdHolder(posix, dir_fd, pathname, flags, mode) {}

    ARCTICDB_NO_MOVE_OR_COPY(Directory);

    // Returns true if the directory was created.
    bool mkdir(const fs::path& pathname, mode_t mode) const {
        int res = posix_.mkdirat(fd_, pathname.c_str(), mode);
        if (res != 0) {
            if (errno != EEXIST)
                raise_io_error(errno, fmt::format("failed to create subdir {}", pathname.c_str()));
            return false;
        }
        return true;
    }

    int unlinkat(const fs::path& pathname) const { return posix_.unlinkat(fd_, pathname.c_str(), 0); }

    // Ignore errors intentionally. This should only be used in exception handlers, where we can't throw any more
    // errors.
    void unlinkat_swallow_errors(const fs::path& pathname) const {
        int res = posix_.unlinkat(fd_, pathname.c_str(), 0);
        if (res != 0)
            log::storage().warn("Failed to delete file {}: {}", pathname.c_str(), std::strerror(errno));
    }
};

class File : public FdHolder {
  public:
    File(PosixInterface& posix, const fs::path& storage_dir, const fs::path& pathname, int flags, mode_t mode) :
        FdHolder(posix, storage_dir, pathname, flags, mode) {}

    File(PosixInterface& posix, const Directory& dir_fd, const fs::path& pathname, int flags, mode_t mode) :
        FdHolder(posix, dir_fd, pathname, flags, mode) {}

    ARCTICDB_NO_MOVE_OR_COPY(File);

    void write_all(PosixInterface& posix, std::span<const std::uint8_t> buffer) {
        const size_t buffer_size = buffer.size();
        size_t written = 0;
        while (written < buffer_size) {
            const ssize_t n = posix.write(fd_, buffer.data() + written, buffer_size - written);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                raise_io_error(errno, "write failed");
            }
            written += static_cast<size_t>(n);
        }
    }

    std::shared_ptr<Buffer> read_all() const {
        struct stat st;
        int stat_res = posix_.fstat(fd_, &st);
        if (stat_res != 0)
            raise_io_error(errno, "stat failed");
        const size_t buffer_size = st.st_size;
        auto buffer = std::make_shared<Buffer>(buffer_size);
        size_t read = 0;
        while (read < buffer_size) {
            const ssize_t n = posix_.read(fd_, buffer->data() + read, buffer_size - read);
            if (n > 0)
                read += static_cast<size_t>(n);
            else if (n == 0)
                storage::raise<ErrorCode::E_UNEXPECTED_FILE_STORAGE_ERROR>(
                        "Unexpected EOF after {} of {} bytes", read, buffer_size
                );
            else if (errno != EINTR)
                raise_io_error(errno, "read failed");
        }

        return buffer;
    }
};

// Owns a directory stream opened through PosixInterface, so that fault injection sees the closedir.
class DirHandle {
  public:
    DirHandle(PosixInterface& posix, DIR* dirp) : posix_(posix), dirp_(dirp) {}
    ~DirHandle() {
        if (posix_.closedir(dirp_) != 0)
            log::storage().warn("Failed to close directory: {}", std::strerror(errno));
    }

    ARCTICDB_NO_MOVE_OR_COPY(DirHandle);

    DIR* get() const noexcept { return dirp_; }

  private:
    PosixInterface& posix_;
    DIR* dirp_;
};

// Borrowed reference to memory owned by FileTraversal.
struct FileEntry {
    std::string_view name;
    const fs::path& directory;

    FileEntry(std::string_view name, fs::path& directory) : name(name), directory(directory) {}
    ARCTICDB_NO_MOVE_OR_COPY(FileEntry);
};

// Traverses all files recursively under the directory root.
// Satisfies the single pass range concept.
// Its value type is `FileEntry`.
// Warning on lifetimes: The yielded `FileEntry` is invalidated if FileTraversal is destroyed
// or if the iterator is advanced. Entries can be processed within a loop body. Storing one
// elsewhere is impossible because FileEntry is not copyable or movable.
class FileTraversal {
  public:
    explicit FileTraversal(PosixInterface& posix, const fs::path& root) : posix_(posix) {
        open_directory(root);
        advance();
    }

    ARCTICDB_NO_MOVE_OR_COPY(FileTraversal);

    class Iterator : public boost::iterator_facade<Iterator, FileEntry, boost::single_pass_traversal_tag, FileEntry> {
      public:
        // boost::iterator_facade falls back to its own single_pass_traversal_tag category when the value type is not
        // movable, and the ranges machinery rejects a category that is not a standard tag.
        using iterator_concept = std::input_iterator_tag;

        Iterator() = default;
        explicit Iterator(FileTraversal* traversal) : traversal_(traversal) {}

      private:
        friend class boost::iterator_core_access;

        void increment() {
            traversal_->advance();
            if (traversal_->current_name_.empty())
                traversal_ = nullptr;
        }

        bool equal(const Iterator& other) const { return traversal_ == other.traversal_; }

        FileEntry dereference() const { return FileEntry{traversal_->current_name_, traversal_->stack_.back().path}; }

        // Null once the traversal is exhausted. Copies share the position, as a single pass stream must.
        FileTraversal* traversal_ = nullptr;
    };

    Iterator begin() { return Iterator{current_name_.empty() ? nullptr : this}; }
    Iterator end() { return Iterator{}; }

  private:
    struct DirLevel {
        fs::path path;
        std::unique_ptr<DirHandle> handle;
    };

    void advance() {
        current_name_ = {};
        while (!stack_.empty()) {
            DirLevel& level = stack_.back();
            errno = 0;
            struct dirent* dir_entry = posix_.readdir(level.handle->get());
            if (!dir_entry) {
                if (errno != 0 && errno != ENOENT)
                    raise_io_error(errno, fmt::format("Failed to list directory {}", level.path.c_str()));
                stack_.pop_back();
                continue;
            }

            std::string_view file_name(dir_entry->d_name);
            if (is_dot_dir(file_name))
                continue;

            if (dir_entry->d_type == DT_DIR) {
                open_directory(level.path / file_name);
                continue;
            }

            current_name_ = file_name;
            return;
        }
    }

    void open_directory(const fs::path& path) {
        DIR* dirp = posix_.opendir(path.c_str());
        if (!dirp) {
            if (errno == ENOENT)
                return;
            raise_io_error(errno, fmt::format("Failed to open directory {}", path.c_str()));
        }
        stack_.push_back(DirLevel{path, std::make_unique<DirHandle>(posix_, dirp)});
    }

    PosixInterface& posix_;
    std::vector<DirLevel> stack_;
    std::string_view current_name_;
};

std::optional<VariantKey> try_parse_as_key(const FileEntry& entry, KeyType key_type) noexcept {
    if (is_skipped_name(entry.name))
        return std::nullopt;
    try {
        return variant_key_from_bytes(reinterpret_cast<const uint8_t*>(entry.name.data()), entry.name.size(), key_type);
    } catch (const ArcticCategorizedException<ErrorCategory::INTERNAL>& e) {
        log::storage().info("File name '{}' could not be parsed into a VariantKey: {}", entry.name, e.what());
        return std::nullopt;
    }
}

// Names which do not parse are logged and skipped, since the directory may also hold files written by other
// versions of ArcticDB or by users.
// Returns range of VariantKey.
auto as_variant_keys(KeyType key_type) {
    auto try_parse_with_bound_key = std::bind(try_parse_as_key, std::placeholders::_1, key_type);
    return std::views::transform(try_parse_with_bound_key) |
           std::views::filter([](const std::optional<VariantKey>& key) { return key.has_value(); }) |
           std::views::transform([](std::optional<VariantKey> key) { return std::move(*key); });
}

Segment read_impl(PosixInterface& posix, const fs::path& storage_dir, const VariantKey& variant_key) {
    const fs::path dest_path = storage_dir / get_key_subpath(variant_key);
    bool retried_stale = false;
    while (true) {
        try {
            File file(posix, storage_dir, dest_path, O_RDONLY | O_CLOEXEC, 0);
            Segment result = Segment::from_buffer(file.read_all());
            file.close();
            return result;
        } catch (const StaleFileDescriptor& e) {
            if (!retried_stale) {
                retried_stale = true;
                continue;
            } else
                throw;
        } catch (const storage::KeyNotFoundException&) {
            // TODO: Kind of a silly exception handler, attaching variant_key here is important.
            throw storage::KeyNotFoundException(variant_key);
        }
    }
}

bool remove_impl(PosixInterface& posix, const fs::path& storage_dir, const VariantKey& key) {
    const std::string key_file_name = to_tokenized_key(key);
    const fs::path directory_path = storage_dir / entity::key_type_long_name(variant_key_type(key));
    Directory dest_dir(posix, storage_dir, directory_path);
    if (dest_dir.unlinkat(key_file_name.c_str()) != 0) {
        if (errno == ENOENT)
            return false;
        const fs::path full_path = directory_path / key_file_name;
        raise_io_error(errno, fmt::format("Failed to remove file {}: {}", full_path.c_str(), std::strerror(errno)));
    }
    dest_dir.sync();
    return true;
}

} // namespace

namespace arcticdb::storage::file_backed {

FileBackedStorage::FileBackedStorage(
        const LibraryPath& library_path, OpenMode mode, const Config& conf,
        const std::shared_ptr<PosixInterface>& posix_interface
) :
    Storage(library_path, mode),
    posix_(posix_interface) {
    const std::string lib_path_str = library_path.to_delim_path();
    const fs::path root_path = conf.dir_path();
    storage_dir_ = fs::absolute(root_path / lib_path_str);

    struct stat st;
    // TODO: Root path is assumed to exist.
    if (::stat(root_path.c_str(), &st) == 0) {
        new_dir_mode_ = st.st_mode & DIR_MODE_MASK;
    } else
        storage::raise<ErrorCode::E_UNEXPECTED_FILE_STORAGE_ERROR>(
                "Could not fstat root directory {}: {}", root_path.c_str(), std::strerror(errno)
        );

    Directory root_path_fd(*posix_, root_path, root_path);
    if (mode > OpenMode::READ) {
        // TODO: It's important to set sensible permissions on these directories, so that group users have read access.
        bool created = root_path_fd.mkdir(lib_path_str, new_dir_mode_);
        if (created)
            root_path_fd.sync();
    }
    // Ensure we can open the path as a directory.
    Directory storage_dir_fd(*posix_, root_path_fd, lib_path_str);
    storage_dir_fd.close();
    root_path_fd.close();
}

FileBackedStorage::FileBackedStorage(const LibraryPath& library_path, OpenMode mode, const Config& conf) :
    FileBackedStorage(library_path, mode, conf, std::make_shared<RealPosix>()) {}

FileBackedStorage::~FileBackedStorage() = default;

std::string FileBackedStorage::name() const { return fmt::format("file_backed_storage-{}", storage_dir_.c_str()); }

void FileBackedStorage::do_write(KeySegmentPair& key_seg) {
    std::string maybe_safe_file_name_for_key = to_tokenized_key(key_seg.variant_key());
    Segment& seg = *key_seg.segment_ptr();
    const std::string tmp_file_name = generate_tmp_file_name();
    const std::string key_name = entity::key_type_long_name(variant_key_type(key_seg.variant_key()));

    bool retried_stale = false;
    while (true) {
        Directory storage_dir_fd(*posix_, storage_dir_, storage_dir_);
        storage_dir_fd.mkdir(key_name, new_dir_mode_);
        Directory key_type_dir_fd(*posix_, storage_dir_fd, key_name);
        try {
            File tmp_key_fd(
                    *posix_, key_type_dir_fd, tmp_file_name, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, NEW_FILE_MODE
            );
            size_t segment_size = seg.calculate_size();
            // TODO: Unnecessary copy
            std::vector<std::uint8_t> buffer(segment_size);
            seg.write_to(buffer.data());
            tmp_key_fd.write_all(*posix_, buffer);
            tmp_key_fd.sync();
            tmp_key_fd.close();

            if (posix_->renameat(
                        key_type_dir_fd.fd(),
                        tmp_file_name.c_str(),
                        key_type_dir_fd.fd(),
                        maybe_safe_file_name_for_key.c_str()
                ) != 0) {
                raise_io_error(errno, fmt::format("Failed to move temp file to {}", maybe_safe_file_name_for_key));
            }

            // After data is written, and file is closed, fsync on dir makes the file durable for local file systems.
            // No-op on NFS.
            key_type_dir_fd.sync();
            key_type_dir_fd.close();
            storage_dir_fd.close();
            break;
        } catch (const StaleFileDescriptor& e) {
            key_type_dir_fd.unlinkat_swallow_errors(tmp_file_name);
            if (!retried_stale) {
                retried_stale = true;
                continue;
            }
            throw;
        } catch (...) {
            key_type_dir_fd.unlinkat_swallow_errors(tmp_file_name);
            throw;
        }
    }
}

void FileBackedStorage::do_write_if_none(KeySegmentPair& kv [[maybe_unused]]) {
    storage::raise<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>(
            "Atomic operations not yet supported on file backed storage"
    );
}

void FileBackedStorage::do_update(KeySegmentPair& key_seg, UpdateOpts opts) {
    if (!opts.upsert_ && !do_key_exists(key_seg.variant_key())) {
        // TODO: S3 implementations skip this check altogether according to s3::detail::do_update_impl().
        // The weird thing is that this doesn't fit with my mental model. I know that lib.update(upsert=True) is
        // necessary.
        throw KeyNotFoundException(
                key_seg.variant_key(),
                fmt::format("do_update called with upsert=false on non-existent key: {}", key_seg.variant_key())
        );
    }

    return do_write(key_seg);
}

void FileBackedStorage::do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts) {
    visitor(variant_key, read_impl(*posix_, storage_dir_, VariantKey{variant_key}));
}

KeySegmentPair FileBackedStorage::do_read(VariantKey&& variant_key, ReadKeyOpts) {
    return {std::move(variant_key), read_impl(*posix_, storage_dir_, VariantKey{variant_key})};
}

void FileBackedStorage::do_remove(VariantKey&& variant_key, RemoveOpts opts) {
    const bool success = remove_impl(*posix_, storage_dir_, variant_key);
    if (!success && !opts.ignores_missing_key_)
        throw KeyNotFoundException(variant_key);
}

void FileBackedStorage::do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) {
    std::vector<VariantKey> failed_deletes;
    for (const VariantKey& variant_key : variant_keys) {
        const bool success = remove_impl(*posix_, storage_dir_, variant_key);
        if (!success && !opts.ignores_missing_key_)
            failed_deletes.emplace_back(variant_key);
    }
    if (!failed_deletes.empty())
        throw KeyNotFoundException(std::move(failed_deletes));
}

bool FileBackedStorage::do_key_exists(const VariantKey& key) {
    return exists(*posix_, storage_dir_, get_key_subpath(key));
}

bool FileBackedStorage::do_fast_delete() { return false; }

bool FileBackedStorage::do_iterate_type_until_match(
        KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix
) {
    const auto prefix_matches = stream_id_prefix_matcher(prefix);
    FileTraversal traversal(*posix_, storage_dir_ / entity::key_type_long_name(key_type));
    for (VariantKey&& key : traversal | as_variant_keys(key_type)) {
        if (prefix_matches(variant_key_id(key)) && visitor(std::move(key)))
            return true;
    }
    return false;
}

void FileBackedStorage::do_visit_object_sizes(
        KeyType key_type, const std::string& prefix, const ObjectSizesVisitor& visitor
) {
    const auto prefix_matches = stream_id_prefix_matcher(prefix);
    for (const FileEntry& entry : FileTraversal(*posix_, storage_dir_ / entity::key_type_long_name(key_type))) {
        const std::optional<VariantKey> maybe_key = try_parse_as_key(entry, key_type);
        if (!maybe_key)
            continue;
        const VariantKey& key = *maybe_key;
        struct stat st;
        if (posix_->stat((entry.directory / entry.name).c_str(), &st) != 0) {
            log::storage().info("Could not stat file {}: {}. Skipping", entry.name, std::strerror(errno));
            continue;
        }
        if (prefix_matches(variant_key_id(key)))
            visitor(key, st.st_size);
    }
}

[[nodiscard]] std::string FileBackedStorage::do_key_path(const VariantKey& key) const { return get_key_subpath(key); }

[[nodiscard]] const std::set<char>& FileBackedStorage::do_unsupported_symbol_chars() const {
    static const std::set<char> unsupported_characters = []() {
        std::set<char> result = GLOBALLY_UNSUPPORTED_CHARS;
        // Forward slash should be intentionally omitted from this list, it should be escaped. Far too useful to users.
        // TODO: Remove forward slash from list.
        for (char x : {'?', '!', '*', '\\', ':', '"', '|', '/'})
            result.insert(x);

        return result;
    }();
    return unsupported_characters;
}

[[nodiscard]] const std::set<char>& FileBackedStorage::do_unsupported_library_chars() const {
    const std::set<char>& unsupported_symbol_characters = do_unsupported_symbol_chars();
    static const std::set<char> unsupported_characters = [&unsupported_symbol_characters]() {
        std::set<char> result = unsupported_symbol_characters;
        result.insert('/');
        return result;
    }();
    return unsupported_characters;
}

[[nodiscard]] std::optional<char> FileBackedStorage::do_verify_library_suffix([[maybe_unused]] std::string_view path
) const {
    // Shamelessly lifted off LMDB backend. The concept is the same.
#ifdef _WIN32
    // Windows directory names cannot end in '.' or whitespace.
    if (!path.empty() && (path.back() == '.' || std::isspace(path.back()))) {
        return path.back();
    }
#endif
    return std::nullopt;
}

size_t FileBackedStorage::get_num_temporary_files() const {
    return std::ranges::count_if(FileTraversal(*posix_, storage_dir_), is_tmp_file_name, &FileEntry::name);
}

} // namespace arcticdb::storage::file_backed

#endif
