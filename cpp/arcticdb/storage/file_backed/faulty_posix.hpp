/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/file_backed/posix_interface.hpp>

#include <filesystem>
#include <mutex>
#include <optional>
#include <queue>
#include <unordered_map>
#include <unordered_set>

// TODO: This file is better placed in storage/test. It only needs to be built for tests.
//       Keeping it here for now for quicker iteration.

namespace arcticdb::storage::file_backed {

enum class SyscallId : std::uint8_t { OPEN, CLOSE, WRITE, READ, SYNC, RENAME, LINK, MKDIR, STAT, UNLINK };

using PosixError = int;

/**
 * Interface interacts with the file system, but has additional public APIs to inject faults.
 * Relying on a real filesystem means don't have to mock complex side effects such as files
 * lingering due to lack of cleanup on stack unwinds. There may be downsides too.
 */
class FaultyPosix : public PosixInterface {
  public:
    int open(const char* pathname, int flags, mode_t mode) override;
    int openat(int dirfd, const char* pathname, int flags, mode_t mode) override;
    int close(int fd) override;
    ssize_t write(int fd, const void* buf, size_t count) override;
    ssize_t read(int fd, void* buf, size_t count) override;
    int fsync(int fd) override;
    int rename(const char* oldpath, const char* newpath) override;
    int link(const char* oldpath, const char* newpath) override;
    int mkdir(const char* pathname, mode_t mode) override;
    int mkdirat(int fd, const char* pathname, mode_t mode) override;
    int stat(const char* pathname, struct stat* statbuf) override;
    int fstat(int fd, struct stat* statbuf) override;
    DIR* opendir(const char* name) override;
    struct dirent* readdir(DIR* dirp) override;
    int closedir(DIR* dirp) override;
    int unlink(const char* pathname) override;

    // Fault injection API
  public:
    void inject_fault(SyscallId, std::optional<PosixError>);
    bool uncosumed_faults() const noexcept;
    size_t num_open_file_descriptors() const noexcept;

  private:
    // Need to protect class members since APIs will potentially be called from folly futures.
    std::mutex data_mutex_;
    // TODO: Hmm, how will I tell we opened a temporary file? Need awareness of ".tmp" prefix.
    std::unordered_set<std::filesystem::path> temporary_files_;
    std::unordered_map<SyscallId, std::queue<std::optional<PosixError>>> faults_;
    size_t num_open_file_descriptors_ = 0;
};

} // namespace arcticdb::storage::file_backed
