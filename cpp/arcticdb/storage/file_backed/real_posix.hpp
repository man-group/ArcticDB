/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/file_backed/posix_interface.hpp>

namespace arcticdb::storage::file_backed {

class RealPosix : public PosixInterface {
  public:
    int open(const char* pathname, int flags, mode_t mode) override;
    int openat(int dirfd, const char* pathname, int flags, mode_t mode) override;
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
};

} // namespace arcticdb::storage::file_backed
