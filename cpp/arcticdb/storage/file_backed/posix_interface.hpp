/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace arcticdb::storage::file_backed {

/**
 * Abstract away the low level OS I/O functions for testing purposes. Implementations can
 * inject faults and the implementation should copy in prescribed ways.
 */
class PosixInterface {
  public:
    virtual ~PosixInterface();
    [[nodiscard]] virtual int open(const char* pathname, int flags, mode_t mode) = 0;
    [[nodiscard]] virtual int openat(int dirfd, const char* pathname, int flags, mode_t mode) = 0;
    [[nodiscard]] virtual int close(int fd) = 0;
    // Wow so I can feed 64 bits of buffer size but the function can only signal 63 bits of bytes written? Cool.
    [[nodiscard]] virtual ssize_t write(int fd, const void* buf, size_t count) = 0;
    [[nodiscard]] virtual ssize_t read(int fd, void* buf, size_t count) = 0;
    [[nodiscard]] virtual int fsync(int fd) = 0;
    [[nodiscard]] virtual int rename(const char* oldpath, const char* newpath) = 0;
    [[nodiscard]] virtual int link(const char* oldpath, const char* newpath) = 0;
    [[nodiscard]] virtual int mkdir(const char* pathname, mode_t mode) = 0;
    [[nodiscard]] virtual int mkdirat(int fd, const char* pathname, mode_t mode) = 0;
    [[nodiscard]] virtual int stat(const char* pathname, struct stat* statbuf) = 0;
    [[nodiscard]] virtual int fstat(int fd, struct stat* statbuf) = 0;
    [[nodiscard]] virtual DIR* opendir(const char* name) = 0;
    [[nodiscard]] virtual struct dirent* readdir(DIR* dirp) = 0;
    [[nodiscard]] virtual int closedir(DIR* dirp) = 0;
    [[nodiscard]] virtual int unlink(const char* pathname) = 0;
};

} // namespace arcticdb::storage::file_backed
