/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/file_backed/real_posix.hpp>

#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace arcticdb::storage::file_backed {

int RealPosix::open(const char* pathname, int flags, mode_t mode) { return ::open(pathname, flags, mode); }

int RealPosix::openat(int dirfd, const char* pathname, int flags, mode_t mode) {
    return ::openat(dirfd, pathname, flags, mode);
}

ssize_t RealPosix::write(int fd, const void* buf, size_t count) { return ::write(fd, buf, count); }
int RealPosix::fsync(int fd) { return ::fsync(fd); }
int RealPosix::rename(const char* oldpath, const char* newpath) { return ::rename(oldpath, newpath); }
int RealPosix::link(const char* oldpath, const char* newpath) { return ::link(oldpath, newpath); }
int RealPosix::mkdir(const char* pathname, mode_t mode) { return ::mkdir(pathname, mode); }
int RealPosix::stat(const char* pathname, struct stat* statbuf) { return ::stat(pathname, statbuf); }
int RealPosix::fstat(int fd, struct stat* statbuf) { return ::fstat(fd, statbuf); }

} // namespace arcticdb::storage::file_backed
