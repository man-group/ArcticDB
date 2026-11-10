/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/file_backed/faulty_posix.hpp>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <queue>
#include <ranges>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#define PROPAGATE_FAULT_IF_EXISTS(syscall_id)                                                                          \
    std::unique_lock lock(data_mutex_);                                                                                \
    if (std::queue<std::optional<PosixError>>& fault_sequence = faults_[syscall_id]; !fault_sequence.empty()) {        \
        const std::optional<PosixError> maybe_error = fault_sequence.front();                                          \
        if (!maybe_error)                                                                                              \
            return res;                                                                                                \
        errno = *maybe_error;                                                                                          \
        fault_sequence.pop();                                                                                          \
        return -1;                                                                                                     \
    }                                                                                                                  \
    return res;

namespace arcticdb::storage::file_backed {

int FaultyPosix::open(const char* pathname, int flags, mode_t mode) {
    std::unique_lock lock(data_mutex_);
    if (std::queue<std::optional<PosixError>>& fault_sequence = faults_[SyscallId::OPEN]; !fault_sequence.empty()) {
        const std::optional<PosixError> maybe_error = fault_sequence.front();
        if (maybe_error) {
            errno = *maybe_error;
            fault_sequence.pop();
            return -1;
        }
    }
    int res = ::open(pathname, flags, mode);
    if (res >= 0)
        ++num_open_file_descriptors_;

    return res;
}

int FaultyPosix::openat(int dirfd, const char* pathname, int flags, mode_t mode) {
    std::unique_lock lock(data_mutex_);
    if (std::queue<std::optional<PosixError>>& fault_sequence = faults_[SyscallId::OPEN]; !fault_sequence.empty()) {
        const std::optional<PosixError> maybe_error = fault_sequence.front();
        if (maybe_error) {
            errno = *maybe_error;
            fault_sequence.pop();
            return -1;
        }
    }
    int res = ::openat(dirfd, pathname, flags, mode);
    if (res >= 0)
        ++num_open_file_descriptors_;

    return res;
}

int FaultyPosix::close(int fd) {
    int res = ::close(fd);
    if (num_open_file_descriptors_ == 0)
        throw std::runtime_error("Would underflow num_open_file_descriptors_");
    --num_open_file_descriptors_;
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::CLOSE);
}

ssize_t FaultyPosix::write(int fd, const void* buf, size_t count) {
    ssize_t res = ::write(fd, buf, count);
    // TODO: Return value from Fault struct is narrower than ssize_t. Is that an issue?
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::WRITE);
}

ssize_t FaultyPosix::read(int fd, void* buf, size_t count) {
    ssize_t res = ::read(fd, buf, count);
    // TODO: Return value from Fault struct is narrower than ssize_t. Is that an issue?
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::READ);
}

int FaultyPosix::fsync(int fd) {
    int res = ::fsync(fd);
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::SYNC);
}

int FaultyPosix::rename(const char* oldpath, const char* newpath) {
    int res = ::rename(oldpath, newpath);
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::RENAME);
}

int FaultyPosix::link(const char* oldpath, const char* newpath) {
    int res = ::link(oldpath, newpath);
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::LINK);
}

int FaultyPosix::mkdir(const char* pathname, mode_t mode) {
    int res = ::mkdir(pathname, mode);
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::MKDIR);
}

int FaultyPosix::mkdirat(int fd, const char* pathname, mode_t mode) {
    int res = ::mkdirat(fd, pathname, mode);
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::MKDIR);
}

int FaultyPosix::stat(const char* pathname, struct stat* statbuf) {
    int res = ::stat(pathname, statbuf);
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::STAT);
}

int FaultyPosix::fstat(int fd, struct stat* statbuf) {
    int res = ::fstat(fd, statbuf);
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::STAT);
}

int FaultyPosix::unlink(const char* pathname) {
    int res = ::unlink(pathname);
    PROPAGATE_FAULT_IF_EXISTS(SyscallId::UNLINK);
}

DIR* FaultyPosix::opendir(const char* name) { return ::opendir(name); }
struct dirent* FaultyPosix::readdir(DIR* dirp) { return ::readdir(dirp); }
int FaultyPosix::closedir(DIR* dirp) { return ::closedir(dirp); }

void FaultyPosix::inject_fault(SyscallId syscall, std::optional<PosixError> maybe_error) {
    std::unique_lock lock(data_mutex_);
    faults_[syscall].push(maybe_error);
}

bool FaultyPosix::uncosumed_faults() const noexcept {
    return !std::ranges::all_of(faults_ | std::ranges::views::values, &std::queue<std::optional<PosixError>>::empty);
}

size_t FaultyPosix::num_open_file_descriptors() const noexcept { return num_open_file_descriptors_; }

} // namespace arcticdb::storage::file_backed
