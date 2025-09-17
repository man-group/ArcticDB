/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#ifndef _WIN32
#include <unistd.h>
#include <pwd.h>
#include <arcticdb/util/preconditions.hpp>
#include <sys/types.h>
#endif

namespace arcticdb {

inline std::string get_home_directory() {
#ifdef _WIN32
    const char* home_drive = getenv("HOMEDRIVE");
    const char* home_path = getenv("HOMEPATH");
    return std::string(home_drive) + std::string(home_path);
#else
    if (const char* home_dir = getenv("HOME"); home_dir != nullptr) {
        return {home_dir};
    } else {
        auto buffer_size = sysconf(_SC_GETPW_R_SIZE_MAX);
        if (buffer_size == -1)
            buffer_size = 16384;

        std::string buffer(buffer_size, 0);
        struct passwd pwd{};
        struct passwd* user_data = nullptr;
        auto result = getpwuid_r(getuid(), &pwd, buffer.data(), buffer_size, &user_data);
        util::check(user_data != nullptr, "Failed to get user home directory: {}", std::strerror(result));
        return {user_data->pw_dir};
    }
#endif
}

} // namespace arcticdb
