/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>

namespace arcticdb::storage {

class PermissionException : public std::exception {
public:
    PermissionException(const LibraryPath& path, OpenMode mode, std::string_view operation)
        : lib_path_(path),
          mode_(mode),
          msg_(fmt::format("{} not permitted. lib={}, mode={}", operation, lib_path_, mode_))
    {
    }

    const char* what() const noexcept override
    {
        return msg_.data();
    }

    const LibraryPath& library_path() const
    {
        return lib_path_;
    }

private:
    LibraryPath lib_path_;
    OpenMode mode_;
    std::string msg_;
};

} // namespace arcticdb::storage