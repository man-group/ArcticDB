/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>

namespace arcticdb::storage {

class PermissionException : public std::exception {
  public:
    PermissionException(const LibraryPath &path, OpenMode mode, std::string_view operation) :
        lib_path_(path), mode_(mode), msg_(fmt::format(
        "{} not permitted. lib={}, mode={}", operation, lib_path_, mode_)
    ) {}

    const char *what() const noexcept override {
        return msg_.data();
    }

    const LibraryPath &library_path() const {
        return lib_path_;
    }

  private:
    LibraryPath lib_path_;
    OpenMode mode_;
    std::string msg_;
};

}