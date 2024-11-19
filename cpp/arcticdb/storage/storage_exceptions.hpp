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

class DuplicateKeyException : public ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY> {
public:
    explicit DuplicateKeyException(std::string message) :
        ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY>(message) { }

    explicit DuplicateKeyException(VariantKey key) :
        ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY>(std::string(variant_key_view(key))),
        key_(std::move(key)) {}

    [[nodiscard]] const VariantKey &key() const {
        return key_;
    }
private:
    VariantKey key_;
};

class NoDataFoundException : public ArcticCategorizedException<ErrorCategory::MISSING_DATA> {
public:
    explicit NoDataFoundException(VariantId key) :
        ArcticCategorizedException<ErrorCategory::MISSING_DATA>(std::visit([](const auto &key) { return fmt::format("{}", key); }, key)),
        key_(key){
    }

    explicit NoDataFoundException(const std::string& msg) :
        ArcticCategorizedException<ErrorCategory::MISSING_DATA>(msg) {
    }

    explicit NoDataFoundException(const char* msg) :
        ArcticCategorizedException<ErrorCategory::MISSING_DATA>(std::string(msg)) {
    }

    [[nodiscard]] const VariantId &key() const {
        util::check(static_cast<bool>(key_), "Key not found");
        return *key_;
    }
private:
    std::optional<VariantId> key_;
};

class KeyNotFoundException : public ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND> {
public:
    explicit KeyNotFoundException(std::string message) :
        ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND>(message) {
    }

    explicit KeyNotFoundException(Composite<VariantKey>&& keys) :
        ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND>(fmt::format("Not found: {}", keys)),
        keys_(std::make_shared<Composite<VariantKey>>(std::move(keys))) {
    }

    explicit KeyNotFoundException(Composite<VariantKey>&& keys, std::string err_output) :
        ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND>(err_output),
        keys_(std::make_shared<Composite<VariantKey>>(std::move(keys))) {
    }

    explicit KeyNotFoundException(const VariantKey& single_key):
        KeyNotFoundException(Composite<VariantKey>{VariantKey{single_key}}) {}

    explicit KeyNotFoundException(const VariantKey& single_key, std::string err_output):
        KeyNotFoundException(Composite<VariantKey>{VariantKey{single_key}}, err_output) {}


    Composite<VariantKey>& keys() {
        return *keys_;
    }
private:
    std::shared_ptr<Composite<VariantKey>> keys_;
    mutable std::string msg_;
};

class LibraryPermissionException : public PermissionException {
  public:
    LibraryPermissionException(const LibraryPath &path, OpenMode mode, std::string_view operation) :
        PermissionException(fmt::format("{} not permitted. lib={}, mode={}", operation, path, mode)),
        lib_path_(path), mode_(mode) {}

    const LibraryPath &library_path() const {
        return lib_path_;
    }

    OpenMode mode() const {
        return mode_;
    }

  private:
    LibraryPath lib_path_;
    OpenMode mode_;
};

}