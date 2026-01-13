/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>

#include <boost/container/small_vector.hpp>

namespace arcticdb::storage {

class DuplicateKeyException : public ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY> {
  public:
    explicit DuplicateKeyException(const std::string& message) :
        ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY>(message) {}

    explicit DuplicateKeyException(VariantKey key) :
        ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY>(std::string(variant_key_view(key))),
        key_(std::move(key)) {}

    [[nodiscard]] const VariantKey& key() const { return key_; }

  private:
    VariantKey key_;
};

class NoDataFoundException : public ArcticCategorizedException<ErrorCategory::MISSING_DATA> {
  public:
    explicit NoDataFoundException(VariantId key) :
        ArcticCategorizedException<ErrorCategory::MISSING_DATA>(
                std::visit([](const auto& key) { return fmt::format("{}", key); }, key)
        ),
        key_(key) {}

    explicit NoDataFoundException(const std::string& msg) :
        ArcticCategorizedException<ErrorCategory::MISSING_DATA>(msg) {}

    explicit NoDataFoundException(const char* msg) :
        ArcticCategorizedException<ErrorCategory::MISSING_DATA>(std::string(msg)) {}

    [[nodiscard]] const VariantId& key() const {
        util::check(static_cast<bool>(key_), "Key not found");
        return *key_;
    }

  private:
    std::optional<VariantId> key_;
};

class KeyNotFoundException : public ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND> {
  public:
    explicit KeyNotFoundException(const std::string& message) :
        ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND>(message) {}

    explicit KeyNotFoundException(std::vector<VariantKey>&& keys) :
        ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND>(fmt::format("Not found: {}", keys)),
        keys_(std::make_shared<std::vector<VariantKey>>(std::move(keys))) {}

    explicit KeyNotFoundException(std::vector<VariantKey>&& keys, const std::string& err_output) :
        ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND>(err_output),
        keys_(std::make_shared<std::vector<VariantKey>>(std::move(keys))) {}

    explicit KeyNotFoundException(const VariantKey& single_key) :
        KeyNotFoundException(std::vector<VariantKey>{single_key}) {}

    explicit KeyNotFoundException(const VariantKey& single_key, const std::string& err_output) :
        KeyNotFoundException(std::vector<VariantKey>{single_key}, err_output) {}

    explicit KeyNotFoundException(boost::container::small_vector<VariantKey, 1>& keys) :
        ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND>(fmt::format("Not found: {}", keys)),
        keys_(std::make_shared<std::vector<VariantKey>>(
                std::make_move_iterator(keys.begin()), std::make_move_iterator(keys.end())
        )) {}

    explicit KeyNotFoundException(boost::container::small_vector<VariantKey, 1>& keys, const std::string& err_output) :
        ArcticSpecificException<ErrorCode::E_KEY_NOT_FOUND>(err_output),
        keys_(std::make_shared<std::vector<VariantKey>>(
                std::make_move_iterator(keys.begin()), std::make_move_iterator(keys.end())
        )) {}

    std::vector<VariantKey>& keys() { return *keys_; }

  private:
    std::shared_ptr<std::vector<VariantKey>> keys_;
    mutable std::string msg_;
};

struct KeyNotFoundInStageResultInfo {
    uint64_t stage_result_index_;
    VariantKey missing_key_;

    [[nodiscard]] uint64_t stage_result_index() const { return stage_result_index_; }

    [[nodiscard]] VariantKey missing_key() const { return missing_key_; }

    [[nodiscard]] std::string to_string() const {
        return fmt::format("stage_result_index=[{}] missing_key=[{}]", stage_result_index_, missing_key_);
    }
};

inline bool operator==(const KeyNotFoundInStageResultInfo& left, const KeyNotFoundInStageResultInfo& right) {
    return left.stage_result_index_ == right.stage_result_index_ && left.missing_key_ == right.missing_key_;
}

inline bool operator!=(const KeyNotFoundInStageResultInfo& left, const KeyNotFoundInStageResultInfo& right) {
    return !(left == right);
}

} // namespace arcticdb::storage

namespace fmt {
using namespace arcticdb::storage;

template<>
struct formatter<KeyNotFoundInStageResultInfo> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const KeyNotFoundInStageResultInfo& k, FormatContext& ctx) const {
        return fmt::format_to(
                ctx.out(),
                "stage_result_index={}, missing_key={}",
                k.stage_result_index(),
                variant_key_view(k.missing_key())
        );
    };
};
} // namespace fmt

namespace arcticdb::storage {

class LibraryPermissionException : public PermissionException {
  public:
    LibraryPermissionException(const LibraryPath& path, OpenMode mode, std::string_view operation) :
        PermissionException(fmt::format("{} not permitted. lib={}, mode={}", operation, path, mode)),
        lib_path_(path),
        mode_(mode) {}

    const LibraryPath& library_path() const { return lib_path_; }

    OpenMode mode() const { return mode_; }

  private:
    LibraryPath lib_path_;
    OpenMode mode_;
};

} // namespace arcticdb::storage