/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/codec/segment.hpp>

namespace arcticdb::storage {

enum class StorageOperation {
    READ,
    WRITE,
    DELETE, // Triggers a global failure (i.e. delete_objects will fail for all objects if one of them triggers a delete failure)
    DELETE_LOCAL, // Triggers a local failure (i.e. delete_objects will fail just for this object and succeed for the rest)
    LIST,
    EXISTS,
};

inline std::string operation_to_string(StorageOperation operation) {
    switch (operation) {
        case StorageOperation::READ: return "Read";
        case StorageOperation::WRITE: return "Write";
        case StorageOperation::DELETE: return "Delete";
        case StorageOperation::DELETE_LOCAL: return "DeleteLocal";
        case StorageOperation::LIST: return "List";
        case StorageOperation::EXISTS: return "Exists";
    }
    util::raise_rte("Invalid Storage operation provided for mock client");
}

template<typename Output, typename Error>
struct StorageResult {
    std::variant<Output, Error> result;

    [[nodiscard]] bool is_success() const {
        return std::holds_alternative<Output>(result);
    }

    Error& get_error() {
        return std::get<Error>(result);
    }
    Output& get_output() {
        return std::get<Output>(result);
    }
};

template<typename Key, typename Failure>
class MockStorageClient {
public:
    virtual std::optional<Failure> has_failure_trigger(const Key& key, StorageOperation op) const = 0;

    virtual Failure missing_key_failure() const = 0;

    virtual bool matches_prefix(const Key& key, const Key& prefix) const = 0;

    StorageResult<Segment, Failure> read_internal(const Key& key) const {
        if (auto failure = has_failure_trigger(key, StorageOperation::READ); failure.has_value()) {
            return {failure.value()};
        }
        auto it = contents_.find(key);
        if (it == contents_.end()) {
            return {missing_key_failure()};
        }
        return {it->second};
    }

    StorageResult<std::monostate, Failure> write_internal(const Key& key, Segment&& segment) {
        if (auto failure = has_failure_trigger(key, StorageOperation::WRITE); failure.has_value()) {
            return {failure.value()};
        }
        contents_.insert_or_assign(key, std::move(segment));
        return {std::monostate()};
    }

    // returns Failure or failed deletes in case of a DELETE_LOCAL failure
    StorageResult<std::vector<Key>, Failure> delete_internal(const std::vector<Key>& keys) {
        for (const auto& key : keys) {
            if (auto failure = has_failure_trigger(key, StorageOperation::DELETE); failure.has_value()) {
                return {failure.value()};
            }
        }

        std::vector<Key> failed_deletes;
        for (const auto& key : keys) {
            if (auto failure = has_failure_trigger(key, StorageOperation::DELETE_LOCAL); failure.has_value()) {
                failed_deletes.push_back(key);
            } else {
                contents_.erase(key);
            }
        }

        return {failed_deletes};
    }

    StorageResult<std::vector<Key>, Failure> list_internal(const Key& prefix) const {
        std::vector<Key> output;

        for (auto& key : contents_) {
            if (matches_prefix(key.first, prefix)) {
                auto failure = has_failure_trigger(key.first, StorageOperation::LIST);
                if (failure.has_value()) {
                    return {failure.value()};
                }
                output.push_back(key.first);
            }
        }

        return {output};
    }

    bool has_key(const Key& key) const {
        return contents_.find(key) != contents_.end();
    }

    StorageResult<bool, Failure> exists_internal(const Key& key) const {
        if (auto failure = has_failure_trigger(key, StorageOperation::EXISTS); failure.has_value()) {
            return {failure.value()};
        }
        return {has_key(key)};
    }

protected:
    std::map<Key, Segment> contents_;
};

}
