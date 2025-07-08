/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

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
        default: util::raise_rte("Invalid Storage operation provided for mock client");
    }
}

}
