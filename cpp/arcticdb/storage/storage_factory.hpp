/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>


namespace arcticdb {
    namespace storage {

        std::unique_ptr<Storage> create_storage(
                const LibraryPath& library_path,
                OpenMode mode,
                const arcticdb::proto::storage::VariantStorage &storage_config);

    } // namespace storage
} // namespace arcticdb
