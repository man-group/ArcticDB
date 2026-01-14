/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

namespace arcticdb {
static const char* const WriteVersionId = "__write__";
static const char* const TombstoneVersionId = "__tombstone__";
static const char* const TombstoneAllVersionId = "__tombstone_all__";
static const char* const CreateSnapshotId = "__create_snapshot__";
static const char* const DeleteSnapshotId = "__delete_snapshot__";
static const char* const LastSyncId = "__last_sync__";
static const char* const LastBackupId = "__last_backup__";
static const char* const FailedTargetId = "__failed_target__";
static const char* const StorageLogId = "__storage_log__";
static const char* const FailedStorageLogId = "__failed_storage_log__";
// Used by v2 replication in the enterprise repo to manually recreate a symbol
static const char* const RecreateSymbolId = "__recreate__";
// Used by v2 replication in the enterprise repo to manually refresh a symbol
static const char* const RefreshSymbolId = "__refresh__";
} // namespace arcticdb