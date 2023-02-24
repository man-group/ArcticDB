/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
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
    static const char* const StorageLogId = "__storage_log__";
}