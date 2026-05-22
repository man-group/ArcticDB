"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest

from arcticdb_ext.version_store import constants

EXPECTED = {
    "WRITE_VERSION_ID": "__write__",
    "TOMBSTONE_VERSION_ID": "__tombstone__",
    "TOMBSTONE_ALL_VERSION_ID": "__tombstone_all__",
    "CREATE_SNAPSHOT_ID": "__create_snapshot__",
    "DELETE_SNAPSHOT_ID": "__delete_snapshot__",
    "LAST_SYNC_ID": "__last_sync__",
    "LAST_BACKUP_ID": "__last_backup__",
    "LAST_BACKGROUND_DELETION_ID": "__last_background_deletion__",
    "FAILED_TARGET_ID": "__failed_target__",
    "STORAGE_LOG_ID": "__storage_log__",
    "FAILED_STORAGE_LOG_ID": "__failed_storage_log__",
    "RECREATE_SYMBOL_ID": "__recreate__",
    "REFRESH_SYMBOL_ID": "__refresh__",
}


@pytest.mark.parametrize("name,value", list(EXPECTED.items()))
def test_constant_value(name, value):
    assert getattr(constants, name) == value
