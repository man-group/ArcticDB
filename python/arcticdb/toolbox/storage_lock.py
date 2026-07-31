"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import Any, Mapping, Optional

from arcticdb_ext.tools import StorageLock as _StorageLockImpl
from arcticdb.version_store._normalization import normalize_metadata, denormalize_user_metadata


class StorageLock:
    """Dict-in/dict-out wrapper around the (unreliable) ``arcticdb_ext.tools.StorageLock``.

    Metadata passed at acquire time is stored on the lock and can be read back by any process via
    ``read_metadata`` to trace which job holds the lock.
    """

    def __init__(self, ext_lock: _StorageLockImpl):
        self._ext_lock = ext_lock

    def lock(self, metadata: Optional[Mapping[str, Any]] = None) -> None:
        self._ext_lock.lock(normalize_metadata(metadata))

    def lock_timeout(self, timeout_ms: int, metadata: Optional[Mapping[str, Any]] = None) -> None:
        self._ext_lock.lock_timeout(timeout_ms, normalize_metadata(metadata))

    def try_lock(self, metadata: Optional[Mapping[str, Any]] = None) -> bool:
        return self._ext_lock.try_lock(normalize_metadata(metadata))

    def unlock(self) -> None:
        self._ext_lock.unlock()

    def read_metadata(self) -> Optional[Mapping[str, Any]]:
        udm = self._ext_lock.read_metadata()
        return denormalize_user_metadata(udm) if udm is not None else None
