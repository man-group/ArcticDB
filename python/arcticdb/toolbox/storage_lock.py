"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import Any, Optional

from arcticdb_ext.tools import StorageLock as _StorageLockImpl
from arcticdb.version_store._normalization import normalize_metadata, denormalize_user_metadata

_MAX_LOCK_METADATA_SIZE = 1 << 20  # 1MB


class StorageLock:
    """Wrapper around the (unreliable) ``arcticdb_ext.tools.StorageLock`` that normalizes/denormalizes
    user-supplied metadata.

    Metadata passed at acquire time is stored on the lock and can be read back by any process via
    ``read_metadata`` to trace which job holds the lock. Metadata is capped at 1MB, well below the general
    16MB user-metadata limit, because the locking mechanism relies on reads and writes happening much
    faster than the pre-emption window Storage.WaitMs (default 1000).
    """

    def __init__(self, ext_lock: _StorageLockImpl):
        self._ext_lock = ext_lock

    def lock(self, metadata: Optional[Any] = None) -> None:
        self._ext_lock.lock(normalize_metadata(metadata, max_size=_MAX_LOCK_METADATA_SIZE))

    def lock_timeout(self, timeout_ms: int, metadata: Optional[Any] = None) -> None:
        self._ext_lock.lock_timeout(timeout_ms, normalize_metadata(metadata, max_size=_MAX_LOCK_METADATA_SIZE))

    def try_lock(self, metadata: Optional[Any] = None) -> bool:
        return self._ext_lock.try_lock(normalize_metadata(metadata, max_size=_MAX_LOCK_METADATA_SIZE))

    def unlock(self) -> None:
        self._ext_lock.unlock()

    def read_metadata(self) -> Optional[Any]:
        """Return the current lock's metadata, or None if there is no lock or it carries no metadata."""
        udm = self._ext_lock.read_metadata()
        return denormalize_user_metadata(udm) if udm is not None else None

    def _test_release_local_lock(self) -> None:
        self._ext_lock._test_release_local_lock()
