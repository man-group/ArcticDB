"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from enum import Enum
from typing import Dict, Iterable, Optional

from arcticdb_ext.exceptions import ArcticException
from arcticdb_ext.storage import KeyType as NativeKeyType
from arcticdb.version_store import NativeVersionStore
from dataclasses import dataclass


@dataclass
class Size:
    """Information about the size of objects."""

    bytes_compressed: int
    """Compressed size in bytes."""

    count: int
    """The number of objects contributing to the size."""

    def __add__(self, other):
        return Size(self.bytes_compressed + other.bytes_compressed, self.count + other.count)


def sum_sizes(sizes: Iterable[Size]) -> Size:
    """The sum of the given sizes."""
    return sum(sizes, Size(0, 0))


class KeyType(Enum):
    """
    The key types that ArcticDB can report sizes for, i.e. those that exist as objects in storage.

    The first ten are the most important ones, and are the only types scanned by `AdminTools.get_sizes` unless you
    ask for others explicitly. The rest are historical, specialized or transient; each one costs an extra listing
    operation to scan, and most are empty in a typical library.

    This is the complete set - `get_sizes(key_types=list(KeyType))` accounts for every object in the library.
    ArcticDB has a few internal key types that are absent because they are never objects in storage: TOMBSTONE and
    TOMBSTONE_ALL live inside VERSION key segments, and STREAM_GROUP is not a real key type at all.

    More information about the ArcticDB data layout is available [here](https://docs.arcticdb.io/latest/technical/on_disk_storage/).
    """

    TABLE_DATA = 1
    """Where the contents of data are stored, in a tiled format."""

    TABLE_INDEX = 2
    """Metadata used by ArcticDB to select the TABLE_DATA blocks to read. One per un-deleted version of the symbol."""

    VERSION = 3
    """Metadata used by ArcticDB to store the chain of versions associated with a symbol. It is possible to have
    more VERSION keys than the version number of a symbol, as we also write a VERSION key when we delete data."""

    VERSION_REF = 4
    """A pointer to the latest version of a symbol. One per symbol."""

    APPEND_DATA = 5
    """Only used for staged writes. Data that has been staged but not yet finalized."""

    MULTI_KEY = 6
    """Only used for "recursively normalized" data, which cannot currently be written with the `Library` API. 

    Records all the TABLE_INDEX keys used to compose the overall structure.
    For example, if you save a list of two dataframes with recursive normalizers, this key would refer to the two
    index keys used to serialize the two dataframes."""

    SNAPSHOT_REF = 7
    """Metadata used by ArcticDB to store the contents of a snapshot (the structure created when you call `lib.snapshot`)."""

    LOG = 8
    """Only used for enterprise replication. Small objects recording a stream of changes to the library."""

    LOG_COMPACTED = 9
    """Only used for some enterprise replication installations. A compacted form of the LOG keys."""

    SYMBOL_LIST = 10
    """A collection of keys that together store the total set of symbols stored in a library. Used for `list_symbols`."""

    SNAPSHOT = 11
    """Superseded by SNAPSHOT_REF, but may still be present in libraries written by older clients."""

    SNAPSHOT_TOMBSTONE = 12
    """Records that a snapshot has been deleted."""

    BACKUP_SNAPSHOT_REF = 13
    """Only used for enterprise backups. A SNAPSHOT_REF written to the backup storage."""

    APPEND_REF = 14
    """Only used for staged writes. The head of the list of staged APPEND_DATA segments for a symbol."""

    STORAGE_INFO = 15
    """Small objects recording state about the storage itself, such as when it was last replicated."""

    LOCK = 16
    """Transient. Present only while an operation holds a lock on a symbol."""

    ATOMIC_LOCK = 17
    """Transient. Present only while an operation holds a lock implemented with atomic writes."""

    COLUMN_STATS = 18
    """Pre-computed statistics about columns, used to skip data segments when filtering."""

    LIBRARY_CONFIG = 19
    """The library's configuration, for installations that store it in the storage rather than externally."""

    REPLICATION_FAIL_INFO = 20
    """Only used for enterprise replication. Records symbols that failed to replicate."""

    BLOCK_VERSION_REF = 21
    """Used by some background jobs to track their state across many versions."""

    VERSION_JOURNAL = 22
    """No longer written. Retained for backwards compatibility with very old libraries."""

    METRICS = 23
    """No longer written. Was used to record timing and other performance metrics."""

    PARTITION = 24
    """Not in general use."""

    OFFSET = 25
    """Not in general use."""

    GENERATION = 26
    """Not in general use. Streaming data support."""

    @staticmethod
    def _from_native(native_key_type: NativeKeyType):
        key_type = _FROM_NATIVE.get(native_key_type)
        if key_type is None:
            raise ArcticException(f"Unexpected KeyType {native_key_type} - this indicates a bug in ArcticDB")
        return key_type

    def to_native(self) -> NativeKeyType:
        """The corresponding `arcticdb_ext.storage.KeyType`, for callers of the extension APIs directly."""
        return _TO_NATIVE[self]


# Every member is named after its counterpart in the extension, so the mapping is derived rather than
# written out. ATOMIC_LOCK is the one exception - the extension binds it as SLOW_LOCK. A member with no
# counterpart fails here at import rather than on first use.
_NATIVE_NAMES = {"ATOMIC_LOCK": "SLOW_LOCK"}

_TO_NATIVE = {key_type: getattr(NativeKeyType, _NATIVE_NAMES.get(key_type.name, key_type.name)) for key_type in KeyType}

_FROM_NATIVE = {native: key_type for key_type, native in _TO_NATIVE.items()}


class AdminTools:
    """
    A collection of tools for administrative tasks on an ArcticDB library.

    This API is not currently stable and not governed by the semver version numbers of ArcticDB releases.

    See Also
    --------

    Library.admin_tools: The API to get a handle on this object from a Library.
    """

    def __init__(self, nvs: NativeVersionStore):
        self._nvs = nvs

    def get_sizes(self, key_types: Optional[Iterable[KeyType]] = None) -> Dict[KeyType, Size]:
        """
        A breakdown of compressed sizes (in bytes) in the library, grouped by key type.

        Every key type scanned appears in the output, including those with no objects in the library.

        Parameters
        ----------
        key_types : Iterable[KeyType], optional
            The key types to scan. Defaults to the ten most important ones,

            ```
            TABLE_DATA
            TABLE_INDEX
            VERSION
            VERSION_REF
            APPEND_DATA
            MULTI_KEY
            SNAPSHOT_REF
            LOG
            LOG_COMPACTED
            SYMBOL_LIST
            ```

            Pass `list(KeyType)` for a complete breakdown. Each key type costs one listing operation over its
            prefix in storage, so scanning types that are empty in your library is cheap but not free.

        Returns
        -------
        Dict[KeyType, Size]
            One entry per scanned key type, giving its total compressed size in bytes and object count.

        Raises
        ------
        StorageException
            If the storage does not support size calculation, or a listing operation fails.

        Examples
        --------
        >>> sizes = lib.admin_tools().get_sizes()
        >>> sizes[KeyType.TABLE_DATA].bytes_compressed
        24847
        >>> lib.admin_tools().get_sizes(key_types=[KeyType.TABLE_DATA])
        {<KeyType.TABLE_DATA: 1>: Size(bytes_compressed=24847, count=7)}
        """
        native_key_types = [k.to_native() for k in key_types] if key_types is not None else None
        sizes = self._nvs.version_store.scan_object_sizes(key_types=native_key_types)
        return {KeyType._from_native(s.key_type): Size(s.compressed_size, s.count) for s in sizes}

    def get_sizes_by_symbol(self) -> Dict[str, Dict[KeyType, Size]]:
        """
        A breakdown of compressed sizes (in bytes) in the library, grouped by symbol and then key type.

        The following key types (and only these) are always included in the output,

        ```
        VERSION_REF
        VERSION
        TABLE_INDEX
        TABLE_DATA
        APPEND_DATA
        ```
        """
        sizes = self._nvs.version_store.scan_object_sizes_by_stream()
        res = dict()
        for sym, sym_sizes in sizes.items():
            sizes_as_python = {KeyType._from_native(t): Size(s.compressed_size, s.count) for t, s in sym_sizes.items()}
            res[sym] = sizes_as_python
        return res

    def get_sizes_for_symbol(self, symbol: str) -> Dict[KeyType, Size]:
        """
        A breakdown of compressed sizes (in bytes) used by the given symbol, grouped by key type.

        The following key types (and only these) are always included in the output:

        ```
        VERSION_REF
        VERSION
        TABLE_INDEX
        TABLE_DATA
        APPEND_DATA
        ```

        Does not raise if the symbol does not exist.
        """
        sizes = self._nvs.version_store.scan_object_sizes_for_stream(symbol)
        return {KeyType._from_native(s.key_type): Size(s.compressed_size, s.count) for s in sizes}
