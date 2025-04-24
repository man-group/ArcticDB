"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from enum import Enum
from typing import Dict, Iterable

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
    This is a subset of all the key types that ArcticDB uses, covering the most important types.

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

    @staticmethod
    def _from_native(native_key_type: NativeKeyType):
        if native_key_type == NativeKeyType.TABLE_DATA:
            return KeyType.TABLE_DATA
        elif native_key_type == NativeKeyType.TABLE_INDEX:
            return KeyType.TABLE_INDEX
        elif native_key_type == NativeKeyType.VERSION:
            return KeyType.VERSION
        elif native_key_type == NativeKeyType.VERSION_REF:
            return KeyType.VERSION_REF
        elif native_key_type == NativeKeyType.SNAPSHOT_REF:
            return KeyType.SNAPSHOT_REF
        elif native_key_type == NativeKeyType.APPEND_DATA:
            return KeyType.APPEND_DATA
        elif native_key_type == NativeKeyType.MULTI_KEY:
            return KeyType.MULTI_KEY
        elif native_key_type == NativeKeyType.LOG:
            return KeyType.LOG
        elif native_key_type == NativeKeyType.LOG_COMPACTED:
            return KeyType.LOG_COMPACTED
        elif native_key_type == NativeKeyType.SYMBOL_LIST:
            return KeyType.SYMBOL_LIST
        else:
            raise ArcticException(f"Unexpected KeyType {native_key_type} - this indicates a bug in ArcticDB")


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

    def get_sizes(self) -> Dict[KeyType, Size]:
        """
        A breakdown of compressed sizes (in bytes) in the library, grouped by key type.

        All the key types in KeyType are always included in the output.
        """
        sizes = self._nvs.version_store.scan_object_sizes()
        return {KeyType._from_native(s.key_type) : Size(s.compressed_size, s.count) for s in sizes}

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
