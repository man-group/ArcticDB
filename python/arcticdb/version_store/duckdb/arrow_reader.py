"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Union

import pyarrow as pa

if TYPE_CHECKING:
    from arcticdb_ext.version_store import LazyRecordBatchIterator, RecordBatchIterator

_IDX_PREFIX = "__idx__"


def _strip_idx_prefix_from_names(names: List[str]) -> List[str]:
    """Strip the ``__idx__`` prefix that ArcticDB adds to MultiIndex levels 1+.

    Handles the (theoretical) case where stripping would create a duplicate by
    appending underscores, mirroring ``_normalization.py`` denormalization logic.
    """
    seen: set = set()
    clean: List[str] = []
    for name in names:
        stripped = name[len(_IDX_PREFIX) :] if name.startswith(_IDX_PREFIX) else name
        while stripped in seen:
            stripped = f"_{stripped}_"
        seen.add(stripped)
        clean.append(stripped)
    return clean


def _build_clean_to_storage_map(storage_names: List[str]) -> Dict[str, str]:
    """Build a mapping from user-facing (clean) column names to storage names.

    Only includes entries where the names differ (i.e. where ``__idx__`` was stripped).
    """
    clean_names = _strip_idx_prefix_from_names(storage_names)
    return {clean: storage for clean, storage in zip(clean_names, storage_names) if clean != storage}


class ArcticRecordBatchReader:
    """
    Lazy record batch reader that streams Arrow data from ArcticDB storage.

    Implements the PyArrow RecordBatchReader protocol for zero-copy integration
    with DuckDB and other Arrow-compatible tools.

    This class enables memory-efficient processing of large datasets by streaming
    record batches one at a time instead of materializing the entire dataset.

    This is primarily used internally by Library.sql() and Library.duckdb().

    Note
    ----
    This reader is single-use. Once exhausted, it cannot be reset or reused.
    Attempting to iterate over an exhausted reader will immediately raise StopIteration.
    """

    def __init__(self, cpp_iterator: "Union[RecordBatchIterator, LazyRecordBatchIterator]"):
        """
        Initialize the reader with a C++ record batch iterator.

        Parameters
        ----------
        cpp_iterator : RecordBatchIterator or LazyRecordBatchIterator
            The C++ iterator. RecordBatchIterator reads eagerly from memory;
            LazyRecordBatchIterator reads segments on-demand from storage.
        """
        self._cpp_iterator = cpp_iterator
        self._schema: Optional[pa.Schema] = None
        self._first_batch: Optional[pa.RecordBatch] = None  # Cache for first batch
        self._first_batch_returned = False
        self._exhausted = False
        self._iteration_started = False

    def _ensure_schema(self) -> None:
        """Extract schema from first batch if not already done."""
        if self._schema is not None:
            return

        if self._cpp_iterator.num_batches() == 0:
            # Empty result - return empty schema
            self._schema = pa.schema([])
            return

        # Extract first batch and cache it
        batch_data = self._cpp_iterator.next()
        if batch_data is not None:
            self._first_batch = pa.RecordBatch._import_from_c(batch_data.array(), batch_data.schema())
            self._schema = self._first_batch.schema
        else:
            self._schema = pa.schema([])

    @property
    def schema(self) -> pa.Schema:
        """
        Returns the PyArrow schema for this reader.

        The schema is lazily extracted from the first record batch.
        """
        self._ensure_schema()
        return self._schema

    def read_next_batch(self) -> Optional[pa.RecordBatch]:
        """
        Read the next record batch.

        Returns
        -------
        Optional[pa.RecordBatch]
            The next record batch, or None if exhausted.
        """
        if self._exhausted:
            return None

        self._iteration_started = True

        # First, ensure schema is extracted (which caches first batch)
        self._ensure_schema()

        # Return cached first batch if not yet returned
        if self._first_batch is not None and not self._first_batch_returned:
            self._first_batch_returned = True
            return self._first_batch

        # Get next batch from C++ iterator
        batch_data = self._cpp_iterator.next()
        if batch_data is None:
            self._exhausted = True
            return None

        return pa.RecordBatch._import_from_c(batch_data.array(), batch_data.schema())

    def read_all(self, strip_idx_prefix: bool = True) -> pa.Table:
        """
        Read all remaining record batches and return as a PyArrow Table.

        This materializes all data into memory. For large datasets, prefer
        iterating over batches or using DuckDB's lazy evaluation.

        Parameters
        ----------
        strip_idx_prefix : bool, default True
            If True, strip the ``__idx__`` prefix from MultiIndex column names.

        Returns
        -------
        pa.Table
            A PyArrow Table containing all data.

        Raises
        ------
        RuntimeError
            If called after iteration has already started (reader is single-use).
        """
        if self._iteration_started:
            raise RuntimeError(
                "Cannot call read_all() after iteration has started. "
                "ArcticRecordBatchReader is single-use - create a new reader to read all data."
            )

        batches = list(self)
        if not batches:
            # Return empty table with schema if available
            self._ensure_schema()
            if self._schema and len(self._schema) > 0:
                return pa.Table.from_pydict({field.name: [] for field in self._schema}, schema=self._schema)
            return pa.table({})
        table = pa.Table.from_batches(batches)
        if strip_idx_prefix:
            storage_names = table.column_names
            clean_names = _strip_idx_prefix_from_names(storage_names)
            if clean_names != storage_names:
                table = table.rename_columns(clean_names)
        return table

    @property
    def is_exhausted(self) -> bool:
        """Return True if the reader has been fully consumed."""
        return self._exhausted

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        """Iterate over record batches."""
        if self._exhausted:
            raise RuntimeError(
                "Cannot iterate over exhausted reader. "
                "ArcticRecordBatchReader is single-use - create a new reader to iterate again."
            )
        if self._iteration_started:
            raise RuntimeError(
                "Cannot create multiple iterators from the same reader. " "ArcticRecordBatchReader is single-use."
            )
        self._iteration_started = True
        return self

    def __next__(self) -> pa.RecordBatch:
        """Return the next record batch or raise StopIteration."""
        batch = self.read_next_batch()
        if batch is None:
            raise StopIteration
        return batch

    def __len__(self) -> int:
        """Return the total number of batches."""
        return self._cpp_iterator.num_batches()

    @property
    def num_batches(self) -> int:
        """Return the total number of batches."""
        return self._cpp_iterator.num_batches()

    @property
    def current_index(self) -> int:
        """Return the current batch index (0-indexed)."""
        return self._cpp_iterator.current_index()

    def to_pyarrow_reader(self) -> pa.RecordBatchReader:
        """
        Convert to a proper PyArrow RecordBatchReader.

        This is useful for passing to libraries like DuckDB that require
        a native PyArrow RecordBatchReader type.

        The ``__idx__`` prefix that ArcticDB adds to MultiIndex levels 1+ is
        stripped so that SQL queries can reference the original index names.

        Returns
        -------
        pa.RecordBatchReader
            A PyArrow RecordBatchReader that streams batches from ArcticDB.
        """
        storage_schema = self.schema
        storage_names = [f.name for f in storage_schema]
        clean_names = _strip_idx_prefix_from_names(storage_names)

        if clean_names == storage_names:
            return pa.RecordBatchReader.from_batches(storage_schema, self)

        clean_schema = pa.schema(
            [pa.field(clean, field.type, field.nullable) for clean, field in zip(clean_names, storage_schema)]
        )

        def _rename_batches(reader, names):
            for batch in reader:
                yield batch.rename_columns(names)

        return pa.RecordBatchReader.from_batches(clean_schema, _rename_batches(self, clean_names))
