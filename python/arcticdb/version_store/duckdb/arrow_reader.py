"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

from typing import TYPE_CHECKING, Iterator, Optional

import pyarrow as pa

if TYPE_CHECKING:
    from arcticdb_ext.version_store import RecordBatchIterator


class ArcticRecordBatchReader:
    """
    Lazy record batch reader that streams Arrow data from ArcticDB storage.

    Implements the PyArrow RecordBatchReader protocol for zero-copy integration
    with DuckDB and other Arrow-compatible tools.

    This class enables memory-efficient processing of large datasets by streaming
    record batches one at a time instead of materializing the entire dataset.

    This is primarily used internally by Library.sql() and Library.duckdb().
    """

    def __init__(self, cpp_iterator: "RecordBatchIterator"):
        """
        Initialize the reader with a C++ RecordBatchIterator.

        Parameters
        ----------
        cpp_iterator : RecordBatchIterator
            The C++ iterator from ArrowOutputFrame.create_iterator()
        """
        self._cpp_iterator = cpp_iterator
        self._schema: Optional[pa.Schema] = None
        self._first_batch: Optional[pa.RecordBatch] = None  # Cache for first batch
        self._first_batch_returned = False
        self._exhausted = False

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

    def read_all(self) -> pa.Table:
        """
        Read all remaining record batches and return as a PyArrow Table.

        This materializes all data into memory. For large datasets, prefer
        iterating over batches or using DuckDB's lazy evaluation.

        Returns
        -------
        pa.Table
            A PyArrow Table containing all data.
        """
        batches = list(self)
        if not batches:
            return pa.Table.from_arrays([])
        return pa.Table.from_batches(batches)

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        """Iterate over record batches."""
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

        Returns
        -------
        pa.RecordBatchReader
            A PyArrow RecordBatchReader that streams batches from ArcticDB.
        """
        return pa.RecordBatchReader.from_batches(self.schema, self)
