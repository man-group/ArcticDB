"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

import pyarrow as pa

if TYPE_CHECKING:
    from arcticdb_ext.version_store import LazyRecordBatchIterator


def _descriptor_to_arrow_schema(descriptor) -> pa.Schema:
    """Build a PyArrow schema from a C++ StreamDescriptor.

    Used to discover the schema of empty symbols without reading any data segments.
    The descriptor is always available from the index-only read, even when there are
    no data segments.
    """
    from arcticdb_ext.types import DataType

    _DATATYPE_TO_ARROW = {
        DataType.UINT8: pa.uint8(),
        DataType.UINT16: pa.uint16(),
        DataType.UINT32: pa.uint32(),
        DataType.UINT64: pa.uint64(),
        DataType.INT8: pa.int8(),
        DataType.INT16: pa.int16(),
        DataType.INT32: pa.int32(),
        DataType.INT64: pa.int64(),
        DataType.FLOAT32: pa.float32(),
        DataType.FLOAT64: pa.float64(),
        DataType.BOOL8: pa.bool_(),
        DataType.NANOSECONDS_UTC64: pa.timestamp("ns"),
        DataType.ASCII_DYNAMIC64: pa.large_string(),
        DataType.UTF_DYNAMIC64: pa.large_string(),
    }

    fields = []
    for field_wrapper in descriptor.fields():
        dt = field_wrapper.type.data_type()
        arrow_type = _DATATYPE_TO_ARROW.get(dt, pa.string())
        fields.append(pa.field(field_wrapper.name, arrow_type))
    return pa.schema(fields)


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


def _pad_batch_to_schema(batch: pa.RecordBatch, target_schema: pa.Schema) -> pa.RecordBatch:
    """Pad a record batch to match the target schema by adding null columns for missing fields.

    Columns present in the batch but absent from the target schema are dropped.
    Columns present in the target schema but absent from the batch are filled with nulls.
    The result columns are ordered to match ``target_schema``.

    Returns the batch unchanged (no copy) if it already matches the target schema.
    """
    if batch.schema.equals(target_schema):
        return batch

    batch_columns = {f.name: i for i, f in enumerate(batch.schema)}
    num_rows = len(batch)

    arrays = []
    for field in target_schema:
        idx = batch_columns.get(field.name)
        if idx is not None:
            col = batch.column(idx)
            if col.type != field.type:
                col = col.cast(field.type)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(num_rows, type=field.type))

    return pa.RecordBatch.from_arrays(arrays, schema=target_schema)


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

    def __init__(self, cpp_iterator: "LazyRecordBatchIterator", columns: Optional[List[str]] = None):
        """
        Initialize the reader with a C++ lazy record batch iterator.

        Parameters
        ----------
        cpp_iterator : LazyRecordBatchIterator
            The C++ iterator that reads segments on-demand from storage.
        columns : list of str, optional
            If provided, restricts the schema to only these columns (plus any
            ``__idx__``-prefixed variants). Used for column projection so the
            merged descriptor is filtered to the projected set.
        """
        self._cpp_iterator = cpp_iterator
        self._projected_columns: Optional[set] = set(columns) if columns is not None else None
        self._schema: Optional[pa.Schema] = None
        self._first_batch: Optional[pa.RecordBatch] = None  # Cache for first batch
        self._first_batch_returned = False
        self._exhausted = False
        self._iteration_started = False

    def _ensure_schema(self) -> None:
        """Derive schema from the merged descriptor and first batch, then cache the first batch.

        The merged descriptor (from the version key's TimeseriesDescriptor) contains
        ALL column names across ALL segments. The first batch provides the actual Arrow
        types produced by the C++ conversion (which may differ from descriptor types,
        e.g. dictionary-encoded strings vs large_string). For columns not in the first
        batch, the descriptor-derived types are used as fallback.
        """
        if self._schema is not None:
            return

        # Get the full set of columns from the merged descriptor
        descriptor_schema = _descriptor_to_arrow_schema(self._cpp_iterator.descriptor())

        # If column projection is active, restrict the descriptor schema to only
        # the projected columns. This prevents the schema from including columns
        # that won't appear in any batch.
        if self._projected_columns is not None:
            descriptor_schema = pa.schema([f for f in descriptor_schema if f.name in self._projected_columns])

        if self._cpp_iterator.num_batches() == 0:
            self._schema = descriptor_schema
            return

        # Cache the first batch so iteration doesn't lose it
        batch_data = self._cpp_iterator.next()
        if batch_data is None:
            # All segments were empty after filtering
            self._schema = descriptor_schema
            return

        self._first_batch = pa.RecordBatch._import_from_c(batch_data.array(), batch_data.schema())

        # Build the final schema from the descriptor, using the first batch's actual Arrow
        # types where available (they reflect C++ conversion, e.g. dictionary-encoded strings).
        # For columns absent from the first batch (dynamic schema), use the descriptor type.
        batch_type_map = {f.name: f for f in self._first_batch.schema}
        fields = []
        for desc_field in descriptor_schema:
            batch_field = batch_type_map.get(desc_field.name)
            if batch_field is not None:
                fields.append(batch_field)
            else:
                fields.append(desc_field)
        self._schema = pa.schema(fields)

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

        self._ensure_schema()
        batches = [_pad_batch_to_schema(b, self._schema) for b in self]
        if not batches:
            if self._schema and len(self._schema) > 0:
                return pa.Table.from_pydict({field.name: [] for field in self._schema}, schema=self._schema)
            return pa.table({})
        table = pa.Table.from_batches(batches, schema=self._schema)
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

        For dynamic-schema symbols where segments have different column subsets,
        each batch is padded with null columns to match the full schema derived
        from the merged descriptor.

        Returns
        -------
        pa.RecordBatchReader
            A PyArrow RecordBatchReader that streams batches from ArcticDB.
        """
        storage_schema = self.schema
        storage_names = [f.name for f in storage_schema]
        clean_names = _strip_idx_prefix_from_names(storage_names)

        def _padded_batches(reader, schema, names):
            for batch in reader:
                padded = _pad_batch_to_schema(batch, schema)
                if names is not None:
                    padded = padded.rename_columns(names)
                yield padded

        if clean_names == storage_names:
            return pa.RecordBatchReader.from_batches(storage_schema, _padded_batches(self, storage_schema, None))

        clean_schema = pa.schema(
            [pa.field(clean, field.type, field.nullable) for clean, field in zip(clean_names, storage_schema)]
        )

        return pa.RecordBatchReader.from_batches(clean_schema, _padded_batches(self, storage_schema, clean_names))
