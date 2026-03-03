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


# Mapping from ArcticDB type name strings to PyArrow types.
# Type names appear in both C++ DataType enum names and in the
# TD<type=TYPENAME, dim=0> strings from get_description().
_TYPENAME_TO_ARROW = {
    "UINT8": pa.uint8(),
    "UINT16": pa.uint16(),
    "UINT32": pa.uint32(),
    "UINT64": pa.uint64(),
    "INT8": pa.int8(),
    "INT16": pa.int16(),
    "INT32": pa.int32(),
    "INT64": pa.int64(),
    "FLOAT32": pa.float32(),
    "FLOAT64": pa.float64(),
    "BOOL8": pa.bool_(),
    "NANOSECONDS_UTC64": pa.timestamp("ns"),
    "MICROS_UTC64": pa.timestamp("us"),
    "ASCII_DYNAMIC64": pa.large_string(),
    "ASCII_FIXED64": pa.string(),
    "UTF_DYNAMIC64": pa.large_string(),
    "UTF_FIXED64": pa.large_string(),
}


def _descriptor_to_arrow_schema(descriptor) -> pa.Schema:
    """Build a PyArrow schema from a C++ StreamDescriptor.

    Used to discover the schema of empty symbols without reading any data segments.
    The descriptor is always available from the index-only read, even when there are
    no data segments.
    """
    fields = []
    for field_wrapper in descriptor.fields():
        dt = field_wrapper.type.data_type()
        arrow_type = _TYPENAME_TO_ARROW.get(dt.name, pa.string())
        fields.append(pa.field(field_wrapper.name, arrow_type))
    return pa.schema(fields)


def _description_to_arrow_schema(description) -> pa.Schema:
    """Build a PyArrow schema from a SymbolDescription (get_description() result).

    Lightweight alternative to _descriptor_to_arrow_schema that works from Python
    metadata without creating a C++ iterator. Used by SHOW TABLES / SHOW ALL TABLES
    to register schema-only empty tables.
    """
    fields = []
    for col in description.columns:
        dtype_str = str(col.dtype)
        arrow_type = pa.string()  # fallback
        for key, at in _TYPENAME_TO_ARROW.items():
            if key in dtype_str:
                arrow_type = at
                break
        fields.append(pa.field(col.name, arrow_type))
    return pa.schema(fields)


_IDX_PREFIX = "__idx__"


def _expand_columns_with_idx_prefix(columns: List[str]) -> List[str]:
    """Expand user-facing column names to include ``__idx__``-prefixed variants.

    ArcticDB stores MultiIndex level columns with an ``__idx__`` prefix.
    When requesting columns by name we must request both forms so the C++
    reader matches whichever form is actually stored.
    """
    expanded = []
    for c in columns:
        expanded.append(c)
        if not c.startswith(_IDX_PREFIX):
            expanded.append(_IDX_PREFIX + c)
    return expanded


def _strip_idx_prefix_from_names(names: List[str]) -> List[str]:
    """Strip the ``__idx__`` prefix that ArcticDB adds to MultiIndex levels 1+.

    Handles the (theoretical) case where stripping would create a duplicate by
    appending underscores, mirroring ``_normalization.py`` denormalization logic.
    """
    _MAX_COLLISION_RETRIES = 100
    seen: set = set()
    clean: List[str] = []
    for name in names:
        stripped = name[len(_IDX_PREFIX) :] if name.startswith(_IDX_PREFIX) else name
        retries = 0
        while stripped in seen:
            stripped = f"_{stripped}_"
            retries += 1
            if retries >= _MAX_COLLISION_RETRIES:
                raise ValueError(f"Too many name collisions deduplicating column '{name}'")
        seen.add(stripped)
        clean.append(stripped)
    return clean


def _build_clean_to_storage_map(storage_names: List[str]) -> Dict[str, str]:
    """Build a mapping from user-facing (clean) column names to storage names.

    Only includes entries where the names differ (i.e. where ``__idx__`` was stripped).
    """
    clean_names = _strip_idx_prefix_from_names(storage_names)
    return {clean: storage for clean, storage in zip(clean_names, storage_names) if clean != storage}


# Numeric type hierarchy for type-widening detection.
# Maps Arrow numeric types to a width rank — higher rank means wider type.
_NUMERIC_TYPE_RANK = {
    pa.int8(): 1,
    pa.int16(): 2,
    pa.int32(): 3,
    pa.int64(): 4,
    pa.uint8(): 1,
    pa.uint16(): 2,
    pa.uint32(): 3,
    pa.uint64(): 4,
    pa.float16(): 5,
    pa.float32(): 6,
    pa.float64(): 7,
}


def _is_wider_numeric_type(desc_type: pa.DataType, batch_type: pa.DataType) -> bool:
    """Return True if *desc_type* is a strictly wider numeric type than *batch_type*.

    Used during schema derivation to prefer the descriptor's widened type when
    type widening has occurred across segments (e.g. int64 first segment, float64
    second segment → descriptor says float64).

    Only applies to numeric types. For non-numeric types (strings, timestamps,
    dictionary-encoded), returns False so the batch's actual Arrow type is used.
    """
    desc_rank = _NUMERIC_TYPE_RANK.get(desc_type)
    batch_rank = _NUMERIC_TYPE_RANK.get(batch_type)
    if desc_rank is not None and batch_rank is not None:
        return desc_rank > batch_rank
    return False


class ArcticRecordBatchReader:
    """
    Lazy record batch reader that streams Arrow data from ArcticDB storage.

    Implements the PyArrow RecordBatchReader protocol for zero-copy integration
    with DuckDB and other Arrow-compatible tools.

    This class enables memory-efficient processing of large datasets by streaming
    record batches one at a time instead of materializing the entire dataset.

    Column-slice merging and schema padding are handled in C++ by the
    LazyRecordBatchIterator, so each batch returned already has the full column
    set in the correct order.

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

    def _read_next_raw_batch(self) -> Optional[pa.RecordBatch]:
        """Read a single batch from the C++ iterator.

        The C++ LazyRecordBatchIterator handles column-slice merging and schema
        padding, so each returned batch already has the full column set.
        """
        batch_data = self._cpp_iterator.next()
        if batch_data is None:
            return None
        return pa.RecordBatch._import_from_c(batch_data.array(), batch_data.schema())

    def _ensure_schema(self) -> None:
        """Derive schema from the first batch, then cache it.

        C++ ``pad_batch_to_schema`` guarantees every batch has exactly the same
        columns in the same order (matching ``target_fields_``).  This includes
        the index column(s) which C++ always adds via
        ``requested_column_bitset_including_index``.  We therefore derive the
        schema from the first batch's actual columns rather than filtering the
        descriptor by projected columns (which wouldn't know about the index).

        For type widening (e.g. int64 first segment, float64 second), the
        descriptor's wider type is preferred over the first batch's narrower type.
        """
        if self._schema is not None:
            return

        # Descriptor schema is used as fallback for empty symbols and for
        # type widening detection.
        descriptor_schema = _descriptor_to_arrow_schema(self._cpp_iterator.descriptor())

        if self._cpp_iterator.num_batches() == 0:
            # No data segments — use descriptor filtered by projected columns.
            if self._projected_columns is not None:
                descriptor_schema = pa.schema([f for f in descriptor_schema if f.name in self._projected_columns])
            self._schema = descriptor_schema
            return

        # Cache the first batch so iteration doesn't lose it
        self._first_batch = self._read_next_raw_batch()
        if self._first_batch is None:
            # All segments were empty after filtering
            if self._projected_columns is not None:
                descriptor_schema = pa.schema([f for f in descriptor_schema if f.name in self._projected_columns])
            self._schema = descriptor_schema
            return

        # Derive schema from the first batch (reflects C++'s actual output).
        # Check each column against the descriptor for type widening: when the
        # descriptor has a wider numeric type (e.g. float64 after int64→float64
        # append), prefer the descriptor's widened type.
        desc_type_map = {f.name: f for f in descriptor_schema}
        fields = []
        for batch_field in self._first_batch.schema:
            desc_field = desc_type_map.get(batch_field.name)
            if desc_field is not None and _is_wider_numeric_type(desc_field.type, batch_field.type):
                fields.append(desc_field)
            else:
                fields.append(batch_field)
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

        batch = self._read_next_raw_batch()
        if batch is None:
            self._exhausted = True
            return None

        return batch

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
        # Use an explicit loop instead of a list comprehension to work around
        # a CPython 3.13.1-3.13.3 bug (gh-127682) where list comprehensions
        # call __iter__ twice, triggering our single-use iterator guard.
        batches = []
        for b in self:
            batches.append(b)
        if not batches:
            return pa.Table.from_pydict({field.name: [] for field in self._schema}, schema=self._schema)
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

        Returns
        -------
        pa.RecordBatchReader
            A PyArrow RecordBatchReader that streams batches from ArcticDB.
        """
        storage_schema = self.schema
        storage_names = [f.name for f in storage_schema]
        clean_names = _strip_idx_prefix_from_names(storage_names)

        # Use a generator to yield batches from ``read_next_batch()``.
        # ``pa.RecordBatchReader.from_batches()`` calls ``__iter__`` twice on its
        # iterable argument, which conflicts with our single-use ``__iter__`` guard.
        # A generator is its own iterator and returns ``self`` from ``__iter__``,
        # so the double call is harmless.
        #
        # C++ ``pad_batch_to_schema`` guarantees every batch has exactly the same
        # columns in the same order, and ``_ensure_schema`` derives the schema from
        # the first batch, so no Python-side alignment is needed.
        def _read_batches(reader):
            while True:
                batch = reader.read_next_batch()
                if batch is None:
                    return
                yield batch

        if clean_names == storage_names:
            return pa.RecordBatchReader.from_batches(storage_schema, _read_batches(self))

        clean_schema = pa.schema(
            [pa.field(clean, field.type, field.nullable) for clean, field in zip(clean_names, storage_schema)]
        )

        def _renamed_batches(reader, names):
            for batch in _read_batches(reader):
                yield batch.rename_columns(names)

        return pa.RecordBatchReader.from_batches(clean_schema, _renamed_batches(self, clean_names))
