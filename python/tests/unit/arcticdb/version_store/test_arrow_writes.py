"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb.util.test import assert_frame_equal
from arcticdb.version_store._normalization import ArrowTableNormalizer
from arcticdb_ext.storage import KeyType


def test_record_batch_roundtrip():
    table = pa.table({"col": pa.array([0, 1], pa.int64())})
    normalizer = ArrowTableNormalizer()
    arcticdb_record_batches, _ = normalizer.normalize(table)
    pa_record_batches = []
    for record_batch in arcticdb_record_batches:
        pa_record_batches.append(pa.RecordBatch._import_from_c(record_batch.array(), record_batch.schema()))
    returned_table = pa.Table.from_batches(pa_record_batches)
    assert table.equals(returned_table)


def test_multiple_record_batches_roundtrip():
    t0 = pa.table({"col": pa.array([0, 1], pa.int64())})
    t1 = pa.table({"col": pa.array([2, 3, 4], pa.int64())})
    table = pa.concat_tables([t0, t1])
    normalizer = ArrowTableNormalizer()
    arcticdb_record_batches, _ = normalizer.normalize(table)
    pa_record_batches = []
    for record_batch in arcticdb_record_batches:
        pa_record_batches.append(pa.RecordBatch._import_from_c(record_batch.array(), record_batch.schema()))
    returned_table = pa.Table.from_batches(pa_record_batches)
    assert table.equals(returned_table)


def test_basic_write(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_basic_write"
    table = pa.table({"col": pa.array([0, 1], pa.int64())})
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


def test_basic_write_bools(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_basic_write_bools"
    table = pa.table({"col": pa.array([True, False])})
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


def test_basic_write_multiple_record_batches(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_basic_write_multiple_record_batches"
    rb0 = pa.RecordBatch.from_arrays([pa.array([0, 1], pa.int32())], names=["col"])
    rb1 = pa.RecordBatch.from_arrays([pa.array([2, 3, 4], pa.int32())], names=["col"])
    rb2 = pa.RecordBatch.from_arrays([pa.array([5, 6, 7, 8], pa.int32())], names=["col"])
    table = pa.Table.from_batches([rb0, rb1, rb2])
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


def test_basic_write_multiple_record_batches_indexed(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_basic_write_multiple_record_batches_indexed"
    rb0 = pa.RecordBatch.from_arrays([pa.array([pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")], pa.timestamp("ns")), pa.array([0, 1], pa.int32())], names=["ts", "col"])
    rb1 = pa.RecordBatch.from_arrays([pa.array([pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-04"), pd.Timestamp("2025-01-05")], pa.timestamp("ns")), pa.array([2, 3, 4], pa.int32())], names=["ts", "col"])
    rb2 = pa.RecordBatch.from_arrays([pa.array([pd.Timestamp("2025-01-06"), pd.Timestamp("2025-01-07"), pd.Timestamp("2025-01-08"), pd.Timestamp("2025-01-09")], pa.timestamp("ns")), pa.array([5, 6, 7, 8], pa.int32())], names=["ts", "col"])
    table = pa.Table.from_batches([rb0, rb1, rb2])
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.parametrize(
    "data",
    [
        [True],
        [True, False],
        [False, True, True],
        [True, False, False, True],
        [False, True, True, False, True],
    ]
)
def test_write_bools_sliced(lmdb_version_store_tiny_segment, data):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format("experimental_arrow")
    sym = "test_write_bools_sliced"
    table = pa.table(
        {
            "col": pa.array(data)
        }
    )
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


def test_basic_write_bools_multiple_record_batches(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_basic_write_bools_multiple_record_batches"
    rb0 = pa.RecordBatch.from_arrays([pa.array([True, False, True, False])], names=["col"])
    rb1 = pa.RecordBatch.from_arrays([pa.array([False, False])], names=["col"])
    rb2 = pa.RecordBatch.from_arrays([pa.array([True])], names=["col"])
    table = pa.Table.from_batches([rb0, rb1, rb2])
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


def test_write_with_index(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_with_index"
    table = pa.table({"ts": pa.array([pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")], pa.timestamp("ns")), "col": pa.array([0, 1], pa.int64())})
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.parametrize("num_rows", [1, 2, 3, 4, 5])
@pytest.mark.parametrize("num_cols", [1, 2, 3, 4, 5])
def test_write_sliced(lmdb_version_store_tiny_segment, num_rows, num_cols):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format("experimental_arrow")
    sym = "test_write_sliced"
    table = pa.table(
        {
            f"col{idx}": pa.array(np.arange(idx * num_rows, (idx + 1) * num_rows, dtype=np.uint32), pa.uint32()) for idx in range(num_cols)
        }
    )
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.parametrize("num_rows", [1, 2, 3, 4, 5])
@pytest.mark.parametrize("num_cols", [1, 2, 3, 4, 5])
def test_write_sliced_with_index(lmdb_version_store_tiny_segment, num_rows, num_cols):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format("experimental_arrow")
    lib_tool = lib.library_tool()
    sym = "test_write_sliced_with_index"
    df = pd.DataFrame(
        {
            f"col{idx}": np.arange(idx * num_rows, (idx + 1) * num_rows, dtype=np.uint32) for idx in range(num_cols)
        },
        index = pd.date_range("2025-01-01", periods=num_rows)
    )
    df.index.name = "ts"
    lib.write(sym, df)
    received_written_as_pandas = lib.read(sym).data
    index_written_as_pandas = lib_tool.read_index(sym)

    table = pa.Table.from_pandas(df)
    # from_pandas puts index columns on the end, put it back at the front
    table = table.select(["ts"] + [f"col{idx}" for idx in range(num_cols)])
    lib.write(sym, table)
    received_written_as_arrow = lib.read(sym).data
    index_written_as_arrow = lib_tool.read_index(sym)

    assert received_written_as_arrow.equals(received_written_as_pandas)

    assert_frame_equal(
        index_written_as_pandas.drop(columns=["version_id", "creation_ts"]),
        index_written_as_arrow.drop(columns=["version_id", "creation_ts"]),
    )

    data_keys_written_as_pandas = lib_tool.dataframe_to_keys(index_written_as_pandas, sym)
    data_keys_written_as_arrow = lib_tool.dataframe_to_keys(index_written_as_arrow, sym)

    for pandas_key, arrow_key in zip(data_keys_written_as_pandas, data_keys_written_as_arrow):
        pandas_df = lib_tool.read_to_dataframe(pandas_key)
        arrow_df = lib_tool.read_to_dataframe(arrow_key)
        assert_frame_equal(pandas_df, arrow_df)


def test_write_zero_record_batches(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_zero_record_batches"
    table = pa.Table.from_arrays([])
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)
