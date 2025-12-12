"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from hypothesis import given, settings
import hypothesis.strategies as st
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import random

from arcticdb.exceptions import ArcticException, SchemaException, StreamDescriptorMismatch, UserInputException
from arcticdb.options import ArrowOutputStringFormat
from arcticdb.util.arrow import cast_string_columns
from arcticdb.util.test import assert_frame_equal, assert_frame_equal_with_arrow
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked
from arcticdb.version_store._normalization import ArrowTableNormalizer
from arcticdb_ext.storage import KeyType
from tests.util.naughty_strings import read_big_list_of_naughty_strings


def test_record_batches_roundtrip():
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


def test_write_zero_record_batches(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_zero_record_batches"
    table = pa.Table.from_arrays([])
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


# Arrow stores bools as packed bitsets so worth testing separately even in scenarios as basic as this
@pytest.mark.parametrize("type", [pa.int64(), pa.bool_()])
def test_basic_write(lmdb_version_store_arrow, type):
    lib = lmdb_version_store_arrow
    sym = "test_basic_write"
    table = pa.table({"col": pa.array([0, 1] if type == pa.int64() else [True, False], type)})
    metadata = {"hello", "there"}
    lib.write(sym, table, metadata=metadata)
    received = lib.read(sym)
    assert table.equals(received.data)
    assert received.metadata == metadata


@pytest.mark.parametrize("type", [pa.string(), pa.large_string()])
def test_basic_write_strings(lmdb_version_store_arrow, type):
    lib = lmdb_version_store_arrow
    sym = "test_basic_write_strings"
    table = pa.table({"col": pa.array(["hello", "bonjour", "gutentag", "nihao", "konnichiwa"], type)})
    lib.write(sym, table)
    received = lib.read(sym, arrow_string_format_default=type).data
    assert table.equals(received)


@pytest.mark.skip(reason="Not implemented yet 9951777416")
@pytest.mark.parametrize("type", [pa.timestamp("us"), pa.timestamp("ms"), pa.timestamp("s")])
@pytest.mark.parametrize("index_column", [None, "ts"])
def test_write_with_non_nanosecond_time_types(lmdb_version_store_arrow, type, index_column):
    lib = lmdb_version_store_arrow
    sym = "test_write_with_non_nanosecond_time_types"
    table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("1970-01-01", periods=2), type=type),
            "col": pa.Array.from_pandas(pd.date_range("1970-01-03", periods=2), type=type),
        }
    )
    lib.write(sym, table, index_column=index_column)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.parametrize("type", [pa.int64(), pa.bool_(), pa.string(), pa.large_string()])
def test_write_multiple_record_batches(lmdb_version_store_arrow, type):
    lib = lmdb_version_store_arrow
    sym = "test_write_multiple_record_batches"
    if type == pa.int64():
        arr0 = pa.array([0, 1], type)
        arr1 = pa.array([2, 3, 4], type)
        arr2 = pa.array([5, 6, 7, 8], type)
    elif type == pa.bool_():
        arr0 = pa.array([True, False], type)
        arr1 = pa.array([True, False, False], type)
        arr2 = pa.array([False, False, False, False], type)
    else:
        # String type
        arr0 = pa.array(["1", "22"], type)
        arr1 = pa.array(["333", "4444", "55555"], type)
        arr2 = pa.array(["666666", "7777777", "88888888", "999999999"], type)
    rb0 = pa.RecordBatch.from_arrays([arr0], names=["col"])
    rb1 = pa.RecordBatch.from_arrays([arr1], names=["col"])
    rb2 = pa.RecordBatch.from_arrays([arr2], names=["col"])
    table = pa.Table.from_batches([rb0, rb1, rb2])
    lib.write(sym, table)
    arrow_string_format = type if type == pa.large_string() or type == pa.string() else None
    received = lib.read(sym, arrow_string_format_default=arrow_string_format).data
    assert table.equals(received)


@pytest.mark.parametrize("index_col_position", [0, 1])
def test_write_with_index(lmdb_version_store_arrow, index_col_position):
    lib = lmdb_version_store_arrow
    sym = "test_write_with_index"
    table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
            "col0": pa.array([0, 1], pa.int64()),
            "col1": pa.array(["hello", "bonjour"], pa.string()),
        }
    )
    if index_col_position == 1:
        table = table.select([1, 0, 2])
    lib.write(sym, table, index_column="ts")
    received = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.SMALL_STRING).data
    assert table.equals(received)


def test_write_multiple_record_batches_indexed(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_multiple_record_batches_indexed"
    rb0 = pa.RecordBatch.from_arrays(
        [
            pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
            pa.array([0, 1], pa.int32()),
            pa.array(["a", "bb"], pa.large_string()),
        ],
        names=["ts", "col0", "col1"],
    )
    rb1 = pa.RecordBatch.from_arrays(
        [
            pa.Array.from_pandas(pd.date_range("2025-01-03", periods=3), type=pa.timestamp("ns")),
            pa.array([2, 3, 4], pa.int32()),
            pa.array(["ccc", "dd", "e"], pa.large_string()),
        ],
        names=["ts", "col0", "col1"],
    )
    rb2 = pa.RecordBatch.from_arrays(
        [
            pa.Array.from_pandas(pd.date_range("2025-01-06", periods=4), type=pa.timestamp("ns")),
            pa.array([5, 6, 7, 8], pa.int32()),
            pa.array(["f", "gg", "hh", "i"], pa.large_string()),
        ],
        names=["ts", "col0", "col1"],
    )
    table = pa.Table.from_batches([rb0, rb1, rb2])
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.parametrize("num_rows", [1, 2, 3, 4, 5])
@pytest.mark.parametrize("num_cols", [1, 2, 3, 4, 5])
def test_write_sliced(lmdb_version_store_tiny_segment, num_rows, num_cols):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_write_sliced"
    table = pa.table(
        {
            f"col{idx}": (
                pa.array(np.arange(idx * num_rows, (idx + 1) * num_rows, dtype=np.uint32), pa.uint32())
                if idx % 2 == 0
                else pa.array([f"{row}" for row in range(num_rows)], pa.string())
            )
            for idx in range(num_cols)
        }
    )
    lib.write(sym, table)
    received = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.SMALL_STRING).data
    assert table.equals(received)


# Test slicing of bools separately from other numeric types as the Arrow packed bitset representation could mean the
# implementation differs significantly, although this is not the case at time of writing (25/9/25)
@pytest.mark.parametrize(
    "data",
    [
        [True],
        [True, False],
        [False, True, True],
        [True, False, False, True],
        [False, True, True, False, True],
    ],
)
def test_write_bools_sliced(lmdb_version_store_tiny_segment, data):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_write_bools_sliced"
    table = pa.table({"col": pa.array(data)})
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.parametrize("num_rows", [1, 2, 3, 4, 5])
@pytest.mark.parametrize("num_cols", [1, 2, 3, 4, 5])
def test_write_sliced_with_index(lmdb_version_store_tiny_segment, num_rows, num_cols):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    lib_tool = lib.library_tool()
    sym = "test_write_sliced_with_index"
    df = pd.DataFrame(
        {f"col{idx}": np.arange(idx * num_rows, (idx + 1) * num_rows, dtype=np.uint32) for idx in range(num_cols)},
        index=pd.date_range("2025-01-01", periods=num_rows),
    )
    df.index.name = "ts"
    lib.write(sym, df)
    received_written_as_pandas = lib.read(sym).data
    index_written_as_pandas = lib_tool.read_index(sym)

    table = pa.Table.from_pandas(df)
    # from_pandas puts index columns on the end, put it back at the front
    table = table.select(["ts"] + [f"col{idx}" for idx in range(num_cols)])
    lib.write(sym, table, index_column="ts")
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


@pytest.mark.parametrize("rows_per_slice", [1, 2, 10, 100_000])
def test_many_record_batches_many_slices(version_store_factory, rows_per_slice):
    rng = np.random.default_rng()
    lib = version_store_factory(segment_row_size=rows_per_slice)
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_many_record_batches_many_slices"
    record_batch_sizes = [1, 2, 1, 15, 13, 2, 1, 10]
    tables = []
    for idx, length in enumerate(record_batch_sizes):
        tables.append(
            pa.table(
                {
                    "int32": pa.array(rng.integers(0, 1_000_000, length, dtype=np.int32), pa.int32()),
                    "float64": pa.array(rng.random(length), pa.float64()),
                    "bool": pa.array(rng.choice([True, False], length), pa.bool_()),
                    "string": pa.array([f"{i}" for i in range(length)], pa.string()),
                }
            )
        )
    table = pa.concat_tables(tables)
    lib.write(sym, table)
    received = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.SMALL_STRING).data
    assert table.equals(received)


def test_write_view(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_view"
    table = pa.table(
        {
            "numeric": pa.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], pa.uint16()),
            "bool": pa.array(5 * [True, False], pa.bool_()),
        }
    )
    view = table.slice(3, 3)
    lib.write(sym, view)
    received = lib.read(sym).data
    expected = pa.table(
        {"numeric": pa.array([3, 4, 5], pa.uint16()), "bool": pa.array([False, True, False], pa.bool_())}
    )
    assert expected.equals(received)


@pytest.mark.parametrize("type", [pa.string(), pa.large_string()])
def test_write_view_strings(lmdb_version_store_arrow, type):
    lib = lmdb_version_store_arrow
    sym = "test_write_view_strings"
    table_0 = pa.table({"col": pa.array(["hello", "bonjour", "gutentag"], type)})
    table_1 = pa.table({"col": pa.array(["dog", "bats", "on"], type)})
    table = pa.concat_tables([table_0, table_1])
    view = table.slice(1, 4)
    lib.write(sym, view)
    received = lib.read(sym, arrow_string_format_default=type).data
    assert view.equals(received)


def test_write_owned_and_non_owned_buffers(lmdb_version_store_tiny_segment):
    # This test is about our ChunkedBuffer holding mixes of owned and non-owned blocks, not Arrow views
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_write_owned_and_non_owned_buffers"
    rb0 = pa.RecordBatch.from_arrays([pa.array([0, 1, 2], pa.int32())], names=["col"])
    rb1 = pa.RecordBatch.from_arrays([pa.array([3, 4, 5], pa.int32())], names=["col"])
    table = pa.Table.from_batches([rb0, rb1])
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.xfail(reason="Not implemented yet, issue number 9929831600")
def test_write_with_timezone(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_with_timezone"
    table = pa.table(
        {
            "ts": pa.Array.from_pandas(
                pd.date_range("2025-01-01", periods=2, tz="America/New_York"),
                type=pa.timestamp("ns", tz="America/New_York"),
            ),
            "col": pa.Array.from_pandas(
                pd.date_range("2025-01-03", periods=2, tz="Europe/Amsterdam"),
                type=pa.timestamp("ns", tz="Europe/Amsterdam"),
            ),
        }
    )
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


# TODO: Remove this test and replace with comprehensive sparse data testing as part of 9838111397
@pytest.mark.parametrize("data", [pa.array([0, None, 1], pa.int64()), pa.array([None, True, False, None], pa.bool_())])
def test_write_sparse_data(lmdb_version_store_arrow, data):
    lib = lmdb_version_store_arrow
    sym = "test_write_sparse_data"
    table = pa.table({"my_col": data})
    with pytest.raises(SchemaException) as e:
        lib.write(sym, table)
    assert "my_col" in str(e.value)


@pytest.mark.xfail(reason="Index column position not alway correct yet, issue number 18042073623")
def test_write_with_index_and_read_with_column_slicing(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_with_index_and_read_with_column_slicing"
    table = pa.table(
        {
            "col1": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
            "col2": pa.Array.from_pandas(pd.date_range("2025-01-03", periods=2), type=pa.timestamp("ns")),
            "col3": pa.Array.from_pandas(pd.date_range("2025-01-05", periods=2), type=pa.timestamp("ns")),
        }
    )
    lib.write(sym, table, index_column="col3")
    received = lib.read(sym, columns=["col1"]).data
    expected = table.select([0, 2])
    assert expected.equals(received)


def test_write_non_existent_index_column(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_non_existent_index_column"
    table = pa.table({"col": pa.array([0, 1], pa.int64())})
    with pytest.raises(SchemaException) as e:
        lib.write(sym, table, index_column="blah")
    assert "blah" in str(e.value)


def test_write_non_timestamp_index_column(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_non_timestamp_index_column"
    table = pa.table({"non-ts": pa.array([0, 1], pa.int64()), "col": pa.array([2, 3], pa.int64())})
    with pytest.raises(UserInputException) as e:
        lib.write(sym, table, index_column="non-ts")
    assert "int64" in str(e.value).lower()


def test_write_unsupported_types(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_unsupported_types"
    table = pa.table({"col": pa.array(np.arange(2, dtype=np.float16), pa.float16())})
    with pytest.raises(SchemaException) as e:
        lib.write(sym, table)
    assert "unsupported" in str(e.value).lower()

    table = pa.table({"col": pa.compute.dictionary_encode(pa.array(["hello", "goodbye"], pa.string()))})
    assert pa.types.is_dictionary(table.column(0).type)
    with pytest.raises(Exception) as e:
        lib.write(sym, table)
    assert "unsupported" in str(e.value).lower()


# Reinstate if bounds check is re-added in WriteToSegmentTask::slice_column when 9951777416 is implemented
# def test_write_with_out_of_range_timestamps(lmdb_version_store_arrow):
#     lib = lmdb_version_store_arrow
#     sym = "test_write_with_out_of_range_timestamps"
#     table = pa.table(
#         {
#             "ts": pa.array([pa.scalar(0, type=pa.timestamp("s")), pa.scalar(1, type=pa.timestamp("s"))]),
#             # 10 billion seconds is 10^19 nanoseconds, which is larger than the max value of an int64
#             "col": pa.array(
#                 [pa.scalar(3, type=pa.timestamp("s")), pa.scalar(10_000_000_000, type=pa.timestamp("s"))],
#             ),
#         }
#     )
#     with pytest.raises(UserInputException):
#         lib.write(sym, table)


@pytest.mark.parametrize("existing_data", [True, False])
def test_append(lmdb_version_store_arrow, existing_data):
    lib = lmdb_version_store_arrow
    sym = "test_append"
    if existing_data:
        write_table = pa.table({"col0": pa.array([0, 1], pa.int64()), "col1": pa.array(["a", "bb"], pa.string())})
        lib.write(sym, write_table)
    append_table = pa.table({"col0": pa.array([2, 3], pa.int64()), "col1": pa.array(["ccc", "dddd"], pa.string())})
    lib.append(sym, append_table)

    received = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.SMALL_STRING).data
    expected = pa.concat_tables([write_table, append_table]) if existing_data else append_table
    assert expected.equals(received)


@pytest.mark.parametrize("first_type", [pa.string(), pa.large_string()])
def test_append_mix_strings_and_large_strings(lmdb_version_store_arrow, first_type):
    lib = lmdb_version_store_arrow
    sym = "test_append_mix_strings_and_large_strings"
    write_table = pa.table({"col": pa.array(["a", "bb"], first_type)})
    lib.write(sym, write_table)
    second_type = pa.large_string() if first_type == pa.string() else pa.string()
    append_table = pa.table({"col": pa.array(["ccc", "dddd"], second_type)})
    lib.append(sym, append_table)

    received = lib.read(sym).data
    expected = pa.concat_tables([write_table, append_table], promote_options="permissive")
    assert expected.equals(received)


@pytest.mark.parametrize("existing_data", [True, False])
def test_append_with_index(lmdb_version_store_arrow, existing_data):
    lib = lmdb_version_store_arrow
    sym = "test_append_with_index"
    if existing_data:
        write_table = pa.table(
            {
                "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
                "col": pa.array([0, 1], pa.int64()),
            }
        )
        lib.write(sym, write_table, index_column="ts")
    append_table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-03", periods=2), type=pa.timestamp("ns")),
            "col": pa.array([3, 4], pa.int64()),
        }
    )
    lib.append(sym, append_table, index_column="ts")

    received = lib.read(sym).data
    expected = pa.concat_tables([write_table, append_table]) if existing_data else append_table
    assert expected.equals(received)


@pytest.mark.parametrize("method", ["append", "update"])
def test_wrong_index_name(lmdb_version_store_arrow, method):
    lib = lmdb_version_store_arrow
    sym = "test_wrong_index_name"
    table_0 = pa.table(
        {
            "ts1": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
            "col": pa.array([0, 1], pa.int64()),
        }
    )
    lib.write(sym, table_0, index_column="ts1")
    table_1 = pa.table(
        {
            "ts2": pa.Array.from_pandas(pd.date_range("2025-01-03", periods=2), type=pa.timestamp("ns")),
            "col": pa.array([3, 4], pa.int64()),
        }
    )
    with pytest.raises(StreamDescriptorMismatch):
        getattr(lib, method)(sym, table_1, index_column="ts2")


@pytest.mark.parametrize("existing_data", [True, False])
def test_update(lmdb_version_store_arrow, existing_data):
    lib = lmdb_version_store_arrow
    sym = "test_update"
    if existing_data:
        write_table = pa.table(
            {
                "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=4), type=pa.timestamp("ns")),
                "col0": pa.array([0, 1, 2, 3], pa.int64()),
                "col1": pa.array(["zero", "one", "two", "three"], pa.string()),
            }
        )
        lib.write(sym, write_table, index_column="ts")
    update_table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-02", periods=2), type=pa.timestamp("ns")),
            "col0": pa.array([4, 5], pa.int64()),
            "col1": pa.array(["four", "five"], pa.string()),
        }
    )
    lib.update(sym, update_table, upsert=not existing_data, index_column="ts")

    received = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.SMALL_STRING).data
    if existing_data:
        expected = pa.table(
            {
                "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=4), type=pa.timestamp("ns")),
                "col0": pa.array([0, 4, 5, 3], pa.int64()),
                "col1": pa.array(["zero", "four", "five", "three"], pa.string()),
            }
        )
    else:
        expected = update_table
    assert expected.equals(received)


@pytest.mark.parametrize("first_type", [pa.string(), pa.large_string()])
def test_update_mix_strings_and_large_strings(lmdb_version_store_arrow, first_type):
    lib = lmdb_version_store_arrow
    sym = "test_update_mix_strings_and_large_strings"
    write_table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=4), type=pa.timestamp("ns")),
            "col": pa.array(["a", "bb", "ccc", "dddd"], first_type),
        }
    )
    lib.write(sym, write_table, index_column="ts")
    second_type = pa.large_string() if first_type == pa.string() else pa.string()
    update_table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-02", periods=2), type=pa.timestamp("ns")),
            "col": pa.array(["eeeee", "ffffff"], second_type),
        }
    )
    lib.update(sym, update_table, index_column="ts")

    received = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.SMALL_STRING).data
    expected = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=4), type=pa.timestamp("ns")),
            "col": pa.array(["a", "eeeee", "ffffff", "dddd"], pa.string()),
        }
    )
    assert expected.equals(received)


@pytest.mark.parametrize(
    "date_range",
    [
        (pd.Timestamp("2025-01-01 12:00:00"), pd.Timestamp("2025-01-05 12:00:00")),
        (pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-04")),
    ],
)
def test_update_with_date_range_wider_than_data(lmdb_version_store_arrow, date_range):
    lib = lmdb_version_store_arrow
    sym = "test_update_with_date_range_wider_than_data"
    reference_sym = "test_update_with_date_range_wider_than_data_reference"
    write_table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=6), type=pa.timestamp("ns")),
            "col": pa.array([0, 1, 2, 3, 4, 5], pa.int64()),
        }
    )
    lib.write(reference_sym, write_table.to_pandas().set_index("ts"))
    lib.write(sym, write_table, index_column="ts")
    update_table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-03", periods=2), type=pa.timestamp("ns")),
            "col": pa.array([6, 7], pa.int64()),
        }
    )
    lib.update(reference_sym, update_table.to_pandas().set_index("ts"), date_range=date_range)
    lib.update(sym, update_table, date_range=date_range, index_column="ts")
    expected = lib.read(reference_sym, output_format="pandas").data
    received = lib.read(sym).data.to_pandas().set_index("ts")
    assert_frame_equal(expected, received)


@pytest.mark.parametrize(
    "date_range",
    [
        (pd.Timestamp("2025-01-03 12:00:00"), pd.Timestamp("2025-01-04 12:00:00")),
        (pd.Timestamp("2025-01-03 00:00:00.00000001"), pd.Timestamp("2025-01-04")),
        (pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-03 23:59:59.999999999")),
    ],
)
def test_update_with_date_range_narrower_than_data(lmdb_version_store_arrow, date_range):
    lib = lmdb_version_store_arrow
    sym = "test_update_with_date_range_narrower_than_data"
    write_table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=6), type=pa.timestamp("ns")),
            "col0": pa.array([0, 1, 2, 3, 4, 5], pa.int64()),
            "col1": pa.array(["zero", "one", "two", "three", "four", "five"], pa.string()),
        }
    )
    lib.write(sym, write_table, index_column="ts")
    update_table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-03", periods=2), type=pa.timestamp("ns")),
            "col0": pa.array([6, 7], pa.int64()),
            "col1": pa.array(["six", "seven"], pa.string()),
        }
    )
    # TODO: Fold these parametrizations into above test when working
    with pytest.raises(ArcticException):
        lib.update(sym, update_table, date_range=date_range, index_column="ts")


@pytest.mark.parametrize("method", ["write_parallel", "write_incomplete", "append", "stage"])
def test_staging_without_sorting(version_store_factory, method):
    lib = version_store_factory(segment_row_size=2, dynamic_schema=True)
    lib_tool = lib.library_tool()
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_staging_without_sorting"
    table_0 = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=3), type=pa.timestamp("ns")),
            "col0": pa.array([0, 1, 2], pa.uint16()),
            "col1": pa.array([10, 11, 12], pa.uint8()),
            "col2": pa.array([20, 21, 22], pa.uint32()),
            "col3": pa.array(["zero", "one", "two"], pa.string()),
        }
    )
    table_1 = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-04", periods=3), type=pa.timestamp("ns")),
            "col0": pa.array([3, 4, 5], pa.uint16()),
            "col1": pa.array([13, 14, 15], pa.uint8()),
            "col2": pa.array([23, 24, 25], pa.uint32()),
            "col3": pa.array(["three", "four", "five"], pa.string()),
        }
    )
    if method == "write_parallel":
        lib.write(sym, table_0, parallel=True, index_column="ts")
        lib.write(sym, table_1, parallel=True, index_column="ts")
    elif method == "write_incomplete":
        lib.write(sym, table_0, incomplete=True, index_column="ts")
        lib.write(sym, table_1, incomplete=True, index_column="ts")
    elif method == "append":
        lib.append(sym, table_0, incomplete=True, index_column="ts")
        lib.append(sym, table_1, incomplete=True, index_column="ts")
    elif method == "stage":
        lib.stage(sym, table_0, index_column="ts")
        lib.stage(sym, table_1, index_column="ts")

    assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym)) == 4
    lib.compact_incomplete(sym, False, False)
    expected = pa.concat_tables([table_0, table_1])
    received = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.SMALL_STRING).data
    assert expected.equals(received)


def test_staging_with_sorting(version_store_factory):
    lib = version_store_factory(segment_row_size=2, dynamic_schema=True)
    lib_tool = lib.library_tool()
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_staging_with_sorting"
    table_0 = pa.table(
        {
            "ts": pa.array([3, 1, 2], pa.timestamp("ns")),
            "col0": pa.array([0, 1, 2], pa.uint16()),
            "col1": pa.array([10, 11, 12], pa.uint8()),
            "col2": pa.array([20, 21, 22], pa.uint32()),
        }
    )
    table_1 = pa.table(
        {
            "ts": pa.array([4, 6, 5], pa.timestamp("ns")),
            "col0": pa.array([3, 4, 5], pa.uint16()),
            "col1": pa.array([13, 14, 15], pa.uint8()),
            "col2": pa.array([23, 24, 25], pa.uint32()),
        }
    )
    lib.stage(sym, table_0, sort_on_index=True, index_column="ts")
    lib.stage(sym, table_1, sort_on_index=True, index_column="ts")

    assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym)) == 4
    lib.compact_incomplete(sym, False, False)
    expected = pa.concat_tables([table_0, table_1]).sort_by("ts")
    received = lib.read(sym).data
    assert expected.equals(received)


# Merge with test_staging_with_sorting when 18190648152 is done
@pytest.mark.xfail(reason="Not implemented yet, see issue 18190648152")
def test_staging_with_sorting_strings(version_store_factory):
    lib = version_store_factory(segment_row_size=2, dynamic_schema=True)
    lib_tool = lib.library_tool()
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_staging_with_sorting_strings"
    table_0 = pa.table(
        {
            "ts": pa.array([3, 1, 2], pa.timestamp("ns")),
            "col": pa.array(["three", "one", "two"], pa.string()),
        }
    )
    table_1 = pa.table(
        {
            "ts": pa.array([4, 6, 5], pa.timestamp("ns")),
            "col": pa.array(["four", "six", "five"], pa.string()),
        }
    )
    lib.stage(sym, table_0, sort_on_index=True, index_column="ts")
    lib.stage(sym, table_1, sort_on_index=True, index_column="ts")

    assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym)) == 4
    lib.compact_incomplete(sym, False, False)
    expected = pa.concat_tables([table_0, table_1]).sort_by("ts")
    received = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.SMALL_STRING).data
    assert expected.equals(received)


def test_recursive_normalizers(lmdb_version_store_arrow, recursive_normalizer_meta_structure_v2):
    lib = lmdb_version_store_arrow
    sym = "test_recursive_normalizers"
    table_0 = pa.table({"col0": pa.array(["hello", "there"], pa.string())})
    df_1 = pd.DataFrame({"col1": [2, 3, 4]})
    table_2 = pa.table(
        {
            "col2": pa.array([5, 6, 7, 8], pa.int32()),
            "col3": pa.array(["five", "six", "seven", "eight"], pa.large_string()),
        }
    )
    list_data = [table_0, df_1, table_2]
    lib.write(sym, list_data, recursive_normalizers=True)

    assert not lib.is_symbol_pickled(sym)
    received = lib.read(sym).data
    assert len(received) == 3
    assert table_0.equals(cast_string_columns(received[0], pa.string()))
    assert_frame_equal_with_arrow(df_1, received[1])
    assert table_2.equals(cast_string_columns(received[2], pa.large_string()))

    dict_data = {
        "a": table_0,
        "b": {
            "c": [df_1],
            "d": table_2,
        },
    }
    lib.write(sym, dict_data, recursive_normalizers=True)
    assert not lib.is_symbol_pickled(sym)

    received = lib.read(sym).data
    assert isinstance(received, dict)
    assert "a" in received.keys() and "b" in received.keys()
    assert table_0.equals(cast_string_columns(received["a"], pa.string()))
    assert "c" in received["b"].keys() and "d" in received["b"].keys()
    assert isinstance(received["b"]["c"], list) and len(received["b"]["c"]) == 1
    assert_frame_equal_with_arrow(df_1, received["b"]["c"][0])
    assert table_2.equals(cast_string_columns(received["b"]["d"], pa.large_string()))


def test_batch_write(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    table_0 = pa.table({"col0": pa.array([0, 1], pa.int16())})
    df_1 = pd.DataFrame({"col1": np.arange(2, 5, dtype=np.int32)})
    table_2 = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=3), type=pa.timestamp("ns")),
            "col2": pa.array([5, 6, 7], pa.int64()),
        }
    )
    lib.batch_write(["sym0", "sym1", "sym2"], [table_0, df_1, table_2], index_column_vector=[None, None, "ts"])
    received_0 = lib.read("sym0").data
    assert table_0.equals(received_0)
    received_1 = lib.read("sym1", output_format="pandas").data
    assert_frame_equal(df_1, received_1)
    received_2 = lib.read("sym2").data
    assert table_2.equals(received_2)


def test_batch_append(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    table_0 = pa.table({"col0": pa.array([0, 1], pa.int16())})
    df_1 = pd.DataFrame({"col1": np.arange(2, 5, dtype=np.int32)})
    table_2 = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=3), type=pa.timestamp("ns")),
            "col2": pa.array([5, 6, 7], pa.int64()),
        }
    )
    lib.batch_write(["sym0", "sym1"], [table_0, df_1], index_column_vector=[None, None, "ts"])
    table_2 = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-04", periods=3), type=pa.timestamp("ns")),
            "col2": pa.array([5, 6, 7], pa.int64()),
        }
    )
    lib.batch_append(["sym0", "sym1", "sym2"], [table_0, df_1, table_2], index_column_vector=[None, None, "ts"])
    received_0 = lib.read("sym0").data
    assert pa.concat_tables([table_0, table_0]).equals(received_0)
    received_1 = lib.read("sym1", output_format="pandas").data
    expected_1 = pd.concat([df_1, df_1])
    expected_1.index = pd.RangeIndex(len(expected_1))
    assert_frame_equal(expected_1, received_1)
    received_2 = lib.read("sym2").data
    assert table_2.equals(received_2)


supported_types = [
    pa.bool_(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64(),
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
    pa.float32(),
    pa.float64(),
    pa.string(),
    pa.large_string(),
    pa.timestamp("ns"),
]
num_supported_types = len(supported_types)


# We are more interested in the slicing than the data, so the parameters are for:
# - dataframe length
# - record batch structure
# - timeseries index position
# - library slicing settings
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df_length=st.integers(3, 200_000),
    index_position=st.integers(0, num_supported_types),
    # Defined this way as hypothesis shrinks towards smaller ints, and we want to shrink towards a single record batch/
    # row slice/ column slice
    max_record_batches=st.integers(1, 100),
    max_row_slices=st.integers(1, 100),
    max_col_slices=st.integers(1, num_supported_types),
)
def test_arrow_writes_hypothesis(
    lmdb_version_store_big_map, df_length, index_position, max_record_batches, max_row_slices, max_col_slices
):
    rng = np.random.default_rng()
    lib = lmdb_version_store_big_map
    sym = "test_arrow_writes_hypothesis"
    # version_store_factory doesn't play nicely with hypothesis, so set these values manually
    rows_per_slice = max(df_length // max_row_slices, 1)
    cols_per_slice = num_supported_types // max_col_slices
    lib.lib_cfg().lib_desc.version.write_options.segment_row_size = rows_per_slice
    lib.lib_cfg().lib_desc.version.write_options.column_group_size = cols_per_slice
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    naughty_strings = read_big_list_of_naughty_strings()
    data = {}
    for idx, supported_type in enumerate(supported_types):
        if idx == index_position:
            data["ts"] = pa.Array.from_pandas(
                pd.date_range("2025-01-01", freq="s", periods=df_length), type=pa.timestamp("ns")
            )
        if supported_type in {pa.string(), pa.large_string()}:
            data[str(supported_type)] = pa.array(
                [random.choice(naughty_strings) for _ in range(df_length)], supported_type
            )
        else:
            data[str(supported_type)] = pa.array(rng.integers(0, 100, size=df_length), supported_type)
    if index_position == num_supported_types:
        data["ts"] = pa.Array.from_pandas(
            pd.date_range("2025-01-01", freq="s", periods=df_length), type=pa.timestamp("ns")
        )
    original_table = pa.table(data)
    # original_table.to_batches with max_chunksize creates a zero copy view, take actually copies the data
    max_chunksize = max((df_length + max_record_batches) // max_record_batches, 1)
    tables = []
    for i in range(max_record_batches):
        if i * max_chunksize < df_length:
            tables.append(original_table.take(list(range(i * max_chunksize, min((i + 1) * max_chunksize, df_length)))))
        else:
            break
    table = pa.concat_tables(tables)
    assert table.equals(original_table)
    lib.write(sym, table.slice(0, df_length // 3), index_column="ts")
    lib.append(sym, table.slice((2 * df_length) // 3), index_column="ts")
    lib.update(sym, table.slice(df_length // 3, ((2 * df_length) // 3) - (df_length // 3)), index_column="ts")
    received = lib.read(sym).data
    for i, name in enumerate(received.column_names):
        if pa.types.is_large_string(received.column(i).type):
            received = received.set_column(
                i, name, received.column(name).cast(pa.string() if name == str(pa.string()) else pa.large_string())
            )
    assert table.equals(received)
