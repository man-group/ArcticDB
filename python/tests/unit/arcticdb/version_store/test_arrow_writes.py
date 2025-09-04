"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb.exceptions import SchemaException, UserInputException
from arcticdb.util.test import assert_frame_equal, assert_frame_equal_with_arrow
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
    metadata = {"hello", "there"}
    lib.write(sym, table, metadata=metadata)
    received = lib.read(sym)
    assert table.equals(received.data)
    assert received.metadata == metadata


def test_batch_write(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    table_0 = pa.table({"col0": pa.array([0, 1], pa.int16())})
    df_1 = pd.DataFrame({"col1": np.arange(2, 5, dtype=np.int32)})
    table_2 = pa.table({"col2": pa.array([5, 6, 7], pa.int64())})
    lib.batch_write(["sym0", "sym1", "sym2"], [table_0, df_1, table_2])
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
    table_2 = pa.table({"col2": pa.array([5, 6, 7], pa.int64())})
    lib.batch_write(["sym0", "sym1"], [table_0, df_1])
    lib.batch_append(["sym0", "sym1", "sym2"], [table_0, df_1, table_2])
    received_0 = lib.read("sym0").data
    assert pa.concat_tables([table_0, table_0]).equals(received_0)
    received_1 = lib.read("sym1", output_format="pandas").data
    expected_1 = pd.concat([df_1, df_1])
    expected_1.index = pd.RangeIndex(len(expected_1))
    assert_frame_equal(expected_1, received_1)
    received_2 = lib.read("sym2").data
    assert table_2.equals(received_2)


def test_write_unsupported_type(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_unsupported_type"
    table = pa.table({"col": pa.array([0, 1], pa.float16())})
    with pytest.raises(SchemaException) as e:
        lib.write(sym, table)
    assert "float16" in str(e.value)


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


@pytest.mark.parametrize("existing_data", [True, False])
def test_append(lmdb_version_store_arrow, existing_data):
    lib = lmdb_version_store_arrow
    sym = "test_append"
    if existing_data:
        write_table = pa.table({"col": pa.array([0, 1], pa.int64())})
        lib.write(sym, write_table)
    append_table = pa.table({"col": pa.array([2, 3], pa.int64())})
    lib.append(sym, append_table)

    received = lib.read(sym).data
    expected = pa.concat_tables([write_table, append_table]) if existing_data else append_table
    assert expected.equals(received)


@pytest.mark.parametrize("existing_data", [True, False])
def test_append_with_index(lmdb_version_store_arrow, existing_data):
    lib = lmdb_version_store_arrow
    sym = "test_append_with_index"
    if existing_data:
        write_table = pa.table({"ts": pa.array([pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")], pa.timestamp("ns")), "col": pa.array([0, 1], pa.int64())})
        lib.write(sym, write_table)
    append_table = pa.table({"ts": pa.array([pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-04")], pa.timestamp("ns")), "col": pa.array([3, 4], pa.int64())})
    lib.append(sym, append_table)

    received = lib.read(sym).data
    expected = pa.concat_tables([write_table, append_table]) if existing_data else append_table
    assert expected.equals(received)


@pytest.mark.parametrize("existing_data", [True, False])
def test_update(lmdb_version_store_arrow, existing_data):
    lib = lmdb_version_store_arrow
    sym = "test_update"
    if existing_data:
        write_table = pa.table(
            {
                "ts": pa.array([pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-04")], pa.timestamp("ns")),
                "col": pa.array([0, 1, 2, 3], pa.int64())
            }
        )
        lib.write(sym, write_table)
    update_table = pa.table(
        {
            "ts": pa.array([pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-03")], pa.timestamp("ns")),
            "col": pa.array([4, 5], pa.int64())
        }
    )
    lib.update(sym, update_table, upsert=not existing_data)

    received = lib.read(sym).data
    if existing_data:
        expected = pa.table(
            {
                "ts": pa.array([pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-04")], pa.timestamp("ns")),
                "col": pa.array([0, 4, 5, 3], pa.int64())
            }
        )
    else:
        expected = update_table
    assert expected.equals(received)


@pytest.mark.xfail(reason="SDKLJFCSDLIHSDF")
@pytest.mark.parametrize(
    "date_range",
    [
        # (pd.Timestamp("2025-01-01 12:00:00"), pd.Timestamp("2025-01-05 12:00:00")),
        (pd.Timestamp("2025-01-02 12:00:00"), pd.Timestamp("2025-01-03 12:00:00")),
        # (pd.Timestamp("2025-01-01 12:00:00"), None),
        # (None, pd.Timestamp("2025-01-03 12:00:00")),
    ]
)
def test_update_with_date_range(lmdb_version_store_arrow, date_range):
    lib = lmdb_version_store_arrow
    sym = "test_update_with_date_range"
    reference_sym = "reference"
    write_table = pa.table(
        {
            "ts": pa.array([pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-04"), pd.Timestamp("2025-01-05"), pd.Timestamp("2025-01-06")], pa.timestamp("ns")),
            "col": pa.array([0, 1, 2, 3, 4, 5], pa.int64())
        }
    )
    lib.write(reference_sym, write_table.to_pandas().set_index("ts"))
    lib.write(sym, write_table)
    update_table = pa.table(
        {
            "ts": pa.array([pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-04")], pa.timestamp("ns")),
            "col": pa.array([6, 7], pa.int64())
        }
    )
    lib.update(reference_sym, update_table.to_pandas().set_index("ts"), date_range=date_range)
    # lib.update(sym, update_table, date_range=date_range)
    expected = lib.read(reference_sym, output_format="pandas").data
    print("fin")



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


@pytest.mark.xfail(reason="Not implemented yet, issue number 9929831600")
def test_write_with_timezone(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_with_timezone"
    table = pa.table(
        {
            "ts": pa.array([pd.Timestamp("1970-01-01"), pd.Timestamp("1970-01-02")], pa.timestamp("ns", tz="America/New_York")),
            "col": pa.array([pd.Timestamp("1970-01-04"), pd.Timestamp("1970-01-03")], pa.timestamp("ns", tz="Europe/Amsterdam")),
        }
    )
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.parametrize("unit", ["us", "ms", "s"])
def test_write_with_non_nanosecond_time_types(lmdb_version_store_arrow, unit):
    lib = lmdb_version_store_arrow
    sym = "test_write_with_non_nanosecond_time_types"
    table = pa.table(
        {
            "ts": pa.array([pd.Timestamp("1970-01-01"), pd.Timestamp("1970-01-02")], pa.timestamp(unit)),
            "col": pa.array([pd.Timestamp("1970-01-04"), pd.Timestamp("1970-01-03")], pa.timestamp(unit)),
        }
    )
    lib.write(sym, table)
    received = lib.read(sym).data
    # TODO: Remove when 9951777416 is done
    for (i, name) in enumerate(received.column_names):
        received = received.set_column(i, name, received.column(name).cast(pa.timestamp(unit)))
    assert table.equals(received)


def test_write_with_out_of_range_timestamps(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_with_out_of_range_timestamps"
    table = pa.table(
        {
            "ts": pa.array([pd.Timestamp("1970-01-01"), pd.Timestamp("1970-01-02")], pa.timestamp("s")),
            "col": pa.array([pd.Timestamp("1970-01-04"), pd.Timestamp("2300-01-01")], pa.timestamp("s")),
        }
    )
    with pytest.raises(UserInputException):
        lib.write(sym, table)


@pytest.mark.parametrize("method", ["write_parallel", "write_incomplete", "append", "stage"])
def test_staging_without_sorting(version_store_factory, method):
    lib = version_store_factory(segment_row_size=2, dynamic_schema=True)
    lib_tool = lib.library_tool()
    lib.set_output_format("experimental_arrow")
    sym = "test_staging_without_sorting"
    table_0 = pa.table(
        {
            "ts": pa.array([pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-03")], pa.timestamp("ns")),
            "col0": pa.array([0, 1, 2], pa.uint16()),
            "col1": pa.array([10, 11, 12], pa.uint8()),
            "col2": pa.array([20, 21, 22], pa.uint32()),
        }
    )
    table_1 = pa.table(
        {
            "ts": pa.array([pd.Timestamp("2025-01-04"), pd.Timestamp("2025-01-05"), pd.Timestamp("2025-01-06")], pa.timestamp("ns")),
            "col0": pa.array([3, 4, 5], pa.uint16()),
            "col1": pa.array([13, 14, 15], pa.uint8()),
            "col2": pa.array([23, 24, 25], pa.uint32()),
        }
    )
    if method == "write_parallel":
        lib.write(sym, table_0, parallel=True)
        lib.write(sym, table_1, parallel=True)
    elif method == "write_incomplete":
        lib.write(sym, table_0, incomplete=True)
        lib.write(sym, table_1, incomplete=True)
    elif method == "append":
        lib.append(sym, table_0, incomplete=True)
        lib.append(sym, table_1, incomplete=True)
    elif method == "stage":
        lib.stage(sym, table_0)
        lib.stage(sym, table_1)

    assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym)) == 4
    lib.compact_incomplete(sym, False, False)
    expected = pa.concat_tables([table_0, table_1])
    received = lib.read(sym).data
    # TODO: Remove this when timeseries indexes with norm metadata implemented for Arrow
    received = received.rename_columns({"time": "ts"})
    assert expected.equals(received)


def test_staging_with_sorting(version_store_factory):
    lib = version_store_factory(segment_row_size=2, dynamic_schema=True)
    lib_tool = lib.library_tool()
    lib.set_output_format("experimental_arrow")
    sym = "test_staging_with_sorting"
    table_0 = pa.table(
        {
            "ts": pa.array([pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")], pa.timestamp("ns")),
            "col0": pa.array([0, 1, 2], pa.uint16()),
            "col1": pa.array([10, 11, 12], pa.uint8()),
            "col2": pa.array([20, 21, 22], pa.uint32()),
        }
    )
    table_1 = pa.table(
        {
            "ts": pa.array([pd.Timestamp("2025-01-04"), pd.Timestamp("2025-01-06"), pd.Timestamp("2025-01-05")], pa.timestamp("ns")),
            "col0": pa.array([3, 4, 5], pa.uint16()),
            "col1": pa.array([13, 14, 15], pa.uint8()),
            "col2": pa.array([23, 24, 25], pa.uint32()),
        }
    )
    lib.stage(sym, table_0, sort_on_index=True)
    lib.stage(sym, table_1, sort_on_index=True)

    assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym)) == 4
    lib.compact_incomplete(sym, False, False)
    expected = pa.concat_tables([table_0, table_1]).sort_by("ts")
    received = lib.read(sym).data
    # TODO: Remove this when timeseries indexes with norm metadata implemented for Arrow
    received = received.rename_columns({"time": "ts"})
    assert expected.equals(received)


def test_recursive_normalizers(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_recursive_normalizers"
    table_0 = pa.table({"col0": pa.array([0, 1], pa.int8())})
    df_1 = pd.DataFrame({"col1": [2, 3, 4]})
    table_2 = pa.table({"col2": pa.array([5, 6, 7, 8], pa.int32())})
    list_data = [table_0, df_1, table_2]
    lib.write(sym, list_data, recursive_normalizers=True)

    assert not lib.is_symbol_pickled(sym)
    received = lib.read(sym).data
    assert len(received) == 3
    assert table_0.equals(received[0])
    assert_frame_equal_with_arrow(df_1, received[1])
    assert table_2.equals(received[2])

    dict_data = {
        "a": table_0,
        "b": {
            "c": [df_1],
            "d": table_2,
        }
    }
    lib.write(sym, dict_data, recursive_normalizers=True)
    assert not lib.is_symbol_pickled(sym)

    received = lib.read(sym).data
    assert isinstance(received, dict)
    assert "a" in received.keys() and "b" in received.keys()
    assert table_0.equals(received["a"])
    assert "c" in received["b"].keys() and "d" in received["b"].keys()
    assert isinstance(received["b"]["c"], list) and len(received["b"]["c"]) == 1
    assert_frame_equal_with_arrow(df_1, received["b"]["c"][0])
    assert table_2.equals(received["b"]["d"])
    # TODO: Test reading back as Pandas when this works generally
