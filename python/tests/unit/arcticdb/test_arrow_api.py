import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb import LazyDataFrame, DataError, concat
from arcticdb.options import OutputFormat
from arcticdb.util.test import assert_frame_equal_with_arrow, sample_dataframe

from arcticdb.version_store.library import ArcticUnsupportedDataTypeException, WritePayload, UpdatePayload

all_output_format_args = [
    None,
    OutputFormat.PANDAS,
    "PANDAS",
    "pandas",
    OutputFormat.EXPERIMENTAL_ARROW,
    "EXPERIMENTAL_ARROW",
    "experimental_arrow",
]
no_str_output_format_args = [None, OutputFormat.PANDAS, OutputFormat.EXPERIMENTAL_ARROW]


def expected_output_type(arctic_output_format, library_output_format, output_format_override):
    expected_output_format = (
        output_format_override or library_output_format or arctic_output_format or OutputFormat.PANDAS
    )
    return pa.Table if expected_output_format.lower() == OutputFormat.EXPERIMENTAL_ARROW.lower() else pd.DataFrame


@pytest.mark.parametrize("arctic_output_format", no_str_output_format_args)
@pytest.mark.parametrize("library_output_format", all_output_format_args)
@pytest.mark.parametrize("output_format_override", all_output_format_args)
def test_read_arctic(lmdb_storage, lib_name, arctic_output_format, library_output_format, output_format_override):
    ac = lmdb_storage.create_arctic(output_format=arctic_output_format)
    lib = ac.create_library(lib_name, output_format=library_output_format)
    sym = "sym"
    df = sample_dataframe()
    lib.write(sym, df)
    result = lib.read(sym, output_format=output_format_override).data
    assert isinstance(result, expected_output_type(arctic_output_format, library_output_format, output_format_override))
    assert_frame_equal_with_arrow(df, result)


@pytest.mark.parametrize("arctic_output_format", no_str_output_format_args)
@pytest.mark.parametrize("output_format_override", no_str_output_format_args)
def test_head(lmdb_storage, lib_name, arctic_output_format, output_format_override):
    ac = lmdb_storage.create_arctic(output_format=arctic_output_format)
    lib = ac.create_library(lib_name)
    sym = "sym"
    df = sample_dataframe()
    lib.write(sym, df)
    result = lib.head(sym, n=10, output_format=output_format_override).data
    assert isinstance(result, expected_output_type(arctic_output_format, None, output_format_override))
    expected = df.iloc[:10].reset_index(drop=True)
    assert_frame_equal_with_arrow(expected, result)


@pytest.mark.parametrize("arctic_output_format", no_str_output_format_args)
@pytest.mark.parametrize("output_format_override", no_str_output_format_args)
def test_tail(lmdb_storage, lib_name, arctic_output_format, output_format_override):
    ac = lmdb_storage.create_arctic(output_format=arctic_output_format)
    lib = ac.create_library(lib_name)
    sym = "sym"
    df = sample_dataframe()
    lib.write(sym, df)
    result = lib.tail(sym, n=10, output_format=output_format_override).data
    assert isinstance(result, expected_output_type(arctic_output_format, None, output_format_override))
    expected = df.iloc[-10:].reset_index(drop=True)
    assert_frame_equal_with_arrow(expected, result)


@pytest.mark.parametrize("arctic_output_format", no_str_output_format_args)
@pytest.mark.parametrize("output_format_override", no_str_output_format_args)
def test_lazy_read(lmdb_storage, lib_name, arctic_output_format, output_format_override):
    ac = lmdb_storage.create_arctic(output_format=arctic_output_format)
    lib = ac.create_library(lib_name)
    sym = "sym"
    df = sample_dataframe()
    lib.write(sym, df)
    row_range = (len(df) // 4, len(df) * 3 // 4)

    lazy_df = lib.read(sym, output_format=output_format_override, lazy=True)
    assert isinstance(lazy_df, LazyDataFrame)
    lazy_df = lazy_df.row_range(row_range)
    result = lazy_df.collect().data

    assert isinstance(result, expected_output_type(arctic_output_format, None, output_format_override))
    expected_df = df.iloc[row_range[0] : row_range[1], :].reset_index(drop=True)
    assert_frame_equal_with_arrow(expected_df, result)


@pytest.mark.parametrize("arctic_output_format", no_str_output_format_args)
@pytest.mark.parametrize("output_format_override", no_str_output_format_args)
def test_read_batch(lmdb_storage, lib_name, arctic_output_format, output_format_override):
    ac = lmdb_storage.create_arctic(output_format=arctic_output_format)
    lib = ac.create_library(lib_name)
    syms = ["sym", "sym_1", "sym_2"]
    syms_to_read = ["sym", "missing", "sym_1", "sym_2", "other_missing"]
    expected_df = {}
    for sym in syms:
        df = sample_dataframe()
        expected_df[sym] = df
        lib.write(sym, df)

    batch_results = lib.read_batch(syms_to_read, output_format=output_format_override)
    output_type = expected_output_type(arctic_output_format, None, output_format_override)
    for result in batch_results:
        sym = result.symbol
        if sym in syms:
            assert isinstance(result.data, output_type)
            assert_frame_equal_with_arrow(expected_df[sym], result.data)
        else:
            assert isinstance(result, DataError)


@pytest.mark.parametrize("arctic_output_format", no_str_output_format_args)
@pytest.mark.parametrize("output_format_override", no_str_output_format_args)
def test_read_batch_and_join(lmdb_storage, lib_name, arctic_output_format, output_format_override):
    ac = lmdb_storage.create_arctic(output_format=arctic_output_format)
    lib = ac.create_library(lib_name)
    syms = ["sym", "sym_1", "sym_2"]
    expected_dfs = []
    for sym in syms:
        df = sample_dataframe()
        expected_dfs.append(df)
        lib.write(sym, df)

    lazy_dfs = lib.read_batch(syms, output_format=output_format_override, lazy=True)
    result = concat(lazy_dfs).collect().data
    assert isinstance(result, expected_output_type(arctic_output_format, None, output_format_override))
    expected_df = pd.concat(expected_dfs).reset_index(drop=True)
    assert_frame_equal_with_arrow(expected_df, result)


@pytest.mark.parametrize("allow_arrow_input", [None, False, True])
def test_basic_modifications(lmdb_library, allow_arrow_input):
    lib = lmdb_library
    sym = "test_basic_modifications"
    if allow_arrow_input is not None:
        lib._nvs._set_allow_arrow_input(allow_arrow_input)
    write_table = pa.table(
        {
            "col": pa.array([1, 2], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
        }
    )
    append_table = pa.table(
        {
            "col": pa.array([3, 4], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-03", periods=2), type=pa.timestamp("ns")),
        }
    )
    update_table = pa.table(
        {
            "col": pa.array([5, 6], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-02", periods=2), type=pa.timestamp("ns")),
        }
    )
    if allow_arrow_input:
        lib.write(sym, write_table, index_column="ts")
        lib.append(sym, append_table, index_column="ts")
        lib.update(sym, update_table, index_column="ts")
        received = lib.read(sym, output_format=OutputFormat.EXPERIMENTAL_ARROW).data
        expected = pa.table(
            {
                "col": pa.array([1, 5, 6, 4], pa.int64()),
                "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=4), type=pa.timestamp("ns")),
            }
        )
        assert expected.equals(received)
    else:
        with pytest.raises(ArcticUnsupportedDataTypeException):
            lib.write(sym, write_table, index_column="ts")
        with pytest.raises(Exception):
            lib.append(sym, append_table, index_column="ts")
        with pytest.raises(Exception):
            lib.update(sym, update_table, upsert=True, index_column="ts")


@pytest.mark.parametrize("allow_arrow_input", [None, False, True])
def test_batch_modifications(lmdb_library, allow_arrow_input):
    lib = lmdb_library
    sym = "test_batch_modifications"
    if allow_arrow_input is not None:
        lib._nvs._set_allow_arrow_input(allow_arrow_input)
    write_table = pa.table(
        {
            "col": pa.array([1, 2], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
        }
    )
    append_table = pa.table(
        {
            "col": pa.array([3, 4], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-03", periods=2), type=pa.timestamp("ns")),
        }
    )
    update_table = pa.table(
        {
            "col": pa.array([5, 6], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-02", periods=2), type=pa.timestamp("ns")),
        }
    )
    if allow_arrow_input:
        lib.write_batch([WritePayload(sym, write_table, index_column="ts")])
        lib.append_batch([WritePayload(sym, append_table, index_column="ts")])
        lib.update_batch([UpdatePayload(sym, update_table, index_column="ts")])
        received = lib.read(sym, output_format=OutputFormat.EXPERIMENTAL_ARROW).data
        expected = pa.table(
            {
                "col": pa.array([1, 5, 6, 4], pa.int64()),
                "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=4), type=pa.timestamp("ns")),
            }
        )
        assert expected.equals(received)
    else:
        with pytest.raises(ArcticUnsupportedDataTypeException):
            lib.write_batch([WritePayload(sym, write_table, index_column="ts")])
        with pytest.raises(Exception):
            lib.append_batch([WritePayload(sym, append_table, index_column="ts")])
        with pytest.raises(Exception):
            lib.update_batch([WritePayload(sym, update_table, index_column="ts")], upsert=True)


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("allow_arrow_input", [None, False, True])
def test_write_pickle(lmdb_library, batch, allow_arrow_input):
    lib = lmdb_library
    sym = "test_write_pickle"
    if allow_arrow_input is not None:
        lib._nvs._set_allow_arrow_input(allow_arrow_input)
    table = pa.table(
        {
            "col": pa.array([1, 2], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
        }
    )
    if batch:
        lib.write_pickle_batch([WritePayload(sym, table)])
    else:
        lib.write_pickle(sym, table)
    if allow_arrow_input:
        assert not lib._nvs.is_symbol_pickled(sym)
        received = lib.read(sym, output_format=OutputFormat.EXPERIMENTAL_ARROW).data
        assert table.equals(received)
    else:
        assert lib._nvs.is_symbol_pickled(sym)
        received = lib.read(sym).data
        assert table.equals(received)


@pytest.mark.parametrize("allow_arrow_input", [None, False, True])
def test_stage(lmdb_library, allow_arrow_input):
    lib = lmdb_library
    sym = "test_stage"
    if allow_arrow_input is not None:
        lib._nvs._set_allow_arrow_input(allow_arrow_input)
    table_0 = pa.table(
        {
            "col": pa.array([1, 2], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
        }
    )
    table_1 = pa.table(
        {
            "col": pa.array([3, 4], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-03", periods=2), type=pa.timestamp("ns")),
        }
    )
    if allow_arrow_input:
        lib.stage(sym, table_0, index_column="ts")
        lib.stage(sym, table_1, index_column="ts")
        lib.finalize_staged_data(sym)
        received = lib.read(sym, output_format=OutputFormat.EXPERIMENTAL_ARROW).data
        expected = pa.table(
            {
                "col": pa.array([1, 2, 3, 4], pa.int64()),
                "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=4), type=pa.timestamp("ns")),
            }
        )
        assert expected.equals(received)
    else:
        with pytest.raises(ArcticUnsupportedDataTypeException):
            lib.stage(sym, table_0, index_column="ts")
