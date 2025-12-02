import numpy as np
import pandas as pd
import pyarrow as pa
import polars as pl
import pytest

from arcticdb import LazyDataFrame, DataError, concat
from arcticdb.exceptions import ArcticNativeException
from arcticdb.options import OutputFormat, ArrowOutputStringFormat, LibraryOptions
from arcticdb.util.test import assert_frame_equal_with_arrow, sample_dataframe

from arcticdb.version_store.library import ArcticUnsupportedDataTypeException, WritePayload, UpdatePayload, ReadRequest

all_output_format_args = [
    None,
    OutputFormat.PANDAS,
    "PANDAS",
    "pandas",
    OutputFormat.PYARROW,
    "PYARROW",
    "pyarrow",
    OutputFormat.POLARS,
    "POLARS",
    "polars",
]
no_str_output_format_args = [
    None,
    OutputFormat.PANDAS,
    OutputFormat.PYARROW,
    OutputFormat.POLARS,
]


def expected_output_type(arctic_output_format, library_output_format, output_format_override):
    expected_output_format = (
        output_format_override or library_output_format or arctic_output_format or OutputFormat.PANDAS
    )
    if expected_output_format.lower() == OutputFormat.PANDAS.lower():
        return pd.DataFrame
    if expected_output_format.lower() == OutputFormat.PYARROW.lower():
        return pa.Table
    if expected_output_format.lower() == OutputFormat.POLARS.lower():
        return pl.DataFrame
    raise ValueError("Unexpected format")


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
        received = lib.read(sym, output_format=OutputFormat.PYARROW).data
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
        received = lib.read(sym, output_format=OutputFormat.PYARROW).data
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
        received = lib.read(sym, output_format=OutputFormat.PYARROW).data
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
        received = lib.read(sym, output_format=OutputFormat.PYARROW).data
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


arrow_string_formats = list(ArrowOutputStringFormat)
arrow_string_formats_with_none = arrow_string_formats + [None]


@pytest.mark.parametrize("arctic_str_format", arrow_string_formats)
@pytest.mark.parametrize("library_str_format", arrow_string_formats_with_none)
@pytest.mark.parametrize("read_str_format_default", arrow_string_formats_with_none)
@pytest.mark.parametrize("read_str_format_per_column", arrow_string_formats_with_none)
def test_read_arctic_strings(
    lmdb_storage, lib_name, arctic_str_format, library_str_format, read_str_format_default, read_str_format_per_column
):
    ac = lmdb_storage.create_arctic(output_format=OutputFormat.PYARROW, arrow_string_format_default=arctic_str_format)
    lib = ac.create_library(lib_name, arrow_string_format_default=library_str_format)
    sym = "sym"
    df = pd.DataFrame({"col": ["some", "strings", "in", "this", "column"]})
    lib.write(sym, df)
    arrow_string_format_per_column = {}
    if read_str_format_per_column is not None:
        arrow_string_format_per_column["col"] = read_str_format_per_column
    result = lib.read(
        sym,
        arrow_string_format_default=read_str_format_default,
        arrow_string_format_per_column=arrow_string_format_per_column,
    ).data
    expected_str_format = (
        read_str_format_per_column
        or read_str_format_default
        or library_str_format
        or arctic_str_format
        or ArrowOutputStringFormat.LARGE_STRING
    )
    if expected_str_format == ArrowOutputStringFormat.CATEGORICAL:
        assert pa.types.is_dictionary(result.column(0).type)
    if expected_str_format == ArrowOutputStringFormat.LARGE_STRING:
        assert pa.types.is_large_string(result.column(0).type)
    if expected_str_format == ArrowOutputStringFormat.SMALL_STRING:
        assert pa.types.is_string(result.column(0).type)


@pytest.mark.parametrize("lazy", [True, False])
@pytest.mark.parametrize("batch_default", [ArrowOutputStringFormat.SMALL_STRING, None])
def test_read_batch_strings(lmdb_storage, lib_name, lazy, batch_default):
    ac = lmdb_storage.create_arctic(output_format=OutputFormat.PYARROW)
    lib = ac.create_library(lib_name)
    sym_1, sym_2 = "sym_1", "sym_2"
    df_1 = pd.DataFrame({"col_1": ["a", "a", "bb"], "col_2": ["x", "y", "z"]})
    df_2 = pd.DataFrame({"col_1": ["a", "aa", "aaa"], "col_2": ["a", "a", "a"]})
    lib.write_batch([WritePayload(sym_1, df_1), WritePayload(sym_2, df_2)])

    read_requests = [
        ReadRequest(symbol=sym_1, arrow_string_format_per_column={"col_1": ArrowOutputStringFormat.CATEGORICAL}),
        ReadRequest(
            symbol=sym_2,
            arrow_string_format_default=ArrowOutputStringFormat.LARGE_STRING,
        ),
    ]
    batch_result = lib.read_batch(
        read_requests,
        arrow_string_format_default=batch_default,
        arrow_string_format_per_column={"col_2": ArrowOutputStringFormat.CATEGORICAL},
        lazy=lazy,
    )
    if lazy:
        batch_result = batch_result.collect()
    table_1 = batch_result[0].data
    assert table_1.schema.field(0).type == pa.dictionary(pa.int32(), pa.large_string())  # per_column override
    assert table_1.schema.field(1).type == batch_default or pa.large_string()  # global default for all symbols
    assert_frame_equal_with_arrow(table_1, df_1)
    table_2 = batch_result[1].data
    assert table_2.schema.field(0).type == pa.large_string()  # per symbol default
    assert table_2.schema.field(1).type == pa.dictionary(pa.int32(), pa.large_string())  # global per_column
    assert_frame_equal_with_arrow(table_2, df_2)


@pytest.mark.parametrize("default", [None, ArrowOutputStringFormat.SMALL_STRING])
@pytest.mark.parametrize("per_column", [None, ArrowOutputStringFormat.CATEGORICAL])
def test_read_batch_and_join_strings(lmdb_storage, lib_name, default, per_column):
    ac = lmdb_storage.create_arctic(output_format=OutputFormat.PYARROW)
    lib = ac.create_library(lib_name, library_options=LibraryOptions(dynamic_schema=True))
    sym_1, sym_2 = "sym_1", "sym_2"
    df_1 = pd.DataFrame({"col_1": ["a", "a", "bb"], "col_2": ["x", "y", "z"]})
    df_2 = pd.DataFrame({"col_2": ["a", "aa", "aaa"], "col_3": ["a", "a", "a"]})
    lib.write_batch([WritePayload(sym_1, df_1), WritePayload(sym_2, df_2)])

    arrow_string_format_per_column = {"col_1": per_column, "col_3": per_column} if per_column is not None else None
    lazy_dfs = lib.read_batch(
        [sym_1, sym_2],
        arrow_string_format_default=default,
        arrow_string_format_per_column=arrow_string_format_per_column,
        lazy=True,
    )

    lazy_with_join = concat(lazy_dfs)
    result = lazy_with_join.collect().data
    assert result.schema.field(0).type == per_column or default or pa.large_string()
    assert result.schema.field(1).type == default or pa.large_string()
    assert result.schema.field(2).type == per_column or default or pa.large_string()
    expected_df = pd.concat([df_1, df_2]).reset_index(drop=True)
    assert_frame_equal_with_arrow(expected_df, result)
