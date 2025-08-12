import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb import LazyDataFrame, DataError, concat
from arcticdb.options import OutputFormat
from arcticdb.util.test import assert_frame_equal_with_arrow, sample_dataframe


all_output_format_args = [None, OutputFormat.PANDAS, "PANDAS", "pandas", OutputFormat.EXPERIMENTAL_ARROW, "EXPERIMENTAL_ARROW", "experimental_arrow"]
no_str_output_format_args = [None, OutputFormat.PANDAS, OutputFormat.EXPERIMENTAL_ARROW]


def expected_output_type(arctic_output_format, library_output_format, output_format_override):
    expected_output_format = output_format_override or library_output_format or arctic_output_format or OutputFormat.PANDAS
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
    row_range = (len(df)//4, len(df)*3//4)

    lazy_df = lib.read(sym, output_format=output_format_override, lazy=True)
    assert isinstance(lazy_df, LazyDataFrame)
    lazy_df = lazy_df.row_range(row_range)
    result = lazy_df.collect().data

    assert isinstance(result, expected_output_type(arctic_output_format, None, output_format_override))
    expected_df = df.iloc[row_range[0]:row_range[1], :].reset_index(drop=True)
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
