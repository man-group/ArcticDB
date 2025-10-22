import copy

import pytest
from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np

from arcticdb.dependencies import pyarrow as pa
from arcticdb.options import OutputFormat
from arcticdb.util.arrow import stringify_dictionary_encoded_columns
from arcticdb import QueryBuilder
from tests.util.naughty_strings import read_big_list_of_naughty_strings


def create_dataframe(strings):
    start_date = "2023-01-01"
    data = {"strings": strings, "ints": np.random.randint(1, 100, size=len(strings))}
    date_range = pd.date_range(start=start_date, periods=len(strings), freq="D")
    date_range.freq = None
    df = pd.DataFrame(data, index=date_range)
    df.index.name = "ts"
    return df


def test_write_blns(lmdb_version_store):
    lib = lmdb_version_store
    strings = read_big_list_of_naughty_strings()
    symbol = "blns_write"
    # Pandas
    df = create_dataframe(strings)
    lib.write(symbol, df)
    vit = lib.read(symbol)
    assert_frame_equal(df, vit.data)
    # Arrow
    lib._set_allow_arrow_input()
    table = pa.Table.from_pandas(df)
    lib.write(symbol, table, index_column="ts")
    received = stringify_dictionary_encoded_columns(
        lib.read(symbol, output_format=OutputFormat.EXPERIMENTAL_ARROW).data, pa.string()
    )
    assert table.equals(received)


def test_append_blns(lmdb_version_store):
    lib = lmdb_version_store
    strings = read_big_list_of_naughty_strings()
    symbol = "blns_append"
    # Pandas
    df = create_dataframe(strings)
    half_index = len(df) // 2
    df_first_half = df.iloc[:half_index]
    df_second_half = df.iloc[half_index:]
    lib.write(symbol, df_first_half)
    lib.append(symbol, df_second_half)
    vit = lib.read(symbol)
    assert_frame_equal(df, vit.data)
    # Arrow
    lib._set_allow_arrow_input()
    table_first_half = pa.Table.from_pandas(df_first_half)
    table_second_half = pa.Table.from_pandas(df_second_half)
    lib.write(symbol, table_first_half, index_column="ts")
    lib.append(symbol, table_second_half, index_column="ts")
    received = stringify_dictionary_encoded_columns(
        lib.read(symbol, output_format=OutputFormat.EXPERIMENTAL_ARROW).data, pa.string()
    )
    expected = pa.Table.from_pandas(df)
    assert expected.equals(received)


def test_update_blns(lmdb_version_store):
    lib = lmdb_version_store
    strings = read_big_list_of_naughty_strings()
    symbol = "blns_update"
    # Pandas
    df = create_dataframe(strings)
    total_length = len(df)
    half_length = total_length // 2
    start_index = (total_length - half_length) // 2
    end_index = start_index + half_length
    df_removed_middle = df.drop(df.index[start_index:end_index])
    df_middle_half = df.iloc[start_index:end_index]
    lib.write(symbol, df_removed_middle)
    lib.update(symbol, df_middle_half)
    vit = lib.read(symbol)
    assert_frame_equal(df, vit.data)
    # Arrow
    lib._set_allow_arrow_input()
    table_removed_middle = pa.Table.from_pandas(df_removed_middle)
    table_middle_half = pa.Table.from_pandas(df_middle_half)
    lib.write(symbol, table_removed_middle, index_column="ts")
    lib.update(symbol, table_middle_half, index_column="ts")
    received = stringify_dictionary_encoded_columns(
        lib.read(symbol, output_format=OutputFormat.EXPERIMENTAL_ARROW).data, pa.string()
    )
    expected = pa.Table.from_pandas(df)
    assert expected.equals(received)


def test_batch_read_blns(lmdb_version_store):
    lib = lmdb_version_store
    strings = read_big_list_of_naughty_strings()
    num_symbols = 10
    symbols = [f"blns_batch_read_{idx}" for idx in range(num_symbols)]
    q = QueryBuilder()
    q = q[q["ints"] > 50]
    qbs = (num_symbols // 2) * [None, copy.deepcopy(q)]
    # Pandas
    dfs = [create_dataframe(strings) for _ in range(num_symbols)]
    lib.batch_write(symbols, dfs)
    res = lib.batch_read(symbols, query_builder=qbs)
    for idx, sym in enumerate(symbols):
        expected = dfs[idx]
        if idx % 2 == 1:
            expected = expected[expected["ints"] > 50]
        assert_frame_equal(expected, res[sym].data)
    # Arrow
    lib._set_allow_arrow_input()
    tables = [pa.Table.from_pandas(df) for df in dfs]
    lib.batch_write(symbols, tables, index_column_vector=["ts"] * num_symbols)
    res = lib.batch_read(symbols, query_builder=qbs, output_format=OutputFormat.EXPERIMENTAL_ARROW)
    expr = pa.compute.field("ints") > 50
    for idx, sym in enumerate(symbols):
        expected = tables[idx]
        if idx % 2 == 1:
            expected = expected.filter(expr)
        assert expected.equals(stringify_dictionary_encoded_columns(res[sym].data, pa.string()))


def assert_dicts_of_dfs_equal(dict1, dict2):
    assert dict1.keys() == dict2.keys(), "Dictionary keys do not match"

    for key in dict1:
        pd.testing.assert_frame_equal(dict1[key], dict2[key], obj=f"DataFrame at key '{key}'")


def test_recursive_normalizers_blns(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    strings = read_big_list_of_naughty_strings()
    symbol = "blnd_recursive"
    keys = ["a", "b", "c", "d"]
    # Pandas
    df = create_dataframe(strings)
    dict_data = {s: df for s in keys}
    lib.write(symbol, dict_data, recursive_normalizers=True)
    vit = lib.read(symbol)
    assert_dicts_of_dfs_equal(dict_data, vit.data)
    # Arrow
    lib._set_allow_arrow_input()
    table = pa.Table.from_pandas(df)
    dict_data = {s: table for s in keys}
    lib.write(symbol, dict_data, recursive_normalizers=True)
    received = lib.read(symbol, output_format=OutputFormat.EXPERIMENTAL_ARROW).data
    for key in keys:
        assert key in received.keys()
        assert table.equals(stringify_dictionary_encoded_columns(received[key], pa.string()))


@pytest.mark.skip(reason="These do not roundtrip properly. Monday: 9256783357")
def test_recursive_normalizers_blns_in_keys(lmdb_version_store):
    lib = lmdb_version_store
    strings = read_big_list_of_naughty_strings()
    symbol = "blnd_recursive_in_keys"
    df = pd.DataFrame({"a": [1, 2, 3]})

    for s in strings:
        dict = {s: df}
        try:
            lib.write(symbol, dict, recursive_normalizers=True)
        except:
            # We just want to check that we can read anything we can write, so just skip anything we can't write
            continue
        vit = lib.read(symbol)
        assert s in vit.data
        pd.testing.assert_frame_equal(dict[s], vit.data[s])
