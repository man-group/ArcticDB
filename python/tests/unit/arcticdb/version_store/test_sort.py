import pandas as pd
import numpy as np
import pytest
import arcticdb as adb
from arcticdb.util.test import assert_frame_equal, config_context
from arcticdb_ext.storage import KeyType
from arcticdb_ext.version_store import SortedValue

from arcticdb.util.test import random_strings_of_length

# Reuse all current testing with both the new and old API
@pytest.fixture(params=[True, False], autouse=True)
def setup_use_new_stage_api(request):
    with config_context("dev.stage_new_api_enabled", 1 if request.param else 0):
        yield


@pytest.mark.storage
def test_stage_finalize(arctic_library):
    symbol = "AAPL"
    sort_cols = ["timestamp", "col1"]

    df1 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
            "col1": np.arange(1, 51),
            "col2": [f"a{i:02d}" for i in range(1, 51)],
        }
    ).set_index("timestamp")

    df2 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
            "col1": np.arange(51, 101),
            "col2": [f"b{i:02d}" for i in range(1, 51)],
        }
    ).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, False, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, False, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.storage
def test_stage_finalize_dynamic(arctic_library_dynamic):
    arctic_library = arctic_library_dynamic
    symbol = "AAPL"
    sort_cols = ["timestamp", "col1"]

    df1 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
            "col1": np.arange(1, 51),
            "col2": [f"a{i:02d}" for i in range(1, 51)],
            "col3": np.arange(51, 101),
        }
    ).set_index("timestamp")

    df2 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
            "col1": np.arange(51, 101),
            "col2": [f"b{i:02d}" for i in range(1, 51)],
            "col3": np.arange(101, 151),
        }
    ).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, False, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, False, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.storage
def test_stage_finalize_strings(arctic_library):
    symbol = "AAPL"
    sort_cols = ["timestamp", "col1"]

    df1 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
            "col1": np.arange(1, 51),
            "col2": [f"a{i:02d}" for i in range(1, 51)],
            "col3": random_strings_of_length(50, 12),
        }
    ).set_index("timestamp")

    df2 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
            "col1": np.arange(51, 101),
            "col2": [f"b{i:02d}" for i in range(1, 51)],
            "col3": random_strings_of_length(50, 12),
        }
    ).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, False, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, False, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.storage
def test_stage_finalize_strings_dynamic(arctic_library_dynamic):
    arctic_library = arctic_library_dynamic
    symbol = "AAPL"
    sort_cols = ["timestamp", "col1"]

    df1 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
            "col1": np.arange(1, 51),
            "col2": [f"a{i:02d}" for i in range(1, 51)],
            "col3": random_strings_of_length(50, 12),
        }
    ).set_index("timestamp")

    df2 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
            "col1": np.arange(51, 101),
            "col2": [f"b{i:02d}" for i in range(1, 51)],
            "col4": [f"a{i:02d}" for i in range(101, 151)],
            "col5": random_strings_of_length(50, 12),
        }
    ).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, False, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, False, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.storage
def test_stage_finalize_sort_index(arctic_library):
    symbol = "AAPL"
    sort_cols = ["timestamp"]

    df1 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=50, freq="H"),
            "col1": np.arange(1, 51),
            "col2": [f"a{i:02d}" for i in range(1, 51)],
        }
    ).set_index("timestamp")

    df2 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-04", periods=50, freq="H"),
            "col1": np.arange(51, 101),
            "col2": [f"b{i:02d}" for i in range(1, 51)],
        }
    ).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, True, None)
    arctic_library.stage(symbol, df2_shuffled, False, True, None)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


def test_stage_with_sort_index_chunking(lmdb_version_store_tiny_segment):
    symbol = "AAPL"
    lib = lmdb_version_store_tiny_segment  # 2 rows per segment, 2 cols per segment

    df1 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=50, freq="H"),
            "col1": np.arange(1, 51),
            "col2": [f"a{i:02d}" for i in range(1, 51)],
            "col3": np.arange(1, 51),
        }
    ).set_index("timestamp")
    df1_shuffled = df1.sample(frac=1)

    lib.stage(symbol, df1_shuffled, validate_index=False, sort_on_index=True, sort_columns=None)

    lib_tool = lib.library_tool()
    data_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)
    # We don't apply column slicing when staging incompletes, do apply row slicing
    assert len(data_keys) == 25
    for k in data_keys:
        df = lib_tool.read_to_dataframe(k)
        assert df.shape == (2, 3)  # we should apply row slicing but not column slicing
        assert df.index.is_monotonic_increasing
        assert lib_tool.read_descriptor(k).sorted() == SortedValue.ASCENDING

    ref_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_REF, symbol)
    assert not ref_keys

    lib.compact_incomplete(symbol, append=False, convert_int_to_float=False)

    actual = lib.read(symbol).data
    assert_frame_equal(df1, actual)


def test_stage_with_sort_columns_not_ts(lmdb_version_store_v1):
    symbol = "AAPL"
    lib = lmdb_version_store_v1

    df1 = pd.DataFrame(
        {
            "idx": np.arange(1, 51),
            "col1": np.arange(1, 51),
            "col2": [f"a{i:02d}" for i in range(1, 51)],
            "col3": np.arange(1, 51),
        }
    ).set_index("idx")
    df1_shuffled = df1.sample(frac=1)

    lib.stage(symbol, df1_shuffled, validate_index=False, sort_on_index=False, sort_columns=["idx"])

    lib_tool = lib.library_tool()
    data_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)
    assert len(data_keys) == 1
    data_key = data_keys[0]
    assert lib_tool.read_descriptor(data_key).sorted() == SortedValue.UNKNOWN

    lib.compact_incomplete(symbol, append=False, convert_int_to_float=False)
    actual = lib.read(symbol).data
    assert_frame_equal(df1, actual)


@pytest.mark.storage
def test_stage_finalize_dynamic_with_chunking(arctic_client, lib_name):
    lib_opts = adb.LibraryOptions(dynamic_schema=True, rows_per_segment=2, columns_per_segment=2)
    lib = arctic_client.get_library(lib_name, create_if_missing=True, library_options=lib_opts)
    symbol = "AAPL"
    sort_cols = ["timestamp", "col1"]

    df1 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=7, freq="H"),
            "col1": np.arange(1, 8, dtype=np.uint8),
            "col2": [f"a{i:02d}" for i in range(1, 8)],
            "col3": np.arange(1, 8, dtype=np.int32),
        }
    ).set_index("timestamp")

    df2 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-04", periods=7, freq="H"),
            "col1": np.arange(8, 15, dtype=np.int32),
            "col2": [f"b{i:02d}" for i in range(8, 15)],
            "col3": np.arange(8, 15, dtype=np.uint16),
        }
    ).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    lib.stage(symbol, df1_shuffled, False, False, sort_cols)
    lib.stage(symbol, df2_shuffled, False, False, sort_cols)

    lib_tool = lib._dev_tools.library_tool()
    data_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)
    assert len(data_keys) == 8
    for k in data_keys:
        df = lib_tool.read_to_dataframe(k)
        assert df.index.is_monotonic_increasing

    lib.finalize_staged_data(symbol)
    data_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)
    assert not data_keys

    result = lib.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.storage
def test_stage_finalize_index_and_additional(arctic_library):
    symbol = "AAPL"
    sort_cols = ["col1"]

    df1 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
            "col1": np.arange(1, 51),
            "col2": [f"a{i:02d}" for i in range(1, 51)],
        }
    ).set_index("timestamp")

    df2 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
            "col1": np.arange(51, 101),
            "col2": [f"b{i:02d}" for i in range(1, 51)],
        }
    ).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, True, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, True, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(["timestamp", "col1"])
    pd.testing.assert_frame_equal(result, expected)
