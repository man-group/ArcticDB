import pandas as pd
import numpy as np
import arcticdb as adb
import random
import string

from arcticdb_ext.storage import KeyType

from python.arcticdb.util.test import assert_frame_equal


def test_stage_finalize(arctic_library):
    symbol = "AAPL"
    sort_cols = ["timestamp", "col1"]

    df1 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
        "col1": np.arange(1, 51),
        "col2": [f"a{i:02d}" for i in range(1, 51)]
    }).set_index("timestamp")

    df2 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
        "col1": np.arange(51, 101),
        "col2": [f"b{i:02d}" for i in range(1, 51)]
    }).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, False, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, False, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


def create_lib_dynamic(ac, lib_name):
    lib_opts = adb.LibraryOptions(dynamic_schema=True)
    return ac.get_library(lib_name, create_if_missing=True, library_options=lib_opts)


def test_stage_finalize_dynamic(arctic_client, lib_name):
    arctic_library = create_lib_dynamic(arctic_client, lib_name)
    symbol = "AAPL"
    sort_cols = ["timestamp", "col1"]

    df1 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
        "col1": np.arange(1, 51),
        "col2": [f"a{i:02d}" for i in range(1, 51)],
        "col3": np.arange(51, 101)
    }).set_index("timestamp")

    df2 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
        "col1": np.arange(51, 101),
        "col2": [f"b{i:02d}" for i in range(1, 51)],
        "col3": np.arange(101, 151)
    }).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, False, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, False, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


def random_strings(count, max_length):
    result = []
    for _ in range(count):
        length = random.randrange(max_length) + 2
        result.append(
            "".join(random.choice(string.ascii_letters) for _ in range(length))
        )
    return result


def test_stage_finalize_strings(arctic_library):
    symbol = "AAPL"
    sort_cols = ["timestamp", "col1"]

    df1 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
        "col1": np.arange(1, 51),
        "col2": [f"a{i:02d}" for i in range(1, 51)],
        "col3": random_strings(50, 12)
    }).set_index("timestamp")

    df2 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
        "col1": np.arange(51, 101),
        "col2": [f"b{i:02d}" for i in range(1, 51)],
        "col3": random_strings(50, 12)
    }).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, False, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, False, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


def test_stage_finalize_strings_dynamic(arctic_client, lib_name):
    arctic_library = create_lib_dynamic(arctic_client, lib_name)
    symbol = "AAPL"
    sort_cols = ["timestamp", "col1"]

    df1 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
        "col1": np.arange(1, 51),
        "col2": [f"a{i:02d}" for i in range(1, 51)],
        "col3": random_strings(50, 12)
    }).set_index("timestamp")

    df2 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
        "col1": np.arange(51, 101),
        "col2": [f"b{i:02d}" for i in range(1, 51)],
        "col4": [f"a{i:02d}" for i in range(101, 151)],
        "col5": random_strings(50, 12)
    }).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, False, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, False, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(sort_cols)
    pd.testing.assert_frame_equal(result, expected)


def test_stage_finalize_sort_index(arctic_library):
    symbol = "AAPL"
    sort_cols = ["timestamp"]

    df1 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-01", periods=50, freq="H"),
        "col1": np.arange(1, 51),
        "col2": [f"a{i:02d}" for i in range(1, 51)]
    }).set_index("timestamp")

    df2 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-04", periods=50, freq="H"),
        "col1": np.arange(51, 101),
        "col2": [f"b{i:02d}" for i in range(1, 51)]
    }).set_index("timestamp")

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
    lib = lmdb_version_store_tiny_segment

    df1 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-01", periods=50, freq="H"),
        "col1": np.arange(1, 51),
        "col2": [f"a{i:02d}" for i in range(1, 51)],
        "col3": np.arange(1, 51)
    }).set_index("timestamp")
    df1_shuffled = df1.sample(frac=1)

    lib.stage(symbol, df1_shuffled, validate_index=False, sort_on_index=True, sort_columns=None)

    lib_tool = lib.library_tool()
    data_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)
    # We don't apply column slicing when staging incompletes, do apply row slicing
    assert len(data_keys) == 25
    for k in data_keys:
        df = lib_tool.read_to_dataframe(k)
        assert df.index.is_monotonic_increasing

    ref_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_REF, symbol)
    assert not ref_keys

    lib.compact_incomplete(symbol, append=False, convert_int_to_float=False)

    actual = lib.read(symbol).data
    assert assert_frame_equal(df1, actual)


def test_stage_finalize_index_and_additional(arctic_library):
    symbol = "AAPL"
    sort_cols = ["col1"]

    df1 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
        "col1": np.arange(1, 51),
        "col2": [f"a{i:02d}" for i in range(1, 51)]
    }).set_index("timestamp")

    df2 = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-04", periods=25, freq="H").repeat(2),
        "col1": np.arange(51, 101),
        "col2": [f"b{i:02d}" for i in range(1, 51)]
    }).set_index("timestamp")

    df1_shuffled = df1.sample(frac=1)
    df2_shuffled = df2.sample(frac=1)

    arctic_library.stage(symbol, df1_shuffled, False, True, sort_cols)
    arctic_library.stage(symbol, df2_shuffled, False, True, sort_cols)
    arctic_library.finalize_staged_data(symbol)
    result = arctic_library.read(symbol).data

    expected = pd.concat([df1, df2]).sort_values(["timestamp", "col1"])
    pd.testing.assert_frame_equal(result, expected)

