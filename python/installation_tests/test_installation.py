"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from datetime import datetime, timedelta
import random
import time
import pandas as pd
import numpy as np
import pytest
import arcticdb

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.util.test import random_floats, random_strings_of_length
from arcticdb.version_store import VersionedItem as PythonVersionedItem
from arcticdb.toolbox.library_tool import KeyType
from arcticdb.version_store.library import ReadRequest, StagedDataFinalizeMethod, WritePayload
from arcticdb_ext.exceptions import SortingException 
from arcticdb_ext.version_store import AtomKey, RefKey
from packaging import version


from arcticdb.util.test import (
    assert_frame_equal,
    random_strings_of_length,
    random_floats,
)
from arcticdb.version_store.processing import QueryBuilder
from installation_tests.client_utils import delete_library


PRE_4_X_X = (
    False if "dev" in arcticdb.__version__ else version.parse(arcticdb.__version__) < version.Version("4.0.0")
)    
PRE_5_X_X = (
    False if "dev" in arcticdb.__version__ else version.parse(arcticdb.__version__) < version.Version("5.0.0")
)    
PRE_5_2_X = (
    False if "dev" in arcticdb.__version__ else version.parse(arcticdb.__version__) < version.Version("5.2.0")
)    

def generate_dataframe(columns, dt, num_days, num_rows_per_day):
    dataframes = []
    for _ in range(num_days):
        index = pd.Index([dt + timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in columns}
        new_df = pd.DataFrame(data=vals, index=index)
        dataframes.append(new_df)
        dt = dt + timedelta(days=1)
    return pd.concat(dataframes)


def test_write_batch_dedup(ac_library_factory):
    """Should be able to write different size of batch of data reusing deduplicated data from previous versions."""
    lib = ac_library_factory(LibraryOptions(rows_per_segment=10, dedup=True))
    assert lib._nvs._lib_cfg.lib_desc.version.write_options.segment_row_size == 10
    assert lib._nvs._lib_cfg.lib_desc.version.write_options.de_duplication == True
    num_days = 40
    num_symbols = 2
    num_versions = 4
    dt = datetime(2019, 4, 8, 0, 0, 0)
    column_length = 4
    num_columns = 5
    num_rows_per_day = 1
    read_requests = []
    list_dataframes = {}
    columns = random_strings_of_length(num_columns, num_columns, True)
    df = generate_dataframe(random.sample(columns, num_columns), dt, num_days, num_rows_per_day)
    for v in range(num_versions):
        write_requests = []
        for sym in range(num_symbols):
            write_requests.append(WritePayload("symbol_" + str(sym), df, metadata="great_metadata" + str(v)))
            read_requests.append("symbol_" + str(sym))
            list_dataframes[sym] = df
        write_batch_result = lib.write_batch(write_requests)
        assert all(type(w) == PythonVersionedItem for w in write_batch_result)

    read_batch_result = lib.read_batch(read_requests)
    for sym in range(num_symbols):
        original_dataframe = list_dataframes[sym]
        assert read_batch_result[sym].metadata == "great_metadata" + str(num_versions - 1)
        assert_frame_equal(read_batch_result[sym].data, original_dataframe)

    num_segments = int(
        (num_days * num_rows_per_day) / lib._nvs._lib_cfg.lib_desc.version.write_options.segment_row_size
    )
    for sym in range(num_symbols):
        data_key_version = lib._nvs.read_index("symbol_" + str(sym))["version_id"]
        for s in range(num_segments):
            assert data_key_version.iloc[s] == 0


def test_basic_write_read_update_and_append(ac_library):
    lib = ac_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("my_symbol", df)

    assert lib.list_symbols() == ["my_symbol"]
    assert_frame_equal(lib.read("my_symbol").data, df)
    assert_frame_equal(lib.read("my_symbol", columns=["col1"]).data, df[["col1"]])

    assert_frame_equal(lib.head("my_symbol", n=1).data, df.head(n=1))
    assert_frame_equal(lib.tail("my_symbol", n=1).data, df.tail(n=1).reset_index(drop=True))

    lib.append("my_symbol", pd.DataFrame({"col1": [4, 5, 6], "col2": [7, 8, 9]}))
    assert lib["my_symbol"].version == 1
    assert_frame_equal(
        lib.read("my_symbol").data, pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6], "col2": [4, 5, 6, 7, 8, 9]})
    )

    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    df.index = pd.date_range("2018-01-01", periods=3, freq="h")
    lib.write("timeseries", df, metadata={"hello": "world"})
    assert lib["timeseries"].version == 0

    df = pd.DataFrame({"col1": [4, 5, 6], "col2": [7, 8, 9]})
    df.index = pd.date_range("2018-01-01", periods=3, freq="h")
    lib.update("timeseries", df)
    assert lib["timeseries"].version == 1
    df.index.freq = None
    assert_frame_equal(lib.read("timeseries").data, df)

    lib.write("meta", df, metadata={"hello": "world"})
    assert lib["meta"].version == 0

    read_metadata = lib.read_metadata("meta")
    assert read_metadata.metadata == {"hello": "world"}
    assert read_metadata.data is None
    assert read_metadata.version == 0

    lib.write("meta", df, metadata={"goodbye": "cruel world"})
    read_metadata = lib.read_metadata("meta")
    assert read_metadata.version == 1

     
def test_list_versions_write_append_update(ac_library):
    lib = ac_library
    # Note: can only update timeseries dataframes
    index = pd.date_range(start="2000-01-01", freq="D", periods=3)
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}, index=index)
    lib.write("symbol", df)
    index_append = pd.date_range(start="2000-01-04", freq="D", periods=3)
    df_append = pd.DataFrame({"col1": [7, 8, 9], "col2": [10, 11, 12]}, index=index_append)
    lib.append("symbol", df_append)
    index_update = pd.DatetimeIndex(["2000-01-03", "2000-01-05"])
    df_update = pd.DataFrame({"col1": [13, 14], "col2": [15, 16]}, index=index_update)
    lib.update("symbol", df_update)
    assert_frame_equal(lib.read("symbol").data, pd.concat([df.iloc[:-1], df_update, df_append.iloc[[2]]]))
    assert len(lib.list_versions("symbol")) == 3


def test_read_batch_per_symbol_query_builder(ac_library):
    lib = ac_library

    # Given
    q_1 = QueryBuilder()
    q_1 = q_1[q_1["a"] < 5]
    q_2 = QueryBuilder()
    q_2 = q_2[q_2["a"] < 7]
    lib.write("s1", pd.DataFrame({"a": [3, 5, 7]}))
    lib.write("s2", pd.DataFrame({"a": [4, 6, 8]}))
    # When
    batch = lib.read_batch([ReadRequest("s1", query_builder=q_1), ReadRequest("s2", query_builder=q_2)])
    # Then
    assert_frame_equal(batch[0].data, pd.DataFrame({"a": [3]}))
    assert_frame_equal(batch[1].data, pd.DataFrame({"a": [4, 6]}))    


@pytest.mark.parametrize("finalize_method", (StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE))
@pytest.mark.parametrize("validate_index", (True, False, None))
@pytest.mark.storage
@pytest.mark.skipif(PRE_4_X_X, reason = "finalize_staged_data has 2 arguments only in older ver")
def test_parallel_writes_and_appends_index_validation(ac_library, finalize_method, validate_index):
    lib = ac_library
    sym = "test_parallel_writes_and_appends_index_validation"
    if finalize_method == StagedDataFinalizeMethod.APPEND:
        df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
        lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")])
    df_2 = pd.DataFrame({"col": [5, 6]}, index=[pd.Timestamp("2024-01-03T12"), pd.Timestamp("2024-01-05")])
    lib.write(sym, df_2, staged=True)
    lib.write(sym, df_1, staged=True)
    if validate_index is None:
        # Test default behaviour when arg isn't provided
        with pytest.raises(SortingException):
            lib.finalize_staged_data(sym, finalize_method)
    elif validate_index:
        with pytest.raises(SortingException):
            lib.finalize_staged_data(sym, finalize_method, validate_index=True)
    else:
        lib.finalize_staged_data(sym, finalize_method, validate_index=False)
        received = lib.read(sym).data
        expected = (
            pd.concat([df_0, df_1, df_2])
            if finalize_method == StagedDataFinalizeMethod.APPEND
            else pd.concat([df_1, df_2])
        )
        assert_frame_equal(received, expected)


def test_update_prune_previous_versions(ac_library):
    """Test that updating and pruning previous versions does indeed clear previous versions."""
    lib = ac_library
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": [400, 40]}, index=pd.date_range(start="1/1/2018", end="1/3/2018", freq="2D"))

    lib.update("symbol", update_df, prune_previous_versions=True)

    result = lib.read("symbol").data
    expected = pd.DataFrame({"column": [400, 40, 4]}, index=pd.to_datetime(["1/1/2018", "1/3/2018", "1/4/2018"]))
    assert_frame_equal(result, expected)
    symbols = lib.list_versions("symbol")
    assert len(symbols) == 1
    assert ("symbol", 1) in symbols


@pytest.mark.skipif(PRE_4_X_X, reason = "batch operations with snapshots not avail")
def test_read_batch_mixed_with_snapshots(ac_library):
    num_symbols = 10
    num_versions = 10

    def dataframe_for_offset(version_num, symbol_num):
        offset = (version_num * num_versions) + symbol_num
        return pd.DataFrame({"x": np.arange(offset, offset + 10)})

    def dataframe_and_symbol(version_num, symbol_num):
        symbol_name = "symbol_{}".format(symbol_num)
        dataframe = dataframe_for_offset(version_num, symbol_num)
        return symbol_name, dataframe

    lib = ac_library
    version_write_times = []

    for version in range(num_versions):
        version_write_times.append(pd.Timestamp.now())
        time.sleep(1)
        for sym in range(num_symbols):
            symbol, df = dataframe_and_symbol(version, sym)
            lib.write(symbol, df)

        lib.snapshot("snap_{}".format(version))

    read_requests = [
        ReadRequest("symbol_1", as_of=None),
        ReadRequest("symbol_1", as_of=4),
        ReadRequest("symbol_1", as_of="snap_7"),
        ReadRequest("symbol_2", as_of="snap_7"),
        ReadRequest("symbol_2", as_of=None),
        ReadRequest("symbol_2", as_of=4),
        ReadRequest("symbol_3", as_of="snap_7"),
        ReadRequest("symbol_3", as_of="snap_3"),
        ReadRequest("symbol_3", as_of="snap_1"),
        ReadRequest("symbol_3", as_of=None),
        ReadRequest("symbol_3", as_of=3),
    ]

    vits = lib.read_batch(read_requests)

    expected = dataframe_for_offset(9, 1)
    assert_frame_equal(vits[0].data, expected)
    expected = dataframe_for_offset(4, 1)
    assert_frame_equal(vits[1].data, expected)
    expected = dataframe_for_offset(7, 1)
    assert_frame_equal(vits[2].data, expected)

    expected = dataframe_for_offset(7, 2)
    assert_frame_equal(vits[3].data, expected)
    expected = dataframe_for_offset(9, 2)
    assert_frame_equal(vits[4].data, expected)
    expected = dataframe_for_offset(4, 2)
    assert_frame_equal(vits[5].data, expected)

    expected = dataframe_for_offset(7, 3)
    assert_frame_equal(vits[6].data, expected)
    expected = dataframe_for_offset(3, 3)
    assert_frame_equal(vits[7].data, expected)
    expected = dataframe_for_offset(1, 3)
    assert_frame_equal(vits[8].data, expected)
    expected = dataframe_for_offset(9, 3)
    assert_frame_equal(vits[9].data, expected)
    expected = dataframe_for_offset(3, 3)
    assert_frame_equal(vits[10].data, expected)

    # Trigger iteration of old-style snapshot keys
    library_tool = lib._nvs.library_tool()
    snap_key = RefKey("snap_8", KeyType.SNAPSHOT_REF)
    snap_segment = library_tool.read_to_segment(snap_key)
    old_style_snap_key = AtomKey("snap_8", 1, 2, 3, 4, 5, KeyType.SNAPSHOT)
    library_tool.write(old_style_snap_key, snap_segment)
    assert library_tool.count_keys(KeyType.SNAPSHOT_REF) == 10
    library_tool.remove(snap_key)
    assert library_tool.count_keys(KeyType.SNAPSHOT_REF) == 9
    assert library_tool.count_keys(KeyType.SNAPSHOT) == 1

    read_requests = [
        ReadRequest("symbol_7", as_of=None),
        ReadRequest("symbol_8", as_of=4),
        ReadRequest("symbol_2", as_of="snap_8"),
        ReadRequest("symbol_3", as_of="snap_7"),
        ReadRequest("symbol_1", as_of=4),
    ]

    vits = lib.read_batch(read_requests)
    expected = dataframe_for_offset(9, 7)
    assert_frame_equal(vits[0].data, expected)
    expected = dataframe_for_offset(4, 8)
    assert_frame_equal(vits[1].data, expected)
    expected = dataframe_for_offset(8, 2)
    assert_frame_equal(vits[2].data, expected)
    expected = dataframe_for_offset(7, 3)
    assert_frame_equal(vits[3].data, expected)
    expected = dataframe_for_offset(4, 1)
    assert_frame_equal(vits[4].data, expected)

    read_requests = [
        ReadRequest("symbol_6", as_of="snap_1"),
        ReadRequest("symbol_6", as_of="snap_3"),
        ReadRequest("symbol_6", as_of="snap_2"),
        ReadRequest("symbol_6", as_of="snap_7"),
        ReadRequest("symbol_6", as_of="snap_9"),
        ReadRequest("symbol_6", as_of="snap_4"),
    ]

    vits = lib.read_batch(read_requests)
    expected = dataframe_for_offset(1, 6)
    assert_frame_equal(vits[0].data, expected)
    expected = dataframe_for_offset(3, 6)
    assert_frame_equal(vits[1].data, expected)
    expected = dataframe_for_offset(2, 6)
    assert_frame_equal(vits[2].data, expected)
    expected = dataframe_for_offset(7, 6)
    assert_frame_equal(vits[3].data, expected)
    expected = dataframe_for_offset(9, 6)
    assert_frame_equal(vits[4].data, expected)
    expected = dataframe_for_offset(4, 6)
    assert_frame_equal(vits[5].data, expected)


@pytest.mark.skipif(PRE_5_X_X, reason = "Library has no stage() method before ver 5.x")
def test_stage_finalize_dynamic_with_chunking(ac_client, lib_name):
    lib_opts = LibraryOptions(dynamic_schema=True, rows_per_segment=2, columns_per_segment=2)
    lib = ac_client.get_library(lib_name, create_if_missing=True, library_options=lib_opts)
    try:
        symbol = "AAPL"
        sort_cols = ["timestamp", "col1"]

        df1 = pd.DataFrame(
            {
                "timestamp": pd.date_range("2023-01-01", periods=7, freq="h"),
                "col1": np.arange(1, 8, dtype=np.uint8),
                "col2": [f"a{i:02d}" for i in range(1, 8)],
                "col3": np.arange(1, 8, dtype=np.int32),
            }
        ).set_index("timestamp")

        df2 = pd.DataFrame(
            {
                "timestamp": pd.date_range("2023-01-04", periods=7, freq="h"),
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
        ## NOTE: Conditional on the version check.
        ##       Reasons for failure of older version is not investigated
        if PRE_5_2_X:
            assert len(data_keys) == 2
        else:
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
    finally:
        delete_library(ac_client, lib_name)

@pytest.mark.skipif(PRE_4_X_X, reason = "ModifiableEnterpriseLibraryOption not present before")
def test_modify_options_affect_persistent_lib_config(ac_client, lib_name):
    from arcticdb.options import ModifiableEnterpriseLibraryOption 
    ac = ac_client
    lib = ac.create_library(lib_name)

    try:
        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.REPLICATION, True)
        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.BACKGROUND_DELETION, True)

        new_client = Arctic(ac.get_uri())
        new_lib = new_client[lib_name]
        proto_options = new_lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.sync_passive.enabled
        assert proto_options.delayed_deletes
    finally:
        delete_library(ac_client, lib_name)

@pytest.mark.skipif(PRE_4_X_X, reason = "compact_symbol_list not present before")
def test_force_compact_symbol_list(ac_library):
    lib = ac_library
    lib_tool = lib._nvs.library_tool()
    # No symbol list keys
    assert lib.compact_symbol_list() == 0
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == 1
    assert not len(lib.list_symbols())
    lib_tool.remove(symbol_list_keys[0])

    num_syms = 10
    payloads = list()
    syms = list()
    df = pd.DataFrame({'A': [1], 'B': [2]})
    for sym in range(num_syms):
        name = f"symbol_{sym:03}"
        syms.append(name)
        payloads.append(WritePayload(name, df))
    lib.write_batch(payloads)
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == num_syms
    assert lib.compact_symbol_list() == num_syms
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == 1
    assert set(lib.list_symbols()) == set(syms)
    # Idempotent
    assert lib.compact_symbol_list() == 1
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == 1
    assert set(lib.list_symbols()) == set(syms)
    # Everything deleted
    for sym in syms:
        lib.delete(sym)
    # +1 for previous compacted key
    assert lib.compact_symbol_list() == num_syms + 1
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == 1
    assert not len(lib.list_symbols())

def sample_dataframe(start_date, *arr) -> pd.DataFrame:
    """
        Creates a dataframe based on arrays that are passed.
        Arrays will be used as columns data of the dataframe.
        The returned dataframe will be indexed with timestamp 
        starting from the given date
        Arrays must be numpy arrays of same size
    """
    date_range = pd.date_range(start=start_date, periods=len(arr[0]), freq='D') 
    columns = {}
    cnt = 0
    for ar in arr:
        columns[f"NUMBER{cnt}"] = ar    
        cnt = cnt + 1
    
    return pd.DataFrame(columns, index=date_range)

@pytest.mark.parametrize("mode" , [StagedDataFinalizeMethod.APPEND])
def test_finalize_staged_data_mode_append(ac_library, mode):
    lib = ac_library
    symbol = "symbol"
    df_initial = sample_dataframe('2020-1-1', [1,2,3], [4, 5, 6])
    df_staged = sample_dataframe('2020-1-4', [7, 8, 9], [10, 11, 12])
    lib.write(symbol, df_initial)
    lib.write(symbol, df_staged, staged=True)
    assert_frame_equal(lib.read(symbol).data, df_initial)

    lib.finalize_staged_data(symbol="symbol", mode=mode)
    expected = pd.concat([df_initial, df_staged])
    assert_frame_equal(lib.read(symbol).data, expected)
