"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random
import re
import string
import sys
from typing import List, Union
import pytest
import pandas as pd
import numpy as np

from arcticdb.util.arctic_simulator import ArcticSymbolSimulator
from arcticdb.util.test import (
    assert_series_equal_pandas_1,
    assert_frame_equal_rebuild_index_first,
    assert_frame_equal,
    random_string,
)
from arcticdb.util.utils import DFGenerator, generate_random_series, set_seed, supported_types_list
from arcticdb.version_store._store import NativeVersionStore, VersionedItem
from datetime import timedelta, timezone

from arcticdb.exceptions import ArcticNativeException, SortingException, MissingKeysInStageResultsError
from arcticdb_ext.storage import KeyType
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.version_store import StreamDescriptorMismatch, NoSuchVersionException

from arcticdb_ext.exceptions import (
    UnsortedDataException,
    InternalException,
    NormalizationException,
    UserInputException,
    MissingDataException,
    SchemaException,
)

from tests.conftest import Marks
from tests.util.mark import LINUX, SLOW_TESTS_MARK, WINDOWS


def add_index(df: pd.DataFrame, start_time: pd.Timestamp):
    df.index = pd.date_range(start_time, periods=df.shape[0], freq="s")


def make_df_appendable(original_df: pd.DataFrame, append_df: pd.DataFrame):
    """Creates such index for `append_df` so that it can be appended to `original_df`"""
    last_time = original_df.index[-1]
    add_index(append_df, last_time + timedelta(seconds=1))


def create_all_arcticdb_types_df(length: int, column_prefix: str = ""):
    """Creates a dataframe with columns of all supported arcticdb data types."""
    arr = []
    for dtype in supported_types_list:
        name = f"{column_prefix}_{np.dtype(dtype).name}"
        arr.append(generate_random_series(dtype, length, name, seed=None))
    return pd.concat(arr, axis=1)


def wrap_df_add_new_columns(df: pd.DataFrame, prefix: str = ""):
    """Adds columns at the beginning and at the end of dataframe
    Columns added are of all supported arcticdb types and have
    specified prefix
    """
    length = df.shape[0]
    df1 = create_all_arcticdb_types_df(length, column_prefix=f"{prefix}_pre_")
    df1.index = df.index
    df2 = create_all_arcticdb_types_df(length, column_prefix=f"{prefix}_post_")
    df2.index = df.index
    df = pd.concat([df1, df, df2], axis=1)
    return df


def get_metadata():
    """Returns weird and complex metadata"""
    metadata = {
        "experiment": {"id": 42, "params": {"learning_rate": 0.01, "batch_size": 32}},
        "source": "integration_test",
        "tags": ["v1.2", "regression"],
    }
    return [metadata, {}, [metadata, {}, [metadata, {}], 1], "", None]


def assert_equals(expected: Union[pd.DataFrame, pd.Series], actual: Union[pd.DataFrame, pd.Series]):
    if isinstance(expected.index, pd.RangeIndex) or isinstance(actual.index, pd.RangeIndex):
        expected = expected.reset_index(drop=True)
        actual = actual.reset_index(drop=True)
    if isinstance(expected, pd.DataFrame):
        assert_frame_equal(expected, actual)
    else:
        assert_series_equal_pandas_1(expected, actual)


@SLOW_TESTS_MARK
@pytest.mark.parametrize("dtype", supported_types_list)
@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("append_type", ["append", "stage"])
def test_write_append_update_read_scenario_with_different_series_combinations(
    version_store_factory, dtype, dynamic_schema, append_type
):
    """This test covers series with timestamp index of all supported arcticdb types.
    Write, append and read combinations of different boundary length sizes of series

    Verifies:
     - append/update operations over symbol containing timestamped series
     - append/update operations with different types of series - empty, one element, many elements
     - tests repeated over each supported arcticdb type - ints, floats, str, bool, datetime
     - tests work as expected over static and dynamic schema with small segment row size
    """
    if LINUX and (sys.version_info[:2] == (3, 8)) and dtype == np.float64:
        """https://github.com/man-group/ArcticDB/actions/runs/16363364782/job/46235614310?pr=2470"""
        pytest.skip("Test fails due to issue (9589648728), Skipping")
    segment_row_size = 3
    lib: NativeVersionStore = version_store_factory(dynamic_schema=dynamic_schema, segment_row_size=segment_row_size)
    set_seed(3484356)
    max_length = segment_row_size * 29
    symbol = f"symbol-{re.sub(r'[^A-Za-z0-9]', '_', str(dtype))}"
    name = "some_name!"
    timestamp = pd.Timestamp(4839275892348)
    series_length = [1, 0, 2, max_length]
    meta = get_metadata()

    for length in series_length:
        total_length = 0
        series = generate_random_series(dtype, length, name, start_time=timestamp, seed=None)
        total_length += length
        lib.write(symbol, series)
        result_series = series
        assert_series_equal_pandas_1(series, lib.read(symbol).data, check_index_type=(len(series) > 0))

        for append_series_length in series_length:
            append_series = generate_random_series(
                dtype, append_series_length, name, start_time=timestamp + timedelta(seconds=total_length), seed=None
            )
            if append_type == "append":
                lib.append(symbol, append_series, metadata=meta)
            else:
                meta = None  # Metadata is not added to version with stage method
                lib.stage(symbol, append_series, validate_index=False, sort_on_index=False)
                lib.compact_incomplete(symbol, append=True, convert_int_to_float=False)
            result_series = pd.concat([result_series, append_series])
            ver = lib.read(symbol)
            assert_series_equal_pandas_1(result_series, ver.data, check_index_type=(len(result_series) > 0))
            assert meta == ver.metadata
            # Note update is of same length but starts in previous period

            update_series = generate_random_series(
                dtype, append_series_length, name, start_time=timestamp + timedelta(seconds=total_length - 1), seed=None
            )
            total_length += append_series_length
            lib.update(symbol, update_series, metadata=meta)
            result_series = ArcticSymbolSimulator.simulate_arctic_update(
                result_series, update_series, dynamic_schema=False
            )
            ver = lib.read(symbol)
            assert_series_equal_pandas_1(result_series, ver.data, check_index_type=(len(result_series) > 0))
            assert meta == ver.metadata


@Marks.storage.mark
def test_append_update_dynamic_schema_add_columns_all_types(version_store_and_real_s3_basic_store_factory):
    """
    The test does series of append operations with new columns of all supported column types.
    The resulting symbol will have additional columns each time added, with predefined default
    values for different data types

    Verifies:
     - continuous appends/updates with new columns works as expected across dynamic schema
     - updates/appends with new columns and combinations incomplete, metadata and prune previous
       always deliver desired result
    """

    set_seed(32432)
    counter = 1
    lib: NativeVersionStore = version_store_and_real_s3_basic_store_factory(
        dynamic_schema=True, dynamic_strings=True, segment_row_size=3
    )
    symbol = "232_43213dfmkd_!"
    col_name = "MIDDLE"
    start_time = pd.Timestamp(32513454)
    asym = ArcticSymbolSimulator(keep_versions=True)

    def get_df() -> pd.DataFrame:
        """Creates new dataframe with one constant one column and one row where the value is
        auto incremented on each new dataframe created"""
        nonlocal counter
        df = pd.DataFrame({col_name: [counter]})
        counter += 1
        return df

    initial_df = get_df()
    add_index(initial_df, start_time)
    asym.write(initial_df)
    lib.write(symbol, initial_df)
    read_data = lib.read(symbol).data

    # Will do append operation times to confirm growth is not a problem
    for i in range(10):
        num_versions_before = len(lib.list_versions(symbol))
        middle_df = get_df()
        add_index(middle_df, start_time)
        append_df = wrap_df_add_new_columns(middle_df, i)
        make_df_appendable(read_data, append_df)  # timestamps of append_df to be after the initial

        # Do append with different mixes of parameters
        if i % 2 == 0:
            meta = get_metadata()
            lib.append(symbol, append_df, prune_previous_version=True, validate_index=True, metadata=meta)
            assert num_versions_before, lib.list_versions(symbol)  # previous version is pruned
        else:
            meta = None
            # NOTE: metadata is not stored then incomplete=True
            lib.append(
                symbol, append_df, incomplete=True, prune_previous_version=False, validate_index=True, metadata=meta
            )
            lib.compact_incomplete(symbol, True, False)
            assert num_versions_before, lib.list_versions(symbol) + 1  # previous version is not pruned

        asym.append(append_df)

        # Verify  metadata and dynamically added columns
        ver = lib.read(symbol)
        read_data: pd.DataFrame = ver.data
        assert meta == ver.metadata
        asym.assert_equal_to(ver.data)

    # Will only update last row with 1 row dataframe
    update_timestamp = read_data.index[-1]

    # repeat update operation several times, to confirm growth is not a problem
    for i in range(5):
        num_versions_before = len(lib.list_versions(symbol))
        middle_df = get_df()
        add_index(middle_df, update_timestamp)
        update_df = wrap_df_add_new_columns(middle_df, f"upd_{i}")

        if i % 2 == 0:
            meta = get_metadata()
            lib.update(symbol, update_df, prune_previous_version=False, metadata=meta)
            assert num_versions_before, lib.list_versions(symbol) + 1  # previous version is not pruned
        else:
            meta = "just a message"
            lib.update(symbol, update_df, prune_previous_version=True, metadata=meta)
            assert num_versions_before, lib.list_versions(symbol)  # previous version is pruned

        asym.update(update_df)

        # Verify  metadata and dynamically added columns
        ver = lib.read(symbol)
        read_data: pd.DataFrame = ver.data
        assert meta == ver.metadata
        asym.assert_equal_to(ver.data)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@Marks.storage.mark
def test_append_scenario_with_errors_and_success(version_store_and_real_s3_basic_store_factory, dynamic_schema):
    """
    Test error messages and exception types for various failure scenarios mixed with

    Verifies:
    - Appropriate exception types are raised
    - Edge cases are handled gracefully
    - Exceptions are same across all storage types
    """
    lib: NativeVersionStore = version_store_and_real_s3_basic_store_factory(
        dynamic_schema=dynamic_schema, dynamic_strings=True, segment_row_size=1
    )
    symbol = "test_append_errors"

    df = pd.DataFrame({"value": [1, 2, 3]}, index=pd.date_range("2023-01-01", periods=3, freq="s"))
    df_2 = pd.DataFrame({"value": [3, 4, 5]}, index=pd.date_range("2023-01-02", periods=3, freq="s"))
    df_not_sorted = pd.DataFrame(
        {"value": [11, 24, 1]},
        index=[pd.Timestamp("2024-01-04"), pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-05")],
    )
    df_different_schema = pd.DataFrame(
        {"value": [3.1, 44, 5.324]}, index=pd.date_range("2023-01-03", periods=3, freq="s")
    )
    df_empty = df.iloc[0:0]
    df_same_index = pd.DataFrame({"value": [10, 982]}, index=pd.date_range("2023-01-01", periods=2, freq="s"))
    pickled_data = [34243, 3253, 53425]
    pickled_data_2 = get_metadata()
    df_different_index = pd.DataFrame({"value": [4, 5, 6]}, index=["a", "b", "c"])  # String index instead of datetime
    df_no_index = pd.DataFrame({"value": [4, 5, 6]})

    # Test append to non-existent symbol
    for sym in [symbol, None, ""]:
        with pytest.raises(TypeError):
            lib.append(sym)
    assert len(lib.list_symbols()) == 0

    # Empty dataframe will create symbol with one version
    result = lib.append(symbol, df_empty)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 1
    assert 0 == result.version
    assert_frame_equal(df_empty, lib.read(symbol).data)

    # Create symbol for further tests
    lib.write(symbol, df)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 2
    assert_frame_equal(df, lib.read(symbol).data)

    # Test append with invalid data type
    with pytest.raises(ArcticNativeException):
        lib.append(symbol, "invalid_data_type")
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 2

    # Test append with mismatched index type
    for frame in [df_different_index, df_no_index]:
        with pytest.raises((NormalizationException)):
            lib.append(symbol, frame)
    assert lib.list_symbols() == [symbol]
    assert len(lib.list_versions()) == 2

    before_append = pd.Timestamp.now(tz=timezone.utc).value
    result = lib.append(symbol, df_2)
    after_append = pd.Timestamp.now(tz=timezone.utc).value
    # Verify VersionedItem structure
    assert isinstance(result, VersionedItem)
    assert result.symbol == symbol
    assert result.version == 2
    assert result.metadata == None
    assert result.data is None
    assert result.library == lib._library.library_path
    assert result.host == lib.env
    # Verify timestamp is reasonable (within the append operation timeframe)
    assert before_append <= result.timestamp <= after_append
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 3
    assert lib.list_symbols_with_incomplete_data() == []
    assert_frame_equal(pd.concat([df, df_2], sort=True), lib.read(symbol).data)

    # Test append overlapping indexes
    for frame in [df_same_index]:
        with pytest.raises((InternalException)):
            lib.append(symbol, frame)
    # Append can happen only as incomplete
    lib.append(symbol, frame, validate_index=False, incomplete=True)
    assert lib.list_symbols_with_incomplete_data() == [symbol]
    assert len(lib.list_versions()) == 3

    # Cannot append pickle data to dataframe
    for pick in [pickled_data, pickled_data_2]:
        with pytest.raises(NormalizationException):
            lib.append(symbol, pick)

    # Append with different schema works on dynamic schema only
    if dynamic_schema:
        lib.append(symbol, df_different_schema)
        assert len(lib.list_versions()) == 4
        assert_frame_equal(pd.concat([df, df_2, df_different_schema], sort=True), lib.read(symbol).data)
    else:
        # Should raise StreamDescriptorMismatch
        with pytest.raises(StreamDescriptorMismatch):
            lib.append(symbol, df_different_schema)

    # Append empty dataframe to symbol with content does not increase version
    # but as we saw previously it will create symbol
    result = lib.append(symbol, df_empty)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 4 if dynamic_schema else 3

    # Validate that validate_index works as expected
    with pytest.raises(SortingException):
        lib.append(symbol, df_not_sorted, validate_index=True)
    with pytest.raises(SortingException):
        lib.append(symbol, df_not_sorted, validate_index=True, incomplete=True)
    result2 = lib.append(symbol, df_not_sorted, validate_index=False)
    assert result2.version == result.version + 1


def test_update_date_range_exhaustive(lmdb_version_store):
    """Test update with open-ended and closed date ranges,
    and verifications over the result dataframe what remains"""

    lib = lmdb_version_store
    symbol = "test_date_range_open"
    start_date_original_df = "2023-01-01"
    start_date_update_df = "2023-01-05"

    update_data = pd.DataFrame({"value": [999]}, index=pd.date_range(start_date_update_df, periods=1, freq="D"))

    def run_test(lib, initial_data, start, end, update_expected_at_index, length_of_result_df, scenario):
        nonlocal update_data
        lib.write(symbol, initial_data)  # always reset symbol

        lib.update(symbol, update_data, date_range=(start, end))

        result = lib.read(symbol).data

        assert result.iloc[update_expected_at_index]["value"] == 999, f"Failed for {scenario} Scenario"
        assert len(result) == length_of_result_df
        return result

    initial_data = pd.DataFrame({"value": range(10)}, index=pd.date_range(start_date_original_df, periods=10, freq="D"))
    lib.write(symbol, initial_data)

    # Test an open end scenario - where the start date is the start date of the update
    # all data from original dataframe from its start date until  the update start date is removed
    # and what is left is from update start date until end fate of original dataframe
    run_test(
        lib,
        initial_data,
        start=pd.Timestamp(start_date_update_df),
        end=None,
        update_expected_at_index=4,
        length_of_result_df=5,
        scenario="open end",
    )

    # Test an open end scenario, where data is way before initial timestamp
    # of original dataframe - in this case only the update will be present in the symbol
    # and no parts of the dataframe that was updated
    result_df = run_test(
        lib,
        initial_data,
        start=update_data.index[0] - timedelta(days=300),
        end=None,
        update_expected_at_index=0,
        length_of_result_df=1,
        scenario="open end - way before initial dataframe",
    )
    assert_frame_equal(update_data.head(1), result_df)

    # Test an open start scenario where end date overlaps with the start date
    # of the update. In that case all original start until the update start will be preserved
    # in the result
    run_test(
        lib,
        initial_data,
        start=None,
        end=pd.Timestamp(start_date_update_df),
        update_expected_at_index=0,
        length_of_result_df=6,
        scenario="open start",
    )

    # Both start and end are open - in this case only the update will be the result
    result_df = run_test(
        lib,
        initial_data,
        start=None,
        end=None,
        update_expected_at_index=0,
        length_of_result_df=update_data.shape[0],
        scenario="both open",
    )
    assert_frame_equal(update_data, result_df)


def split_dataframe_into_random_chunks(df: pd.DataFrame, min_size: int = 1, max_size: int = 30) -> List[pd.DataFrame]:
    chunks = []
    i = 0
    while i < len(df):
        chunk_size = np.random.randint(min_size, max_size + 1)
        chunk = df.iloc[i : i + chunk_size]
        if not chunk.empty:
            chunks.append(chunk)
        i += chunk_size
    return chunks


@pytest.mark.parametrize("num_columns", [1, 50])
@Marks.storage.mark
# Problem is on Linux and Python 3.8 with pandas 1.5.3
@pytest.mark.xfail(
    LINUX and (sys.version_info[:2] == (3, 8)),
    reason="update_batch return unexpected exception (9589648728)",
    strict=False,
)
def test_stage_any_size_dataframes_timestamp_indexed(version_store_and_real_s3_basic_store_factory, num_columns):
    """
    Tests  if different size chunks of dataframe can be successfully staged
    """
    lib: NativeVersionStore = version_store_and_real_s3_basic_store_factory(
        dynamic_schema=False, segment_row_size=5, column_group_size=3
    )
    set_seed(321546556)
    symbol = "experimental 342143"
    num_cols = num_columns
    num_rows = 200
    start_time = pd.Timestamp(7364876)
    df = DFGenerator.generate_normal_dataframe(num_rows=num_rows, num_cols=num_cols, start_time=start_time, seed=None)
    df_0col = df[0:0]
    df_1col = DFGenerator.generate_normal_dataframe(
        num_rows=num_rows, num_cols=num_cols, start_time=df.index[-1] + timedelta(seconds=123), seed=None
    )
    chunks = split_dataframe_into_random_chunks(df, max_size=20)
    chunks.append(df_0col)
    chunks.append(df_1col)
    np.random.shuffle(chunks)
    for chnk in chunks:
        lib.stage(symbol, chnk, validate_index=True)
    lib.compact_incomplete(symbol, append=False, prune_previous_version=True, convert_int_to_float=False)
    expected_data = pd.concat(chunks).sort_index()
    assert_frame_equal_rebuild_index_first(expected_data, lib.read(symbol).data)


# Problem is on Linux and Python 3.8 with pandas 1.5.3
@pytest.mark.xfail(
    LINUX and (sys.version_info[:2] == (3, 8)),
    reason="update_batch return unexpected exception (9589648728)",
    strict=False,
)
def test_stage_error(version_store_and_real_s3_basic_store_factory):
    """
    Isolated test for stage() - compact_cincomplete() problem on Linux and Python 3.8 with pandas 1.5.3
    """
    lib: NativeVersionStore = version_store_and_real_s3_basic_store_factory(
        dynamic_schema=False, segment_row_size=5, column_group_size=3
    )
    symbol = "experimental 342143"
    data = {"col_5": [-2.356538e38, 2.220219e38]}

    index = pd.to_datetime(
        [
            "2033-12-11 00:00:00",
            "2033-12-11 00:00:01",
        ]
    )
    df = pd.DataFrame(data, index=index)
    df_0col = df[0:0]
    chunks = [df, df_0col]
    for chnk in chunks:
        lib.stage(symbol, chnk, validate_index=True)
    lib.compact_incomplete(symbol, append=False, prune_previous_version=True, convert_int_to_float=False)
    expected_data = pd.concat(chunks).sort_index()
    assert_frame_equal_rebuild_index_first(expected_data, lib.read(symbol).data)


@Marks.storage.mark
def test_stage_with_and_without_errors(version_store_and_real_s3_basic_store_factory):
    """
    Tests  if different size chunks of dataframe can be staged
    """
    lib: NativeVersionStore = version_store_and_real_s3_basic_store_factory(
        dynamic_schema=False, segment_row_size=5, column_group_size=3
    )

    set_seed(321546556)
    symbol = "32545fsddf"
    df_size = 500

    def check_incomplete_staged(sym: str, remove_staged: bool = True) -> None:
        assert lib.list_symbols_with_incomplete_data() == [sym]
        lib.remove_incomplete(sym)
        assert lib.list_symbols_with_incomplete_data() == []

    df = (
        DFGenerator(size=df_size)
        .add_int_col("int8", dtype=np.int8)
        .add_int_col("uint64", dtype=np.uint64)
        .add_string_col("str", str_size=1, include_unicode=True)
        .add_float_col("float64")
        .add_bool_col("bool")
        .add_timestamp_col("ts", start_date=None)
        .generate_dataframe()
    )

    # df.index = generate_random_timestamp_array(size=df_size, seed=None)

    df_index_ts = df.copy(deep=True).set_index("ts")

    # Unsorted dataframe with index will trigger error if validate_index=True
    with pytest.raises(UnsortedDataException):
        lib.stage(symbol, df_index_ts, validate_index=True)
    assert lib.list_symbols_with_incomplete_data() == []

    # Unsorted dataframe without index will trigger error if sort_on_index=True
    for frame in [df, []]:
        with pytest.raises(InternalException):
            lib.stage(symbol, frame, sort_on_index=True)
        assert lib.list_symbols_with_incomplete_data() == []

    # Unsorted dataframe with index will be staged if both validate_index=True, sort_on_index=True
    lib.stage(symbol, df_index_ts, validate_index=True, sort_on_index=True)
    check_incomplete_staged(symbol)

    # Unsorted dataframe without index will be staged if both validate_index=False, sort_on_index=False
    lib.stage(symbol, df, validate_index=False, sort_on_index=False)
    check_incomplete_staged(symbol)

    # Empty dataframe can be staged by default
    lib.stage(symbol, [])
    check_incomplete_staged(symbol)

    # None can be staged can be staged by default
    lib.stage(symbol, None)
    check_incomplete_staged(symbol)

    # Complex structures can be staged by default
    lib.stage(symbol, get_metadata())
    check_incomplete_staged(symbol)


@pytest.mark.parametrize("dynamic_strings", [True, False])
@Marks.storage.mark
def test_batch_read_and_join_scenarios(basic_store_factory, dynamic_strings):
    """The test covers usage of batch_read_and_join with multiple parameters and error conditions"""
    lib: NativeVersionStore = basic_store_factory(dynamic_strings=dynamic_strings)

    q = QueryBuilder()
    q.concat("outer")
    df0 = (
        DFGenerator(size=20)
        .add_bool_col("bool")
        .add_float_col("A", np.float32)
        .add_int_col("B", np.int32)
        .add_int_col("C", np.uint16)
        .generate_dataframe()
    )

    df0_1 = (
        DFGenerator(size=20)
        .add_bool_col("bool")
        .add_float_col("A", np.float32)
        .add_int_col("B", np.int16)
        .add_int_col("C", np.uint16)
        .add_string_col("str", 10, include_unicode=True)
        .add_timestamp_col("ts")
        .generate_dataframe()
    )

    df1_len = 13
    df1 = (
        DFGenerator(size=df1_len)
        .add_bool_col("bool")
        .add_float_col("A", np.float64)
        .add_float_col("B", np.float32)
        .add_int_col("C", np.int64)
        .generate_dataframe()
    )

    df1_1 = (
        DFGenerator(size=df1_len)
        .add_bool_col("bool")
        .add_string_col("A", 10)
        .add_int_col("B", np.int64)
        .generate_dataframe()
    )

    lib.write("symbol0", df0)
    lib.write("symbol1", df1)

    # Concatenate multiple times
    data: pd.DataFrame = lib.batch_read_and_join(["symbol0", "symbol1", "symbol0", "symbol1"], query_builder=q).data
    expected = pd.concat([df0, df1, df0, df1], ignore_index=True)
    assert_frame_equal(expected, data)

    # Concatenate with error QB
    with pytest.raises(UserInputException):
        data: pd.DataFrame = lib.batch_read_and_join(
            ["symbol0", "symbol1"], query_builder=QueryBuilder(), as_ofs=[0, 0]
        ).data

    # Concatenate with column filter
    data: pd.DataFrame = lib.batch_read_and_join(
        ["symbol0", "symbol1"], query_builder=q, columns=[["A", "C", "none"], None]
    ).data
    df0_subset = df0[["A", "C"]]
    expected = pd.concat([df0_subset, df1], ignore_index=True)
    # Pandas concat will fill NaN for bools, Arcticdb is using False
    expected["bool"] = expected["bool"].fillna(False)
    assert_frame_equal(expected, data)

    # Concatenate symbols with column filters + row range
    # row range limit is beyond the len of data
    data: pd.DataFrame = lib.batch_read_and_join(
        ["symbol0", "symbol1"], query_builder=q, columns=[["A"], ["B"]], row_ranges=[(2, 3), (10, df1_len + 2)]
    ).data
    df0_subset = df0.loc[2:2, ["A"]]
    df1_subset = df1.loc[10:df1_len, ["B"]]
    expected = pd.concat([df0_subset, df1_subset], ignore_index=True)
    assert_frame_equal(expected, data)

    lib.write("symbol0", df0_1)
    lib.write("symbol1", df1_1)

    # Concatenate with wrong schema
    with pytest.raises(SchemaException):
        data: pd.DataFrame = lib.batch_read_and_join(["symbol0", "symbol1"], as_ofs=[1, 1], query_builder=q)

    # Concatenate symbols with column filters + row range
    # row range limit is beyond the len of data
    data: pd.DataFrame = lib.batch_read_and_join(
        ["symbol0", "symbol1", "symbol0"],
        as_ofs=[0, 0, 1],
        query_builder=q,
        columns=[None, ["B", "C"], None],
        row_ranges=[(2, 3), None, None],
    ).data
    df0_subset = df0.loc[2:2]
    df1_subset = df1[["B", "C"]]
    expected = pd.concat([df0_subset, df1_subset, df0_1], ignore_index=True)
    # Pandas concat will fill NaN for bools, Arcticdb is using False
    expected["bool"] = expected["bool"].fillna(False)
    # Pandas concat will fill NaN for strings, Arcticdb is using None
    if not dynamic_strings:
        # make expected result like the actual due to static string
        if not WINDOWS:
            # windows does not have static strings
            expected["str"] = expected["str"].fillna("")
    assert_frame_equal(expected, data)

    # Cover query builders per symbols
    q0 = QueryBuilder()
    q0 = q0[q0["A"] == 123.58743343]
    q1 = QueryBuilder()
    q1 = q1[q1["B"] == -3483.123434343]
    data: pd.DataFrame = lib.batch_read_and_join(
        ["symbol0", "symbol1"], as_ofs=[0, 0], query_builder=q, per_symbol_query_builders=[q0, q1]
    ).data
    assert len(data) == 0  # Nothing is selected
    data: pd.DataFrame = lib.batch_read_and_join(
        ["symbol0", "symbol1"], as_ofs=[0, 0], query_builder=q, per_symbol_query_builders=[q0, None]
    ).data
    expected_df = df1
    expected_df["B"] = expected_df["B"].astype(np.float64)    
    assert_frame_equal(df1, data)


@pytest.mark.xfail(True, reason="When non-existing symbol is used, MissingDataException is not raised 18023146743")
def test_batch_read_and_join_scenarios_errors(basic_store):
    lib: NativeVersionStore = basic_store

    q = QueryBuilder()
    q.concat("outer")
    df0 = DFGenerator(size=20).add_bool_col("bool").generate_dataframe()

    lib.write("symbol0", df0)

    # Concatenate with missing symbol
    with pytest.raises(MissingDataException):
        data: pd.DataFrame = lib.batch_read_and_join(["symbol0", "symbol2"], query_builder=q).data


@Marks.storage.mark
@pytest.mark.xfail(True, reason="Filtering of columns does not work for dynamic schema 18023047637")
def test_batch_read_and_join_scenarios_dynamic_schema_filtering_error(lmdb_version_store_dynamic_schema_v1):
    lib: NativeVersionStore = lmdb_version_store_dynamic_schema_v1

    q = QueryBuilder()
    q.concat("outer")
    df0 = (
        DFGenerator(size=20)
        .add_bool_col("bool")
        .add_float_col("A", np.float32)
        .add_int_col("B", np.int32)
        .add_int_col("C", np.uint16)
        .generate_dataframe()
    )

    df1_len = 13
    df1 = (
        DFGenerator(size=df1_len)
        .add_bool_col("bool")
        .add_float_col("A", np.float64)
        .add_float_col("B", np.float32)
        .add_int_col("C", np.int64)
        .generate_dataframe()
    )

    lib.write("symbol0", df0)
    lib.write("symbol1", df1)

    # Concatenate with column filter
    data: pd.DataFrame = lib.batch_read_and_join(
        ["symbol0", "symbol1"], query_builder=q, columns=[["A", "C", "none"], None]
    ).data
    df0_subset = df0[["A", "C"]]
    expected = pd.concat([df0_subset, df1], ignore_index=True)
    # Pandas concat will fill NaN for bools, Arcticdb is using False
    expected["bool"] = expected["bool"].fillna(False)
    ## ERROR: With dynamic schema filtering of the columns will fail
    #  here in the 'data' df instead of None/Na values for first 19 rows for
    #  bool and B column we will see values, which should not have been there
    #  If this was static schema - ie 'basic_store' fixture all would be fine
    assert_frame_equal(expected, data)

    data: pd.DataFrame = lib.batch_read_and_join(
        ["symbol0", "symbol1"], query_builder=q, columns=[["A"], ["B"]], row_ranges=[(2, 3), (10, df1_len + 2)]
    ).data
    df0_subset = df0.loc[2:2, ["A"]]
    df1_subset = df1.loc[10:df1_len, ["B"]]
    expected = pd.concat([df0_subset, df1_subset], ignore_index=True)
    # ERROR - here we observe that -/+ inf is added for int column "A"
    assert_frame_equal(expected, data)


def test_add_to_snapshot_and_remove_from_snapshots_scenarios(basic_store):
    lib: NativeVersionStore = basic_store
    lib.write("s1", 100)
    lib.write("s2", 200)

    lib.snapshot("snap")
    lib.write("s3", 300)
    lib.write("s1", 101)
    lib.write("s1", 102)
    lib.write("s1", 103)
    lib.write("s2", 201)
    lib.write("s4", 400)

    # We can add empty list of symbols without error
    lib.add_to_snapshot("snap", [])
    # We can remove nothing without error
    lib.remove_from_snapshot("snap", [], [])

    # add to snapshot operation succeeds even symbol does not exist
    lib.add_to_snapshot("snap", ["ss"])
    # remove from snapshot operation succeeds even symbol does not exist
    lib.remove_from_snapshot("snap", ["FDFGEREG"], [213])

    # remove from snapshot operation succeeds even symbol exists but version does not exist
    lib.remove_from_snapshot("snap", ["s2"], [2])
    lib.add_to_snapshot("snap", ["s2", "s1"], [4343, 45949345])

    # Verify the snapshot state is not changed
    assert 100 == lib.read("s1", as_of="snap").data
    assert 200 == lib.read("s2", as_of="snap").data
    with pytest.raises(NoSuchVersionException):
        lib.read("s3", as_of="snap")

    # Verify mixing of existing and non-existing symbols result
    # in proper versions of existing symbols added to the snapshot
    lib.add_to_snapshot("snap", [" ", 5443, "ss", "s1", "s4"])
    assert 103 == lib.read("s1", as_of="snap").data
    assert 400 == lib.read("s4", as_of="snap").data
    assert 200 == lib.read("s2", as_of="snap").data
    with pytest.raises(NoSuchVersionException):
        lib.read("s3", as_of="snap")

    # Verify mixing of existing and non-existing symbols and versions result
    # in proper versions of existing symbols added to the snapshot
    lib.add_to_snapshot("snap", ["Go home ...", "WELCOME!", "s1", "s2", "s2"], [1, 1, 1, 1, 4])
    assert 101 == lib.read("s1", as_of="snap").data
    assert 400 == lib.read("s4", as_of="snap").data
    assert 201 == lib.read("s2", as_of="snap").data
    with pytest.raises(NoSuchVersionException):
        lib.read("s3", as_of="snap")

    # Mix of valid and invalid symbols and versions does not affect removal from snapshot
    lib.remove_from_snapshot("snap", ["s11", "s1", "s2", "s1", "s2"], [33, 222, 123, 1, 1])
    assert 400 == lib.read("s4", as_of="snap").data
    for symbol in ["s1", "s2", "s3"]:
        with pytest.raises(NoSuchVersionException):
            lib.read(symbol, as_of="snap")


@pytest.mark.xfail(True, reason="Negative version numbers does not work, issue 10060901137")
def test_add_to_snapshot_with_negative_numbers(basic_store):
    lib: NativeVersionStore = basic_store
    lib.write("s1", 100)
    lib.snapshot("snap")
    lib.write("s1", 101)
    lib.write("s1", 102)
    lib.write("s1", 103)

    # Lets check negative number version handling
    lib.add_to_snapshot("snap", ["s1"], [-1])
    assert 102 == lib.read("s1", as_of="snap").data
    lib.add_to_snapshot("snap", ["s1"], [-2])
    assert 101 == lib.read("s1", as_of="snap").data


@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_remove_incomplete_for_v1_API(version_store_and_real_s3_basic_store_factory, dynamic_schema):
    """Testing staging and removing incomplete series for v1 API"""

    lib: NativeVersionStore = version_store_and_real_s3_basic_store_factory(
        dynamic_schema=dynamic_schema, segment_row_size=10
    )
    sym = "any symbol will do until don't"
    name = "series_name"
    length_of_series = np.random.randint(5, 26, size=10)

    for iter, length in enumerate(length_of_series):
        timestamp = pd.Timestamp(f"{1990 + iter}-1-1")
        series = generate_random_series(np.float64, length, name, start_time=timestamp, seed=None)
        if iter == 0:
            lib.write(sym, series)
        else:
            lib.stage(sym, series, validate_index=False, sort_on_index=False)

    assert lib.list_symbols_with_incomplete_data() == [sym]
    lib.remove_incomplete("")  # non-existing symbol
    lib.remove_incomplete("any name will do")  # non-existing symbol
    assert lib.list_symbols_with_incomplete_data() == [sym]
    lib.remove_incomplete(sym)
    assert lib.list_symbols_with_incomplete_data() == []


@Marks.storage.mark
def test_complete_incomplete_additional_scenarios(basic_store):
    """The test examines different combinations of input data types and compact_incomplete parameters
    to determine if incompletes are properly completed.

    During the test following 3 parameters are examined together:
      - stage_results
      - validate_index
      - delete_staged_data_on_failure

    The tests are executed with timestamped indexed series and dataframes on a single symbol in
    sequence one after the other
    """
    lib: NativeVersionStore = basic_store
    lib_tool = lib.library_tool()

    timestamps_ok = pd.to_datetime(
        [
            "2020-02-01",
            "2020-03-01",
        ]
    )

    timestamps_sorted_but_in_past = pd.to_datetime(
        [
            "2010-02-01",
            "2010-03-01",
            "2010-05-01",
            "2010-10-01",
        ]
    )

    # An index with NaT is not considered good, we have to see arcticdb reaction of
    # compatct_incomplete with such data
    timestamps_unsorted_with_nat = pd.to_datetime(
        [
            "2023-01-01",
            pd.NaT,
            "2023-01-03",
            "2023-01-04",
        ]
    )

    # An index with proper values but unsorted
    timestamps_unsorted = pd.to_datetime(
        [
            "2023-01-01",
            "2023-01-05",
            "2023-01-02",
        ]
    )

    df = pd.DataFrame({"value": [1, 2]}, index=timestamps_ok)
    df_unsorted = pd.DataFrame({"value": [10, 20, 30]}, index=timestamps_unsorted)
    df_unsorted_with_nat = pd.DataFrame({"value": [10, 20, 30, 40]}, index=timestamps_unsorted_with_nat)
    df_sorted_but_in_past = pd.DataFrame({"value": [10, 20, 30, 40]}, index=timestamps_sorted_but_in_past)

    series = pd.Series(data=[0.1, 0.2], name="name", index=timestamps_ok)
    series_unsorted = pd.Series(data=[1, 2, 3.3, 4], name="name", index=timestamps_unsorted_with_nat)

    symbol = "Sirius v1.0124"

    def do_tests(df_or_series: Union[pd.DataFrame, pd.Series], df_or_series_unsorted: Union[pd.DataFrame, pd.Series]):
        """Here we test behavior of completion of data when we have timestamp index"""
        df_empty = df_or_series.iloc[0:0]

        # Lets prepare a symbol and write data through stage
        lib.stage(symbol, df_or_series, validate_index=True)
        lib.stage(symbol, df_empty, validate_index=True)
        lib.compact_incomplete(symbol, append=False, validate_index=True, convert_int_to_float=False)
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 0

        # Compact incomplete fails on append due to data index is not sorted
        stage_res = lib.stage(symbol, df_or_series_unsorted, validate_index=False)
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 1
        with pytest.raises(UnsortedDataException):
            lib.compact_incomplete(symbol, append=True, validate_index=True, convert_int_to_float=False)
        assert_equals(df_or_series, lib.read(symbol).data)

        # Compact incomplete fails on write due to data index is not sorted, but we will delete the staged data on failure
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 1
        with pytest.raises(UnsortedDataException):
            lib.compact_incomplete(
                symbol,
                append=False,
                validate_index=True,
                convert_int_to_float=False,
                delete_staged_data_on_failure=True,
            )
        assert_equals(df_or_series, lib.read(symbol).data)

        # Validating that the deletion took place
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 0

        # Will try to compact_incomplete with valid and invalid stage result and other parameters
        stage_res_new = lib.stage(symbol, df_or_series_unsorted, validate_index=False)
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 1
        with pytest.raises(MissingKeysInStageResultsError):
            lib.compact_incomplete(
                symbol,
                append=False,
                stage_results=[stage_res_new, stage_res],
                validate_index=True,
                convert_int_to_float=False,
                delete_staged_data_on_failure=True,
            )
        assert_equals(df_or_series, lib.read(symbol).data)
        # Data should be still deleted
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 0

        # Will try to compact_incomplete with valid stage result with invalid index order and other parameters
        a = lib.stage(symbol, df_or_series_unsorted, validate_index=False)
        b = lib.stage(symbol, df_empty, validate_index=False)  # This one is valid empty
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 2
        with pytest.raises(UnsortedDataException):
            lib.compact_incomplete(
                symbol, append=False, stage_results=[a, b], validate_index=True, convert_int_to_float=False
            )
        assert_equals(df_or_series, lib.read(symbol).data)

        # Repeat same compaction with validating index == False
        with pytest.raises(UnsortedDataException):
            lib.compact_incomplete(
                symbol,
                append=True,
                stage_results=[a, b],
                validate_index=False,
                convert_int_to_float=False,
                delete_staged_data_on_failure=True,
            )
        assert_equals(df_or_series, lib.read(symbol).data)
        # Data should be still deleted
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 0

        if isinstance(df_or_series_unsorted.index, pd.DatetimeIndex) and not df_or_series_unsorted.index.isna().any():
            # if the df_or_series_unsorted does not contain NaT only then following tests will be valid
            a = lib.stage(symbol, df_or_series_unsorted.sort_index(), validate_index=False)
            b = lib.stage(symbol, df_empty, validate_index=False)
            lib.compact_incomplete(
                symbol,
                append=True,
                stage_results=[a, b],
                validate_index=False,
                convert_int_to_float=False,
                delete_staged_data_on_failure=True,
            )
            assert_equals(pd.concat([df_or_series, df_or_series_unsorted.sort_index()]), lib.read(symbol).data)

    do_tests(df, df_unsorted_with_nat)
    do_tests(df, df_unsorted)
    do_tests(series, series_unsorted)

    # One final test for index validation - staged data is sorted, but it is in the past vs the already written
    lib.write(symbol, df)
    lib.stage(symbol, df_sorted_but_in_past)
    with pytest.raises(UnsortedDataException):
        lib.compact_incomplete(symbol, append=True, validate_index=True, convert_int_to_float=False)


@Marks.storage.mark
def test_complete_incomplete_additional_scenarios_no_timestamp_index(basic_store):
    """The test examines different combinations of input data types and compact_incomplete parameters
    to determine if incompletes are properly completed.

    During the test following 3 parameters are examined together:
      - stage_results
      - validate_index
      - delete_staged_data_on_failure

    The tests are executed with a range indexed dataframes a series without index and np arrays on a single symbol in
    sequence one after the other
    """
    lib: NativeVersionStore = basic_store
    lib_tool = lib.library_tool()

    df_rangeindex = pd.DataFrame({"value": [1, 2]}, index=[1, 2])
    df_rangeindex_unsorted = pd.DataFrame({"value": [10, 20, 30]}, index=[3, 5, 4])

    df = pd.DataFrame({"value": [1, 2]})
    df_add = pd.DataFrame({"value": [10, 20, 30]})

    series = pd.Series(data=[0.1, 0.2], name="name")
    series_add = pd.Series(data=[1, 2, 3.3, 4], name="name")

    np_arr = np.array([1, 2], dtype=np.int64)
    np_arr_add = np.array([3, 4, 5], dtype=np.int64)

    def do_tests(df_or_series: Union[pd.DataFrame, pd.Series], df_or_series_unsorted: Union[pd.DataFrame, pd.Series]):
        """Here we test completion of data that has different than timestamp index"""
        symbol = "Soya beans & rice"
        if isinstance(df_or_series, np.ndarray):
            df_empty = df_or_series[:0]
        else:
            df_empty = df_or_series.iloc[0:0]

        # Lets prepare a symbol and write data through stage
        lib.stage(symbol, df_or_series, validate_index=True)
        lib.compact_incomplete(symbol, append=False, validate_index=True, convert_int_to_float=False)
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 0

        # Compact incomplete with wrong index should always succeed, regardless of append or write
        stage_res = lib.stage(symbol, df_or_series_unsorted, validate_index=False)
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 1
        lib.compact_incomplete(symbol, append=True, validate_index=True, convert_int_to_float=False)
        assert_equals(pd.concat([df_or_series, df_or_series_unsorted]), lib.read(symbol).data)
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 0

        # Compact incomplete with wrong index should always succeed, regardless of append or write
        stage_res = lib.stage(symbol, df_or_series_unsorted, validate_index=False)
        lib.compact_incomplete(
            symbol, append=False, validate_index=True, convert_int_to_float=False, delete_staged_data_on_failure=True
        )
        assert_equals(df_or_series_unsorted, lib.read(symbol).data)
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 0

        # Will try to compact_incomplete with valid and invalid stage result and other parameters
        stage_res_new = lib.stage(symbol, df_or_series_unsorted, validate_index=False)
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 1
        with pytest.raises(MissingKeysInStageResultsError):
            lib.compact_incomplete(
                symbol,
                append=False,
                stage_results=[stage_res_new, stage_res],
                validate_index=True,
                convert_int_to_float=False,
                delete_staged_data_on_failure=True,
            )
        assert_equals(df_or_series_unsorted, lib.read(symbol).data)
        # Data staged in completed
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 0

        # Will try to compact_incomplete with valid stage result with invalid index order (range index) and other parameters
        stage_res_new = lib.stage(symbol, df_or_series_unsorted, validate_index=False)
        stage_res_new_empty = lib.stage(symbol, df_empty, validate_index=False)
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 2
        lib.compact_incomplete(
            symbol,
            append=False,
            stage_results=[stage_res_new, stage_res_new_empty],
            validate_index=True,
            convert_int_to_float=False,
            delete_staged_data_on_failure=True,
        )
        assert_equals(df_or_series_unsorted, lib.read(symbol).data)
        # Data staged in completed
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, symbol)) == 0

        # Compact with empty stage_results
        with pytest.raises(UserInputException, match=r".*E_NO_STAGED_SEGMENTS.*"):
            lib.compact_incomplete(symbol, append=False, convert_int_to_float=False, stage_results=[])

    do_tests(df_rangeindex, df_rangeindex_unsorted)
    do_tests(series, series_add)
    # This one has issues - a separate test will cover it for now
    # do_tests(np_arr, np_arr_add)


@pytest.mark.xfail(True, reason="Compact_incomplete over staged np.arrays will result in unreadable data 18084289947")
def test_complete_incomplete_additional_scenarios_errors_np_array(basic_store):
    lib: NativeVersionStore = basic_store
    np_arr = np.array([1, 2], dtype=np.int64)
    np_arr_add = np.array([3, 4, 5], dtype=np.int64)

    symbol = "A"
    symbolB = "B"

    # This will pass
    lib.write(symbol, np_arr)
    lib.append(symbol, np_arr_add)

    lib.read(symbol).data

    # This one should also pass
    lib.stage(symbolB, np_arr, validate_index=False)
    lib.stage(symbolB, np_arr_add, validate_index=False)
    lib.compact_incomplete(
        symbolB, append=False, validate_index=False, convert_int_to_float=False, delete_staged_data_on_failure=True
    )
    lib.read(symbolB).data
    ## Above will produce error:
    #     def denormalize(self, item, norm_meta):
    #        original_shape = tuple(norm_meta.shape)
    #        data = item.data[0]
    # >       return data.reshape(original_shape)
    # E       ValueError: cannot reshape array of size 5 into shape (3,)
