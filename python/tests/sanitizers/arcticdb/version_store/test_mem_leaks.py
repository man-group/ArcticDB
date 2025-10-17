"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import logging
import time
import gc

import pytest
import numpy as np
import pandas as pd
import pytest
import arcticdb as adb
import gc
import random
import time
import pandas as pd

from typing import Generator, List, Tuple
from arcticdb.encoding_version import EncodingVersion
from arcticdb.options import LibraryOptions
from arcticdb.util.test import get_sample_dataframe, random_string
from arcticdb.util.utils import DFGenerator
from arcticdb.version_store.library import Library, ReadRequest
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.version_store._store import NativeVersionStore
from tests.conftest import Marks
from tests.util.mark import (
    SANITIZER_TESTS_MARK,
)
from tests.util.marking import marks


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Sanitizer_tests")


def nice_bytes_str(bytes):
    return f" {bytes / (1024 * 1024):.2f}MB/[{bytes}] "


def lets_collect_some_garbage(time_sec: int = 7):
    """
    Do a garbage collection
    """
    gc.collect()
    print(f"Lets pause for {time_sec} secs so GC to kick in")
    time.sleep(time_sec)


def count_drops(numbers, percentage_drop_0_1):
    """
    Count how many times there was a number which was
    'percentage_drop_0_1' percentages lower than previous one

    params:
     - percentage_drop_0_1 - between 0 and 1
    """
    count = 0
    for i in range(1, len(numbers)):
        if numbers[i] < percentage_drop_0_1 * numbers[i - 1]:
            count += 1
    return count


def grow_exp(df_to_grow: pd.DataFrame, num_times_xx2: int):
    """
    Quickly inflates the dataframe by doubling its size for each iteration
    Thus the final size will be 'num_times_xx2' power of two
    """
    for n in range(1, num_times_xx2):
        df_prev = df_to_grow.copy(deep=True)
        df_to_grow = pd.concat([df_to_grow, df_prev])
    return df_to_grow


def generate_big_dataframe(rows: int = 1000000, num_exp_time_growth: int = 5) -> pd.DataFrame:
    """
    A quick and time efficient wat to generate very large dataframe.
    The first parameter will be passed to get_sample_dataframe() so that a dataframe
    of that number of rows is generated. Later this df will be used to grow exponentially (power of 2)
    the number of rows on the final dataframe by concatenating N times the result dataframe to itself
    That process if >6-8 times more time efficient. The down side is that you have N**2 repetitions of
    same original table. But for many usages size is what we need not uniqueness
    """
    logger.info("Generating big dataframe")
    st = time.time()
    df = get_sample_dataframe(rows)
    df = grow_exp(df, num_exp_time_growth)
    logger.info(f"Generation took : {time.time() - st}")
    return df


# endregion

# region HELPER functions for memory leak tests


def construct_df_querybuilder_tests(size: int) -> pd.DataFrame:
    df = get_sample_dataframe(size)
    df.index = pd.date_range(start="2000-1-1", periods=size, freq="s")
    df.index.name = "timestamp"
    return df


def query_filter_then_groupby_with_aggregations() -> QueryBuilder:
    """
    groupby composite aggregation query for QueryBuilder memory tests.
    The query basically will do aggregation of half of dataframe
    """
    q = QueryBuilder()
    return (
        q[q["bool"]]
        .groupby("uint8")
        .agg({"uint32": "mean", "int32": "sum", "strings": "count", "float64": "sum", "float32": "min", "int16": "max"})
    )


def query_no_filter_only_groupby_with_aggregations() -> QueryBuilder:
    """
    groupby composite aggregation query for QueryBuilder memory tests.
    The query basically will do aggregation of half of dataframe
    """
    q = QueryBuilder()
    return q.groupby("uint8").agg(
        {"uint32": "mean", "int32": "sum", "strings": "count", "float64": "sum", "float32": "min", "int16": "max"}
    )


def query_filter_impossible_cond_groupby_with_aggregations_for_whole_frame() -> QueryBuilder:
    """
    groupby composite aggregation query for QueryBuilder memory tests.
    The query basically will do aggregation of whole dataframe
    """
    q = QueryBuilder()
    return (
        q[q["strings"] != "QASDFGH"]
        .groupby("int16")
        .agg({"uint32": "mean", "int32": "sum", "strings": "count", "float64": "sum", "float32": "min", "int16": "max"})
    )


def query_apply_clause_only(strng: str) -> QueryBuilder:
    """
    Apply query for QueryBuilder memory tests.
    This version of apply does couple of nested operations
    with columns that were just added
    """
    q = QueryBuilder()
    q = q[q["strings"] != strng].apply("NEW 1", q["uint32"] * q["int32"] / q["float32"])
    q = q.apply("NEW 2", q["float32"] + q["float64"] + q["NEW 1"])
    q = q.apply("NEW 3", q["NEW 2"] - q["NEW 1"] + q["timestamp"])
    return q


def query_resample_minutes() -> QueryBuilder:
    """
    Resample query for QueryBuilder memory tests
    """
    q = QueryBuilder()
    return q.resample("min").agg(
        {
            "int8": "min",
            "int16": "max",
            "int32": "first",
            "int64": "last",
            "uint64": "sum",
            "float32": "mean",
            "float64": "sum",
            "strings": "count",
            "bool": "sum",
        }
    )


def query_row_range_57percent(size: int) -> QueryBuilder:
    """
    Row range query for QueryBuilder memory tests
    Pass size of dataframe and it will generate random row range
    """
    percentage_rows_returned = 0.57
    start_percentage = random.uniform(0.01, 1.0 - percentage_rows_returned)
    result_size_rows = int(0.57 * size)
    q = QueryBuilder()
    a = random.randint(0, int((size - 1) * start_percentage))
    b = a + result_size_rows
    logger.info(f" GENERATED ROW RANGE {a} - {b}")
    return q.row_range((a, b))


def query_date_range_57percent(start: pd.Timestamp, end: pd.Timestamp) -> QueryBuilder:
    """
    Date range query for QueryBuilder memory tests
    Will generate random date range that will return
    always the specified percentage rows
    """
    percentage_rows_returned = 0.57
    start_percentage = random.uniform(0.01, 1.0 - percentage_rows_returned)
    q = QueryBuilder()
    total_duration = end - start
    percent_duration = total_duration * start_percentage
    a = start + percent_duration
    percent_duration = total_duration * percentage_rows_returned
    b = a + percent_duration
    logger.info(f" GENERATED DATE RANGE {a} - {b}")
    return q.date_range((a, b))


def print_info(data: pd.DataFrame, q: QueryBuilder):
    logger.info(f"Query {q}")
    logger.info(f"Size of DF returned by arctic:{data.shape[0]} columns: {data.shape[1]}")


def gen_random_date(start: pd.Timestamp, end: pd.Timestamp):
    """
    Returns random timestamp from specified period
    """
    date_range = pd.date_range(start=start, end=end, freq="s")
    return random.choice(date_range)


# endregion

# region TESTS non-memray type - "guessing" memory leak through series of repetitions


@SANITIZER_TESTS_MARK
def test_mem_leak_read_all_arctic_lib(arctic_library_lmdb_100gb):
    lib: adb.Library = arctic_library_lmdb_100gb

    df = generate_big_dataframe()

    symbol = "test"
    lib.write(symbol, df)

    df = lib.read(symbol).data


@SANITIZER_TESTS_MARK
@marks([Marks.pipeline])
def test_mem_leak_querybuilder_standard(arctic_library_lmdb_100gb):
    lib: Library = arctic_library_lmdb_100gb

    df = construct_df_querybuilder_tests(size=2000000)
    size = df.shape[0]
    start_date = df.index[0]
    end_date = df.index[-1]

    symbol = "test"
    lib.write(symbol, df)
    del df
    gc.collect()

    def proc_to_examine():
        queries = [
            query_filter_then_groupby_with_aggregations(),
            query_filter_impossible_cond_groupby_with_aggregations_for_whole_frame(),
            query_no_filter_only_groupby_with_aggregations(),
            query_apply_clause_only(random_string(10)),
            query_resample_minutes(),
            query_row_range_57percent(size),
            query_date_range_57percent(start_date, end_date),
        ]
        for q in queries:
            data: pd.DataFrame = lib.read(symbol, query_builder=q).data
            print_info(data, q)
            del data
            gc.collect()
        del queries
        gc.collect()

    proc_to_examine()


@SANITIZER_TESTS_MARK
def test_mem_leak_read_all_native_store(lmdb_version_store_very_big_map):
    lib: NativeVersionStore = lmdb_version_store_very_big_map

    df = generate_big_dataframe()

    symbol = "test"
    lib.write(symbol, df)

    data = lib.read(symbol).data
    del data


@pytest.fixture
# NOTE: for now we run only V1 encoding as test is very slow
def library_with_symbol(
    arctic_library_lmdb, only_test_encoding_version_v1
) -> Generator[Tuple[Library, pd.DataFrame, str], None, None]:
    lib: Library = arctic_library_lmdb
    symbol = "test"
    df = construct_df_querybuilder_tests(size=2000000)
    lib.write(symbol, df)
    yield (lib, df, symbol)


@pytest.fixture
# NOTE: for now we run only V1 encoding as test is very slow
def library_with_tiny_symbol(
    arctic_library_lmdb, only_test_encoding_version_v1
) -> Generator[Tuple[Library, pd.DataFrame, str], None, None]:
    lib: Library = arctic_library_lmdb
    symbol = "test"
    df = construct_df_querybuilder_tests(size=300)
    lib.write(symbol, df)
    yield (lib, df, symbol)


def mem_query(lib: Library, df: pd.DataFrame, num_repetitions: int = 1, read_batch: bool = False):
    size = df.shape[0]
    start_date = df.index[0]
    end_date = df.index[-1]

    symbol = "test"
    lib.write(symbol, df)
    del df
    gc.collect()

    queries = [
        query_filter_then_groupby_with_aggregations(),
        query_no_filter_only_groupby_with_aggregations(),
        query_filter_impossible_cond_groupby_with_aggregations_for_whole_frame(),
        query_apply_clause_only(random_string(10)),
        query_resample_minutes(),
        query_row_range_57percent(size),
        query_date_range_57percent(start_date, end_date),
    ]

    for rep in range(num_repetitions):
        logger.info(f"REPETITION : {rep}")
        if read_batch:
            logger.info("RUN read_batch() tests")
            read_requests = [ReadRequest(symbol=symbol, query_builder=query) for query in queries]
            results_read = lib.read_batch(read_requests)
            cnt = 0
            for result in results_read:
                assert not result.data is None
                if num_repetitions < 20:
                    print_info(result.data, queries[cnt])
                cnt += 1
            del read_requests, results_read
        else:
            logger.info("RUN read() tests")
            for q in queries:
                data: pd.DataFrame = lib.read(symbol, query_builder=q).data
                lib.read_batch
                if num_repetitions < 20:
                    print_info(data, q)
            del data
        gc.collect()

    del lib, queries
    gc.collect()


@SANITIZER_TESTS_MARK
def test_mem_leak_queries_correctness_precheck(library_with_tiny_symbol):
    df: pd.DataFrame = None
    lib: Library = None
    (lib, df, symbol) = library_with_tiny_symbol

    size = df.shape[0]
    start_date = df.index[0]
    end_date = df.index[-1]

    lib.write(symbol, df)

    data: pd.DataFrame = lib.read(
        symbol, query_builder=query_filter_impossible_cond_groupby_with_aggregations_for_whole_frame()
    ).data
    assert len(df.int16.unique()) == data.shape[0]

    data: pd.DataFrame = lib.read(symbol, query_builder=query_row_range_57percent(size)).data
    assert df.shape[0] < data.shape[0] * 2

    data: pd.DataFrame = lib.read(symbol, query_builder=query_date_range_57percent(start_date, end_date)).data
    assert df.shape[0] < data.shape[0] * 2

    data: pd.DataFrame = lib.read(symbol, query_builder=query_apply_clause_only(random_string(10))).data
    assert len(df.columns.to_list()) <= data.shape[0] * 2
    assert 200 < data.shape[0]

    data: pd.DataFrame = lib.read(symbol, query_builder=query_no_filter_only_groupby_with_aggregations()).data
    # groupby column becomes index
    assert sorted(list(df["uint8"].unique())) == sorted(list(data.index.unique()))


@SANITIZER_TESTS_MARK
@marks([Marks.pipeline])
def test_mem_leak_querybuilder_read(library_with_symbol):
    """
    Test to capture memory leaks >= of specified number

    NOTE: we could filter out not meaningful for us stackframes
    in future if something outside of us start to leak using
    the argument "filter_fn" - just add to the filter function
    what we must exclude from calculation
    """
    (lib, df, symbol) = library_with_symbol
    mem_query(lib, df)


@SANITIZER_TESTS_MARK
@marks([Marks.pipeline])
def test_mem_leak_querybuilder_read_manyrepeats(library_with_tiny_symbol):
    """
    Test to capture memory leaks >= of specified number

    NOTE: we could filter out not meaningful for us stackframes
    in future if something outside of us start to leak using
    the argument "filter_fn" - just add to the filter function
    what we must exclude from calculation
    """
    (lib, df, symbol) = library_with_tiny_symbol
    mem_query(lib, df, num_repetitions=125)


@SANITIZER_TESTS_MARK
@marks([Marks.pipeline])
def test_mem_leak_querybuilder_read_batch_manyrepeats(library_with_tiny_symbol):
    """
    Test to capture memory leaks >= of specified number

    NOTE: we could filter out not meaningful for us stackframes
    in future if something outside of us start to leak using
    the argument "filter_fn" - just add to the filter function
    what we must exclude from calculation
    """
    (lib, df, symbol) = library_with_tiny_symbol
    mem_query(lib, df, num_repetitions=125, read_batch=True)


@SANITIZER_TESTS_MARK
@marks([Marks.pipeline])
def test_mem_leak_querybuilder_read_batch(library_with_symbol):
    (lib, df, symbol) = library_with_symbol
    mem_query(lib, df, read_batch=True)


@SANITIZER_TESTS_MARK
def test_mem_limit_querybuilder_read(library_with_symbol):
    (lib, df, symbol) = library_with_symbol
    mem_query(lib, df)


@SANITIZER_TESTS_MARK
def test_mem_limit_querybuilder_read_batch(library_with_symbol):
    (lib, df, symbol) = library_with_symbol
    mem_query(lib, df, True)


@pytest.fixture
def library_with_big_symbol_(arctic_library_lmdb) -> Generator[Tuple[Library, str], None, None]:
    lib: Library = arctic_library_lmdb
    symbol = "symbol"
    df: pd.DataFrame = generate_big_dataframe(300000)
    lib.write(symbol, df)
    del df
    yield (lib, symbol)


@SANITIZER_TESTS_MARK
def test_mem_leak_read_all_arctic_lib(library_with_big_symbol_):
    lib: Library = None
    (lib, symbol) = library_with_big_symbol_
    logger.info("Test starting")
    st = time.time()
    data: pd.DataFrame = lib.read(symbol).data
    del data
    logger.info(f"Test took : {time.time() - st}")

    gc.collect()


@pytest.fixture
def lmdb_library(lmdb_storage, lib_name, request) -> Generator[Library, None, None]:
    """
    Allows passing library creation parameters as parameters of the test or other fixture.
    Example:


        @pytest.mark.parametrize("lmdb_library_any", [
                    {'library_options': LibraryOptions(rows_per_segment=100, columns_per_segment=100)}
                ], indirect=True)
        def test_my_test(lmdb_library_any):
        .....
    """
    params = request.param if hasattr(request, "param") else {}
    yield lmdb_storage.create_arctic().create_library(name=lib_name, **params)


@pytest.fixture
def prepare_head_tails_symbol(lmdb_library):
    lib: Library = lmdb_library
    opts = lib.options()

    total_number_columns = 1002
    symbol = "asdf12345"
    num_rows_list = [279, 199, 1, 350, 999, 0, 1001]
    snapshot_names = []
    for rows in num_rows_list:
        st = time.time()
        df = DFGenerator.generate_wide_dataframe(
            num_rows=rows, num_cols=total_number_columns, num_string_cols=25, start_time=pd.Timestamp(0), seed=64578
        )
        lib.write(symbol, df)
        snap = f"{symbol}_{rows}"
        lib.snapshot(snap)
        snapshot_names.append(snap)
        logger.info(f"Generated {rows} in {time.time() - st} sec")
        if opts.dynamic_schema:
            # Dynamic libraries are dynamic by nature so the test should cover that
            # characteristic
            total_number_columns += 20
            logger.info(f"Total number of columns increased to {total_number_columns}")

    all_columns = df.columns.to_list()
    yield (lib, symbol, num_rows_list, snapshot_names, all_columns)
    lib.delete(symbol=symbol)


@SANITIZER_TESTS_MARK
@marks([Marks.pipeline])
@pytest.mark.parametrize(
    "lmdb_library",
    [
        {
            "library_options": LibraryOptions(
                rows_per_segment=233,
                columns_per_segment=197,
                dynamic_schema=True,
                encoding_version=EncodingVersion.V2,
            )
        },
        {
            "library_options": LibraryOptions(
                rows_per_segment=99,
                columns_per_segment=99,
                dynamic_schema=False,
                encoding_version=EncodingVersion.V1,
            )
        },
    ],
    indirect=True,
)
@marks([Marks.pipeline])
def test_mem_leak_head_tail(prepare_head_tails_symbol):
    symbol: str
    num_rows_list: List[int]
    store: NativeVersionStore = None
    snapshot_names: List[str]
    all_columns: List[str]
    (store, symbol, num_rows_list, snapshot_names, all_columns) = prepare_head_tails_symbol

    start_test: float = time.time()
    max_rows: int = max(num_rows_list)

    np.random.seed(959034)
    # constructing a list of head and tail rows to be selected
    num_rows_to_select = []
    important_values = [0, 1, 0 - 1, 2, -2, max_rows, -max_rows]  # some boundary cases
    num_rows_to_select.extend(important_values)
    num_rows_to_select.extend(np.random.randint(low=5, high=99, size=7))  # add 7 more random values
    # number of iterations will be the list length/size
    iterations = len(num_rows_to_select)
    # constructing a random list of values for snapshot names for each iteration
    snapshots_list: List[str] = np.random.choice(snapshot_names, iterations)
    # constructing a random list of values for versions names for each iteration
    versions_list: List[int] = np.random.randint(0, len(num_rows_list) - 1, iterations)
    # constructing a random list of number of columns to be selected
    number_columns_for_selection_list: List[int] = np.random.randint(0, len(all_columns) - 1, iterations)

    count: int = 0
    # We will execute several time all head/tail operations with specific number of columns.
    # the number of columns consist of random columns and boundary cases see definition above
    for rows in num_rows_to_select:
        selected_columns: List[str] = np.random.choice(
            all_columns, number_columns_for_selection_list[count], replace=False
        ).tolist()
        snap: str = snapshots_list[count]
        ver: str = int(versions_list[count])
        logger.info(f"rows {rows} / snapshot {snap}")
        df1: pd.DataFrame = store.head(n=rows, as_of=snap, symbol=symbol).data
        df2: pd.DataFrame = store.tail(n=rows, as_of=snap, symbol=symbol).data
        df3: pd.DataFrame = store.head(n=rows, as_of=ver, symbol=symbol, columns=selected_columns).data
        difference = list(set(df3.columns.to_list()).difference(set(selected_columns)))
        assert len(difference) == 0, f"Columns not included : {difference}"
        df4: pd.DataFrame = store.tail(n=rows, as_of=ver, symbol=symbol, columns=selected_columns).data
        difference = list(set(df4.columns.to_list()).difference(set(selected_columns)))
        assert len(difference) == 0, f"Columns not included : {difference}"

        logger.info(f"Iteration {count} / {iterations} completed")
        count += 1
        del selected_columns, df1, df2, df3, df4

    del store, symbol, num_rows_list, snapshot_names, all_columns
    del num_rows_to_select, important_values, snapshots_list, versions_list, number_columns_for_selection_list
    gc.collect()
    time.sleep(10)  # collection is not immediate
    logger.info(f"Test completed in {time.time() - start_test}")
