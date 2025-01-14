"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import logging
import sys
import time
import psutil
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

from typing import Generator, Tuple
from arcticdb.util.test import get_sample_dataframe, random_string
from arcticdb.version_store.library import Library, ReadRequest
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.version_store._store import NativeVersionStore
from tests.util.mark import MACOS, SLOW_TESTS_MARK, WINDOWS, MEMRAY_SUPPORTED, MEMRAY_TESTS_MARK, SKIP_CONDA_MARK


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Memory_tests")


# region HELPER functions for non-memray tests


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


def check_process_memory_leaks(
    process_func, number_iterations, max_total_mem_lost_threshold_bytes, max_machine_memory_percentage
) -> np.int64:
    """
    This check accepts a function which will be called iteratively 'number_iterations'. During this
    process the procedure will monitor RSS growth of python process in order to be below specified
    threshold 'max_total_mem_lost_threshold_bytes' (in bytes) AND and the same time will watch so that machine
    utilization of memory is not above certain percentage 'max_machine_memory_percentage' to prevent
    crash of the process

    Specify more than 3 iterations (advised min 30), and be sure that there will be some initial growth in the first 1-2
    iterations, thus your expected growth threshold should be above that

    IMPORTANT: It is strongly advised during creation of future tests to they are executed first
    through command line to determine the optimal iteration  and 'max_total_mem_lost_threshold_bytes' (in bytes) for the
    python process. During the execution you will see lots of information on the screen including maximum growth
    of memory since iterations started as well as minimum growth of memory. That along with information
    how many times the growth of mem dropped 50% could provide insight for presence or lack of memory issues

    NOTE: This test is NOT CONCLUSIVE for absence of memory leaks. It would catch LARGE leaks.
    Small leaks will not be caught. If the procedure fails to sport problems it will return 2 numbers
    for further investigation by the test:
     - how much the process has grown during execution in bytes
     - how many times the process memory dropped more than 50% (indicating some C++ garbage collection)

    NOTE 2: Currently this code controls python GC only.

    """
    p = psutil.Process()
    avail_mem_start: np.int64 = psutil.virtual_memory().available
    print("-" * 80)
    print("Start check process for memory leaks")
    print("Num iterations: ", number_iterations)
    print("Maximum memory growth/lost to the examined process: ", max_total_mem_lost_threshold_bytes)
    print("Maximum machine memory utilization: ", max_total_mem_lost_threshold_bytes)

    lets_collect_some_garbage(10)

    mem_per: np.int64 = psutil.virtual_memory().percent
    assert max_machine_memory_percentage >= mem_per, "Machine has less memory available the check cannot start"
    print("Available memory", nice_bytes_str(avail_mem_start))
    print("-" * 80)
    process_initial_memory: np.int64 = p.memory_info().rss
    print("Process initial RSS", nice_bytes_str(process_initial_memory))

    mem_growth_each_iter = list()

    for n in range(number_iterations):
        p_iter_mem_start: np.int64 = p.memory_info().rss

        print("Starting watched code ...........")
        process_func()
        lets_collect_some_garbage(10)

        p_iter_mem_end: np.int64 = p.memory_info().rss
        process_growth: np.int64 = p.memory_info().rss - process_initial_memory
        mem_growth_each_iter.append(process_growth)
        process_avg_growth = process_growth / (n + 1)
        p_iter_mem_notcleaned: np.int64 = p_iter_mem_end - p_iter_mem_start
        mem_avail: np.int64 = psutil.virtual_memory().available
        mem_per: np.int64 = psutil.virtual_memory().percent
        print(
            f"Iter No[{n}]    : Process added (or if number is negative - process released) {nice_bytes_str(p_iter_mem_notcleaned)}.  Avail memory: {nice_bytes_str(mem_avail)} Used Mem: {mem_per}%"
        )
        print(
            f"  Overall stats : Process growth since start {nice_bytes_str(process_growth)} AVG growth per iter {nice_bytes_str(process_avg_growth)}"
        )
        print(f"  Minimum growth so far: {nice_bytes_str(min(mem_growth_each_iter))}")
        print(f"  Maximum growth so far: {nice_bytes_str(max(mem_growth_each_iter))}")
        print(f"  Number of times there was 50% drop in memory: {count_drops(mem_growth_each_iter, 0.5)}")

        assert max_total_mem_lost_threshold_bytes >= process_growth, (
            f"Memory of the process grew more than defined threshold: {nice_bytes_str(process_growth)} (specified: {nice_bytes_str(max_total_mem_lost_threshold_bytes)} )"
        )
        assert max_machine_memory_percentage >= mem_per, (
            f"Machine utilized more memory than specified threshold :{mem_per}% (specified {max_machine_memory_percentage}%)"
        )

    print(
        "The process assessment finished within expectations. Total consumed additional mem is bellow threshold: ",
        process_growth,
    )
    print("Returning this number as result.")
    return process_growth, count_drops(mem_growth_each_iter, 0.5)


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

# region HELPER functions for memray tests


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


@SKIP_CONDA_MARK  # Conda CI runner doesn't have enough storage to perform these stress tests
@pytest.mark.skipif(
    WINDOWS, reason="Not enough storage on Windows runners, due to large Win OS footprint and less free mem"
)
@pytest.mark.skipif(MACOS, reason="Problem on MacOs most probably similar to WINDOWS")
def test_mem_leak_read_all_arctic_lib(arctic_library_lmdb_100gb):
    lib: adb.Library = arctic_library_lmdb_100gb

    df = generate_big_dataframe()

    symbol = "test"
    lib.write(symbol, df)

    def proc_to_examine():
        data = lib.read(symbol).data
        del data

    df = lib.read(symbol).data
    del df

    """
        To determine optimal max_mem first set the procedure to a very high number like 10 GB
        and run it from commandline for approx 10 iteration.

        > python -m pytest -s python/tests/stress/arcticdb/test_mem_leaks.py::test_mem_leak_read_all_arctic_lib

        Observe 2 things:
         - maximum process memory growth
         - ideally there should be one iteration the mem did go down indicating GC in C++ code

         if both conditions are met then the process is likely to not leak big chunks of mem
         and you can take the max mem growth number add 20 percent safety margin and you will
         arrive at reasonable  max_mem

         Then put for number of iterations 3x or 5x and this mem limit and 
         run the test from command line again to assure it runs ok before commit 

    """
    max_mem_bytes = 295_623_040

    check_process_memory_leaks(proc_to_examine, 20, max_mem_bytes, 80.0)


@pytest.mark.skipif(
    WINDOWS, reason="Not enough storage on Windows runners, due to large Win OS footprint and less free mem"
)
@pytest.mark.skipif(MACOS, reason="Problem on MacOs most probably similar to WINDOWS")
@SKIP_CONDA_MARK  # Conda CI runner doesn't have enough storage to perform these stress tests
def test_mem_leak_querybuilder_standard(arctic_library_lmdb_100gb):
    """
    This test uses old approach with iterations.
    It is created for comparison with the new approach
    with memray
    (If memray is good in future we could drop the old approach)
    """
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

    max_mem_bytes = 550_623_040

    check_process_memory_leaks(proc_to_examine, 10, max_mem_bytes, 80.0)


@SKIP_CONDA_MARK  # Conda CI runner doesn't have enough storage to perform these stress tests
def test_mem_leak_read_all_native_store(lmdb_version_store_very_big_map):
    lib: NativeVersionStore = lmdb_version_store_very_big_map

    df = generate_big_dataframe()

    symbol = "test"
    lib.write(symbol, df)

    def proc_to_examine():
        data = lib.read(symbol).data
        del data

    df = lib.read(symbol).data
    del df

    """ 
    See comment in previous test
    """
    max_mem_bytes = 608_662_528

    check_process_memory_leaks(proc_to_examine, 20, max_mem_bytes, 80.0)


# endregion

# region TESTS pytest-memray type for memory limit and leaks

## NOTE: Currently tests can be executed on Python >= 3.8 only


@pytest.fixture
# NOTE: for now we run only V1 encoding as test is very slow
def library_with_symbol(
    arctic_library_lmdb, only_test_encoding_version_v1
) -> Generator[Tuple[Library, pd.DataFrame, str], None, None]:
    """
    As memray instruments memory, we need to take out
    everything not relevant from mem leak measurement out of
    test, and place it in setup of the test - in other words in the fixture
    Otherwise, memray instruments that code and it results in much slower execution
    as well as mixing results of memory leaks - are they in what we test - for example
    read() or in construct_df_querybuilder_tests() ? or in other code?
    """
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
    """
    As memray instruments memory, we need to take out
    everything not relevant from mem leak measurement out of
    test, and place it in setup of the test - in other words in the fixture
    Otherwise, memray instruments that code and it results in much slower execution
    as well as mixing results of memory leaks
    """
    lib: Library = arctic_library_lmdb
    symbol = "test"
    df = construct_df_querybuilder_tests(size=300)
    lib.write(symbol, df)
    yield (lib, df, symbol)


def mem_query(lib: Library, df: pd.DataFrame, num_repetitions: int = 1, read_batch: bool = False):
    """
    This is the function where we test different types
    of queries against a large dataframe. Later this
    function will be used for memory limit and memory leaks
    tests
    """
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


def test_mem_leak_queries_correctness_precheck(library_with_tiny_symbol):
    """
    This test does precheck to confirm queries work more or less
    If it fails then perhaps there was a problem with
    QueryBuilder functionality.
    All checks are based on size of expected dataframe returned by
    a queryno equality checks
    """

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


if MEMRAY_SUPPORTED:
    ##
    ## PYTEST-MEMRAY integration is available only from ver 3.8 on
    ##

    from pytest_memray import Stack

    def is_relevant(stack: Stack) -> bool:
        """
        function to decide what to filter out and not to count specific stackframes

        Stack class variables:

        filename: str
            The source file being executed, or "???" if unknown.

        function: str
            The function being executed, or "???" if unknown.

        lineno: int
            The line number of the executing line, or 0 if unknown.
        """
        for frame in stack.frames:
            # do something to check if we need this to be added
            # as mem leak
            # print(f"SAMPLE >>> {frame.filename}:{frame.function}[{frame.lineno}]")
            pass
        return True

    @MEMRAY_TESTS_MARK
    @pytest.mark.limit_leaks(location_limit="50 KB" if not MACOS else "60 KB", filter_fn=is_relevant)
    ## Unfortunately it is not possible to xfail memray tests. Instead:
    ##  - log issue for investigation and analysis = to fix leak, or filter out stack frame
    ##  - increase meantime the 'location limit' so that test continue to run and reduce risk of larger mem leaks
    ##  - leave some mark like bellow that code is subject to issue investigation with number of the issue for traceability
    ##  - https://man312219.monday.com/boards/7852509418/pulses/8078461031
    # @pytest.mark.skip(reason = "read() memory leaks Monday#8078461031")
    def test_mem_leak_querybuilder_read_memray(library_with_symbol):
        """
        Test to capture memory leaks >= of specified number

        NOTE: we could filter out not meaningful for us stackframes
        in future if something outside of us start to leak using
        the argument "filter_fn" - just add to the filter function
        what we must exclude from calculation
        """
        (lib, df, symbol) = library_with_symbol
        mem_query(lib, df)

    @SLOW_TESTS_MARK
    @MEMRAY_TESTS_MARK
    @pytest.mark.limit_leaks(location_limit="160 KB" if not MACOS else "200 KB", filter_fn=is_relevant)
    ## Unfortunately it is not possible to xfail memray tests. Instead:
    ##  - log issue for investigation and analysis = to fix leak, or filter out stack frame
    ##  - increase meantime the 'location limit' so that test continue to run and reduce risk of larger mem leaks
    ##  - leave some mark like bellow that code is subject to issue investigation with number of the issue for traceability
    ##  - https://man312219.monday.com/boards/7852509418/pulses/8067881190
    # @pytest.mark.skip(reason = "read() memory leaks Monday#8067881190")
    def test_mem_leak_querybuilder_read_manyrepeats_memray(library_with_tiny_symbol):
        """
        Test to capture memory leaks >= of specified number

        NOTE: we could filter out not meaningful for us stackframes
        in future if something outside of us start to leak using
        the argument "filter_fn" - just add to the filter function
        what we must exclude from calculation
        """
        (lib, df, symbol) = library_with_tiny_symbol
        mem_query(lib, df, num_repetitions=125)

    @SLOW_TESTS_MARK
    @MEMRAY_TESTS_MARK
    @pytest.mark.limit_leaks(location_limit="160 KB" if not MACOS else "200 KB", filter_fn=is_relevant)
    ## Unfortunately it is not possible to xfail memray tests. Instead:
    ##  - log issue for investigation and analysis = to fix leak, or filter out stack frame
    ##  - increase meantime the 'location limit' so that test continue to run and reduce risk of larger mem leaks
    ##  - leave some mark like bellow that code is subject to issue investigation with number of the issue for traceability
    ##  - https://man312219.monday.com/boards/7852509418/pulses/8067881190
    # @pytest.mark.skip(reason = "read() memory leaks Monday#8067881190")
    def test_mem_leak_querybuilder_read_batch_manyrepeats_memray(library_with_tiny_symbol):
        """
        Test to capture memory leaks >= of specified number

        NOTE: we could filter out not meaningful for us stackframes
        in future if something outside of us start to leak using
        the argument "filter_fn" - just add to the filter function
        what we must exclude from calculation
        """
        (lib, df, symbol) = library_with_tiny_symbol
        mem_query(lib, df, num_repetitions=125, read_batch=True)

    @MEMRAY_TESTS_MARK
    @pytest.mark.limit_leaks(location_limit="25 KB", filter_fn=is_relevant)
    def test_mem_leak_querybuilder_read_batch_memray(library_with_symbol):
        """
        Test to capture memory leaks >= of specified number

        NOTE: we could filter out not meaningful for us stackframes
        in future if something outside of us start to leak using
        the argument "filter_fn" - just add to the filter function
        what we must exclude from calculation
        """
        (lib, df, symbol) = library_with_symbol
        mem_query(lib, df, read_batch=True)

    @MEMRAY_TESTS_MARK
    @pytest.mark.limit_memory("490 MB")
    def test_mem_limit_querybuilder_read_memray(library_with_symbol):
        """
        The fact that we do not leak memory does not mean that we
        are efficient on memory usage. This test captures the memory usage
        and limits it, so that we do not go over it (unless fro good reason)
        Thus if the test fails then perhaps we are using now more memory than
        in the past
        """
        (lib, df, symbol) = library_with_symbol
        mem_query(lib, df)

    @MEMRAY_TESTS_MARK
    @pytest.mark.limit_memory("490 MB")
    def test_mem_limit_querybuilder_read_batch_memray(library_with_symbol):
        """
        The fact that we do not leak memory does not mean that we
        are efficient on memory usage. This test captures the memory usage
        and limits it, so that we do not go over it (unless fro good reason)
        Thus if the test fails then perhaps we are using now more memory than
        in the past
        """
        (lib, df, symbol) = library_with_symbol
        mem_query(lib, df, True)

    @pytest.fixture
    def library_with_big_symbol_(arctic_library_lmdb) -> Generator[Tuple[Library, str], None, None]:
        """
        As memray instruments memory, we need to take out
        everything not relevant from mem leak measurement out of
        test, so it works as less as possible
        """
        lib: Library = arctic_library_lmdb
        symbol = "symbol"
        df: pd.DataFrame = generate_big_dataframe(300000)
        lib.write(symbol, df)
        del df
        yield (lib, symbol)

    @MEMRAY_TESTS_MARK
    @pytest.mark.limit_leaks(location_limit="30 KB", filter_fn=is_relevant)
    def test_mem_leak_read_all_arctic_lib_memray(library_with_big_symbol_):
        """
        This is a new version of the initial test that reads the whole
        big dataframe in memory
        """
        lib: Library = None
        (lib, symbol) = library_with_big_symbol_
        logger.info("Test starting")
        st = time.time()
        data: pd.DataFrame = lib.read(symbol).data
        del data
        logger.info(f"Test took : {time.time() - st}")

        gc.collect()
