"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import gc
import random
import time
from typing import Generator, Tuple
import pytest
import pandas as pd
from pytest_memray import Stack

from arcticdb.util.test import get_sample_dataframe, random_string
from arcticdb.version_store.library import Library
from arcticdb.version_store.processing import QueryBuilder
from tests.stress.arcticdb.version_store.test_mem_leaks import check_process_memory_leaks, generate_big_dataframe, grow_exp


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


def construct_df(size: int) -> pd.DataFrame:
    df = get_sample_dataframe(size)
    date_range = pd.date_range(start="2000-1-1", periods=size, freq="s")
    df.index = pd.Index(date_range)
    df.index.name = "timestamp"
    return df


def query_group() -> QueryBuilder:
    q = QueryBuilder()
    return (
        q[q["bool"] == True]
        .groupby("uint8")
        .agg({"uint32": "mean", "int32": "sum", "strings": "count", "float64": "sum", "float32": "min", "int16": "max"})
    )


def query_group_bigger_result() -> QueryBuilder:
    q = QueryBuilder()
    return (
        q[q["strings"] != "QASDFGH"]
        .groupby("int16")
        .agg({"uint32": "mean", "int32": "sum", "strings": "count", "float64": "sum", "float32": "min", "int16": "max"})
    )


def query_apply(str:str) -> QueryBuilder:
    q = QueryBuilder()
    q = q[q["strings"] != str].apply("NEW 1", q["uint32"] * q["int32"] / q["float32"])
    q = q.apply("NEW 2", q["float32"] + q["float64"] + q["NEW 1"])
    q = q.apply("NEW 3", q["NEW 2"] - q["NEW 1"] + q["timestamp"])
    return q


def query_resample() -> QueryBuilder:
    q = QueryBuilder()
    return q.resample("min").agg(
        {"int8" : "min",
        "int16" : "max",
        "int32" : "first",
        "int64" : "last",
        "uint64" : "sum",
        "float32" : "mean",
        "float64" : "sum",
        "strings" : "count",
        "bool" : "sum"}
        )

def query_row_range(size:int) -> QueryBuilder:
    q = QueryBuilder()
    a = random.randint(0,size-1)
    b = random.randint(0,size-1)
    return q.row_range( (min(a,b), max(a,b)) )


def query_date_range(start:pd.Timestamp, end: pd.Timestamp) -> QueryBuilder:
    q = QueryBuilder()
    a = gen_random_date(start,end)
    b = gen_random_date(start,end)
    return q.date_range( (min(a,b), max(a,b)) )


def print_info(data: pd.DataFrame, q: QueryBuilder):
    print("Query", q)
    print("Size of DF returned by arctic:", data.shape[0], " columns:", data.shape[1])


def gen_random_date(start:pd.Timestamp, end: pd.Timestamp):
    date_range = pd.date_range(start=start, end=end, freq='s') 
    return random.choice(date_range)


symbol = "test"


@pytest.fixture
def library_with_symbol(arctic_library_lmdb) -> Generator[Tuple[Library, pd.DataFrame], None, None]:
    """
        As memray instruments memory, we need to take out 
        everything not relevant from mem leak measurement out of 
        test, so it works as less as possible
    """
    lib: Library = arctic_library_lmdb
    df = construct_df(size=2000000)
    lib.write(symbol, df)
    yield (lib, df)

def mem_query(lib: Library, df: pd.DataFrame, num_repetitions:int=1):
    """
        This is the function where we test different types
        of queries against a large dataframe. Later this
        function will be used for memory limit and memory leaks 
        tests
    """
    size = df.shape[0]
    start_date = df.iloc[0].name
    end_date = df.iloc[0].name

    symbol = "test"
    lib.write(symbol, df)
    del df
    gc.collect()

    queries = []

    queries = [query_group(), 
                query_group_bigger_result(), 
                query_apply(random_string(10)), 
                query_resample(),
                query_row_range(size),
                query_date_range(start_date, end_date)]
    
    for rep in range(num_repetitions):
        for q in queries:
            data: pd.DataFrame = lib.read(symbol, query_builder=q).data
            print_info(data, q)
            del data
            gc.collect()

    del lib, queries
    gc.collect()
    time.sleep(10)


@pytest.mark.limit_leaks(location_limit="25 KB", filter_fn=is_relevant)
def test_mem_leak_query(library_with_symbol):
    """
        Test to capture memory leaks >= of specified number

        NOTE: wec could filter out not meaningful for us stackframes
        in future if something outside of us start to leak using
        the argument "filter_fn" - just add to the filter function
        what we must exclude from calculation
    """
    (lib, df) = library_with_symbol
    mem_query(lib, df)


@pytest.mark.limit_memory("490 MB")
def test_mem_limit_query(library_with_symbol):
    """
        The fact that we do not leak memory does not mean that we
        are efficient on memory usage. This test captures the memory usage
        and limits it, so that we do not go over it (unless fro good reason)
        Thus if the test fails then perhaps we are using now more memory than
        in the past
    """
    (lib, df) = library_with_symbol
    mem_query(lib, df)


def test_mem_leak_query_standard(arctic_library_lmdb):
    """
        This test uses old approach with iterations.
        It is created for comparison with the new approach
        with memray
        (If memray is good in future we could drop the old approach)
    """
    lib: Library = arctic_library_lmdb

    df = construct_df(size=2000000)
    size = df.shape[0]
    start_date = df.iloc[0].name
    end_date = df.iloc[0].name

    symbol = "test"
    lib.write(symbol, df)
    del df
    gc.collect()

    queries = [query_group(), query_group_bigger_result(), query_apply(random_string(10)), query_resample()]

    def proc_to_examine():
        queries = [query_group(), 
                    query_group_bigger_result(), 
                    query_apply(random_string(10)), 
                    query_resample(),
                    query_row_range(size),
                    query_date_range(start_date, end_date)]
        for q in queries:
            data: pd.DataFrame = lib.read(symbol, query_builder=q).data
            print_info(data, q)
            del data
            gc.collect()

    max_mem_bytes = 550_623_040

    check_process_memory_leaks(proc_to_examine, 10, max_mem_bytes, 80.0)

    del lib, queries
    gc.collect()
    time.sleep(10)


@pytest.fixture
def library_with_big_symbol(arctic_library_lmdb) -> Generator[Library, None, None]:
    """
        As memray instruments memory, we need to take out 
        everything not relevant from mem leak measurement out of 
        test, so it works as less as possible
    """
    lib: Library = arctic_library_lmdb
    df = generate_big_dataframe(200000)
    lib.write(symbol, df)
    del df
    yield lib


@pytest.mark.limit_leaks(location_limit="30 KB", filter_fn=is_relevant)
def test_mem_leak_read_all_arctic_lib_memray(library_with_big_symbol):
    """
        This is a new version of the initial test that reads the whole
        big dataframe in memory
    """

    print("Test starting")
    st = time.time()
    lib: Library = library_with_big_symbol
    data = lib.read(symbol).data
    del data
    print("Test took :", time.time() - st)

    print("Sleeping for 10 secs")
    gc.collect()
    time.sleep(10)



