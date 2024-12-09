"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import sys
import time
import psutil
import gc

import pytest
import numpy as np
import pandas as pd
import pytest
import arcticdb as adb

from arcticdb.util.test import get_sample_dataframe
from arcticdb.version_store._store import NativeVersionStore


def nice_bytes_str(bytes):
    return f" {bytes / (1024 * 1024):.2f}MB/[{bytes}] "


def lets_collect_some_garbage(time_sec: int = 9):
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
        lets_collect_some_garbage()

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

        assert (
            max_total_mem_lost_threshold_bytes >= process_growth
        ), f"Memory of the process grew more than defined threshold: {nice_bytes_str(process_growth)} (specified: {nice_bytes_str(max_total_mem_lost_threshold_bytes)} )"
        assert (
            max_machine_memory_percentage >= mem_per
        ), f"Machine utilized more memory than specified threshold :{mem_per}% (specified {max_machine_memory_percentage}%)"

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

@pytest.mark.skipif(sys.platform == "win32", reason="Not enough storage on Windows runners")
@pytest.mark.skipif(sys.platform == "darwin", reason="Problem on MacOs")
def test_mem_leak_read_all_arctic_lib(arctic_library_lmdb):
    lib: adb.Library = arctic_library_lmdb

    df = get_sample_dataframe(size=1000000)
    df = grow_exp(df, 5)

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
    max_mem_bytes = 450_623_040

    check_process_memory_leaks(proc_to_examine, 25, max_mem_bytes, 80.0)

@pytest.mark.skipif(sys.platform == "darwin", reason="Problem on MacOs")
def test_mem_leak_read_all_native_store(lmdb_version_store_very_big_map):
    lib: NativeVersionStore = lmdb_version_store_very_big_map

    df = get_sample_dataframe(size=1000000)
    df = grow_exp(df, 5)

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
