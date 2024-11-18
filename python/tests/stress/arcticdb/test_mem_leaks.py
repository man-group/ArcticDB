"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
import psutil
import gc
import os
import numpy as np
import pandas as pd
import pytest
import arcticdb as adb
from pympler import muppy, summary

from arcticdb.util.test import get_sample_dataframe
from arcticdb.version_store._store import NativeVersionStore


def get_process_by_id(id: int | None) -> psutil.Process:
    """
        Returns the id of a process. Call with 'os.getpid()' for current python process
    """
    for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
        if id == proc.pid :
            return proc
    return None

def nice_bytes_str(bytes) :
   return f" {bytes / (1024 * 1024):.2f}MB/[{bytes}] "

def lets_collect_some_garbage(time_sec:int = 7):
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
        if numbers[i] < percentage_drop_0_1 * numbers[i - 1]: count += 1 
    return count    


def check_process_memory_leaks(process_func , number_iterrations, max_total_mem_lost_treshold, max_machine_memory_percentage) -> np.int64:
    """
        This check accepts a function which will be called iterrativly 'number_iterrations'. During this 
        process the procedure will monitor RSS growth of python process in order to be below specified
        threshhold 'max_total_mem_lost_treshold' AND and the same time will watch so that machine
        utilization of memory is not above certain percentage 'max_machine_memory_percentage' to prevent 
        crash of the process

        Specify more than 3 iterations (advised min 30), and be sure that there will be some initial growth in the first 1-2 
        iterrations, thus your expected growth threshold should be above that

        IMPORTANT: It is strongly advised during creation of future tests to they are executed first 
        through command line to determine the optimal iteration  and 'max_total_mem_lost_treshold' for the
        python process. During the execution you will see lots of information on the screen including maximum growth
        of memory since iterations started as weel as minumum growth of memory. That along with information
        how many times the growth of mem droped 50% could provide insight for presence or lack of memory issues

        NOTE: This test is NOT CONCLUSIVE for absence of memory leacks. It would catch LARGE leaks. 
        Small leaks will not be caught. If the procedure fails to sport problems it will return 2 numbers
        for further investigation by the test:
         - how much the process has grown during execution in bytes
         - how many times the process memory dropped more than 50% (indicating some C++ garbage collection)

        NOTE 2: Currently this code controlls python GC only.

    """
    p = get_process_by_id(os.getpid())
    avail_mem_start : np.int64 = psutil.virtual_memory().available
    print ("-" * 80)
    print ("Start check process for memory leaks")
    print ("Num iterrations: ", number_iterrations)
    print ("Maxumum memory growth/lost to the examined process: ", max_total_mem_lost_treshold)
    print ("Maxumum machine memory utilization: ", max_total_mem_lost_treshold)
    
    lets_collect_some_garbage(10)

    mem_per : np.int64 = psutil.virtual_memory().percent
    assert max_machine_memory_percentage >- mem_per , "Machine has less memory available the check cannot start"    
    print ("Available memory" , nice_bytes_str(avail_mem_start))
    print ("-" * 80)
    process_initial_memory : np.int64 = p.memory_info().rss
    print ("Process initial RSS", nice_bytes_str(process_initial_memory))

    mem_growth_each_iter = list()

    for n in range(number_iterrations):
        p_iter_mem_start : np.int64  = p.memory_info().rss
        
        print("Starting watched code ...........")
        process_func()
        lets_collect_some_garbage()

        p_iter_mem_end : np.int64  = p.memory_info().rss
        process_growth : np.int64 = p.memory_info().rss - process_initial_memory
        mem_growth_each_iter.append(process_growth)
        process__avg_growth = process_growth / (n + 1)
        p_iter_mem_notcleaned : np.int64  = p_iter_mem_end - p_iter_mem_start
        mem_avail : np.int64 = psutil.virtual_memory().available
        mem_per : np.int64 = psutil.virtual_memory().percent
        print (f"Iter No[{n}]    : Process did added (or if - number means cleaned) {nice_bytes_str(p_iter_mem_notcleaned)}.  Avail memory: {nice_bytes_str(mem_avail)} Used Mem: {mem_per}%")
        print (f"  Overall stats : Process growth since start {nice_bytes_str(process_growth)} AVG growth per iter {nice_bytes_str(process__avg_growth)}")
        print (f"  Minimum growth so far: {nice_bytes_str(min(mem_growth_each_iter))}")
        print (f"  Maximum growth so far: {nice_bytes_str(max(mem_growth_each_iter))}")
        print (f"  Number of times there was 50% drop in memory: {count_drops(mem_growth_each_iter, 0.5)}")

        assert max_total_mem_lost_treshold >= process_growth , f"Memory of the process grew more than defined threshold: {nice_bytes_str(process_growth)} (specified: {nice_bytes_str(max_total_mem_lost_treshold)} )"
        assert max_machine_memory_percentage >= mem_per , f"Machine utilized more memory than specified treshold :{mem_per}% (specified {max_machine_memory_percentage}%)"
        
    print ("The process assesment finished within expectations. Total consumed additionaml mem is bellow threshold: ", process_growth)
    print ("Returning this number as result.")
    return process_growth, count_drops(mem_growth_each_iter, 0.5)

def grow_exp(df_to_grow : pd.DataFrame, num_times_xx2:int):
    """
        Quickly inflates the dataframe by doubling its size for each iterration
        Thus the final size will be 'num_times_xx2' power of two
    """
    for n in range(1,num_times_xx2):
        df_prev = df_to_grow.copy(deep=True)
        df_to_grow = pd.concat([df_to_grow, df_prev])
    return df_to_grow

@pytest.mark.xfail("ArcticDBPR#1998", reason = "String memory leaks while read")
def test_mem_leak_read_all_arctic_lib(arctic_library_lmdb):
    lib : adb.Library = arctic_library_lmdb

    df = get_sample_dataframe(size=1000000)
    df = grow_exp(df, 5)

    symbol = "test"
    lib.write(symbol, df)

    def proc_to_examine():
        data = lib.read(symbol).data
        del data

    df = lib.read(symbol).data
    print(df)
    del df

    check_process_memory_leaks(proc_to_examine, 30, 4808662528, 80.0)

def test_mem_leak_read_all_native_store(lmdb_version_store_very_big_map):
    lib : NativeVersionStore = lmdb_version_store_very_big_map

    df = get_sample_dataframe(size=1000000)
    df = grow_exp(df, 5)

    symbol = "test"
    lib.write(symbol, df)

    def proc_to_examine():
        data = lib.read(symbol).data
        del data

    df = lib.read(symbol).data
    print(df)
    del df

    check_process_memory_leaks(proc_to_examine, 30, 808662528, 80.0)
