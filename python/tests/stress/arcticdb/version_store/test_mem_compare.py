import inspect
import logging
import time
import pandas as pd
import numpy as np
import memory_profiler
import pytest

from arcticdb.util.test import random_dates, random_integers, random_string
from tests.util.mark import REAL_S3_TESTS_MARK, SLOW_TESTS_MARK

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MemCompare")

def str_col(num, size):
    return [random_string(num) for _ in range(size)]

def create_in_memory_frame(size):
    """
    Dataframe based on user scenario
    """
    types = {
        "element_name object" : str_col(20, size),        
        "element_value" : np.arange(size, dtype=np.float64), 
        "element_unit" : str_col(10, size),
        "period_year" :  random_integers(size, np.int64), 
        "region" : str_col(10, size),
        "last_published_date" : random_dates(size),
        "model_snapshot_id" :  random_integers(size, np.int64), 
        "period" : str_col(20, size), 
        "observation_type" : str_col(10, size),
        "ric" : str_col(10, size),
        "dtype" : str_col(10, size),
    }
    return types

@pytest.fixture
def s3_library(real_s3_library):
    lib = real_s3_library
    yield lib
    lib

@REAL_S3_TESTS_MARK
@SLOW_TESTS_MARK
def test_read_write_memory_compare(lmdb_library, real_s3_library):
    '''
    Purpose of this test is to compare read and write operations memory
    efficieny over large dataframes with pandas and between storage.
        comparisons
            pandas_dataframe to read_lmdb
            pandas_dataframe to read_aws
            pandas_dataframe to write_lmdb
            pandas_dataframe to write_aws
            write_lmdb write_aws
            read_lmdb read_aws
    '''

    lmdb = lmdb_library
    s3 = real_s3_library
    size = 2000000
    logger.info(f"Creating dataframe with {size} rows")
    frame = create_in_memory_frame(size)
    symbol = "test_dataframe"
    df: pd.DataFrame = None

    errors = []
    mem_allocs = {}

    def get_caller_line():
        """
        This function provides caller's line and file.
        To be ued for soft assertion types of checks so that
        when error arises the actual line is kept but execution proceeds
        """
        current_frame = inspect.currentframe()
        caller_frame = current_frame.f_back.f_back # caller of the function is actually
        
        file_name = caller_frame.f_code.co_filename
        line_number = caller_frame.f_lineno

        return f"File: {file_name}:{line_number}"

    def check_effectiveness_of_2similar_operations(timesEff, 
                                                   arctic_function_1st_storage, 
                                                   arctic_function_2nd_storage):
        nonlocal errors
        nonlocal mem_allocs
        mem_threshold = timesEff * mem_allocs[arctic_function_1st_storage]
        func_mem = mem_allocs[arctic_function_2nd_storage]
        logger.info(f"We assume {arctic_function_1st_storage} is {timesEff} times more efficient than {arctic_function_2nd_storage}")
        logger.info(f"ACTUAL Efficiency factor is : {func_mem / mem_allocs[arctic_function_1st_storage]}")          
        if (mem_threshold < func_mem):
            err = f"Too big memory for {arctic_function_2nd_storage.__name__} [{func_mem}] "
            err += f" MB compared to calculated threshold  {mem_threshold} MB \n"
            err += f" [base was {mem_allocs[arctic_function_1st_storage]}],\n {get_caller_line()}\n\n"
            errors.append(err) 
            logger.error(err)
        else:
            logger.info(f"Check OK for: {arctic_function_2nd_storage.__name__}") 

    def create_dataframe():
        nonlocal df
        df = pd.DataFrame(frame)

    def mem_write_dataframe_arctic_lmdb():
        lmdb.write(symbol, df)

    def mem_read_dataframe_arctic_lmdb():
        lmdb.read(symbol)
    
    def mem_write_dataframe_arctic_aws_s3():
        s3.write(symbol, df)

    def mem_read_dataframe_arctic_aws_s3():
        s3.read(symbol)

    funcs = [create_dataframe, 
             mem_write_dataframe_arctic_lmdb, 
             mem_read_dataframe_arctic_lmdb, 
             mem_write_dataframe_arctic_aws_s3, 
             mem_read_dataframe_arctic_aws_s3]

    for func in funcs:
        logger.info(f"START: {func.__name__} ")
        st = time.time()
        mem_usage = memory_profiler.memory_usage(func, interval=0.1, max_iterations=1)
        logger.info(f"Time took: {time.time() - st} ")
        max_memory = max(mem_usage)
        mem_allocs[func] = max_memory
        logger.info(f"{func.__name__} {max_memory}MB")

    memory_usage_per_column = df.memory_usage(index=True,deep=True)
    total_memory_usage = memory_usage_per_column.sum()

    logger.info(f"REPORTED MEM USAGE: {total_memory_usage} .. NOTE: This is not reliable")        

    logger.info(f"{mem_allocs}")

    check_effectiveness_of_2similar_operations(2.4, create_dataframe,mem_write_dataframe_arctic_lmdb)
    check_effectiveness_of_2similar_operations(2.4, create_dataframe,mem_read_dataframe_arctic_lmdb)
    check_effectiveness_of_2similar_operations(2.4, create_dataframe,mem_write_dataframe_arctic_aws_s3)
    check_effectiveness_of_2similar_operations(2.4, create_dataframe,mem_read_dataframe_arctic_aws_s3)
    check_effectiveness_of_2similar_operations(1.4, mem_write_dataframe_arctic_lmdb, mem_write_dataframe_arctic_aws_s3)
    check_effectiveness_of_2similar_operations(1.4, mem_read_dataframe_arctic_lmdb, mem_read_dataframe_arctic_aws_s3)

    if len(errors) > 0:
        assert False, f"Errors {errors}"


