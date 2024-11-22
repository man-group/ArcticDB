"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import copy
import gc
import time
import numpy as np
import pandas as pd
from typing import Union, List
from arcticdb.util.test import create_datetime_index, get_sample_dataframe, random_integers, random_string
from arcticdb.version_store.library import Library, StagedDataFinalizeMethod
from arcticdb.config import Defaults, set_log_level
from arcticdb.util.utils import TimestampNumber

from typing import Union
import numpy as np
import pandas as pd

# Uncomment for logging
# set_log_level(default_level="DEBUG", console_output=False, file_output_path="/tmp/arcticdb.log")
    
class CachedDFGenerator:
    """
        Provides ability to generate dataframes based on sampling a larger
        pregenerated dataframe
    """

    TIME_UNIT='s'

    def __init__(self, max_size:int=1500000, size_string_flds_array=[25,1,5,56]):
        """
            Define the number of rows for the cached dataframe
        """
        self.__cached_xlarge_dataframe:pd.DataFrame = None
        self.max_size = max_size
        self.size_string_flds_array = size_string_flds_array

    def generate_dataframe(self, num_rows:int) -> pd.DataFrame:
        """
            Generate a dataframe having specified number of rows sampling the 
            cached dataframe
        """
        assert num_rows < self.max_size
        if (self.__cached_xlarge_dataframe is None):
            print(">>>> INITIAL PREPARATION OF LARGE DF")
            self.__cached_xlarge_dataframe = self.generate_xLarge_samples_dataframe(
                num_rows=self.max_size, size_string_flds_array=self.size_string_flds_array) 
            print(">>>> COMPLETED")
        else:
            print(">>>> Use cached DF for sampling")
        return self.__cached_xlarge_dataframe.sample(n=num_rows,axis=0)

    def generate_dataframe_timestamp_indexed(self, rows:int, start_time:Union[int, TimestampNumber]=0, freq:str=TIME_UNIT ) -> pd.DataFrame:
        """
            Generates dataframe taking random number of 'rows' of the cached large
            dataframe. Adds timestamp index starting at start_time and having a frequency
            specified either by the TimeStampNumber or 'freq' parameter when start time is 
            integer
        """
        df = self.generate_dataframe(rows)
        if (isinstance(start_time, TimestampNumber)):
            freq = start_time.get_type()
            start_time = start_time.get_value()
        start_timestamp, *other = TimestampNumber.calculate_timestamp_after_n_periods(
            periods=start_time,
            freq=freq,
            start_time=TimestampNumber.TIME_ZERO)
        create_datetime_index(df, "timestamp", "s", start_timestamp)
        return df
    
    @classmethod
    def generate_xLarge_samples_dataframe(cls, num_rows:int, size_string_flds_array:List[int] = [10]) -> pd.DataFrame:
        """
            Generates large dataframe by concatenating several time different DFs with same schema to the right. As the
            method that generates dataframe with all supported column types is used this means that the result dataframe should
            cover all cases that we have for serialization

            'num_rows' - how many rows the dataframe should have
            'size_string_flds_array' - this array contains sizes of the string fields in each dataframe. The total number
                  of elements in the list will give how many times the sample dataframe will be generated and thus the 
                  result dataframe will have that many times the number of column of the original sample dataframe
        """
        df = None
        cnt = 0
        iterc = len(size_string_flds_array)
        print(f"Creating xLarge DF in {iterc} iterations")
        for str_size in size_string_flds_array:
            _df = get_sample_dataframe(size=num_rows, seed=str_size, str_size=str_size)
            cls.dataframe_add_suffix_to_column_name(_df, f"-{cnt}")
            print(f"DF of iteration {cnt} completed with {num_rows} rows")
            if (df is None):
                df = _df
            else:
                df = pd.concat([df,_df], axis=1)
                print(f"Concatenation if DF of iteration {cnt} completed. Result is DF with {len(df.columns.array)}")
            cnt = cnt + 1
        return df
    
    @classmethod
    def dataframe_add_suffix_to_column_name(cls, df: pd.DataFrame, suffix: str):
        """
            If we want to grow dataframe by adding once again a dataframe having same schema
            abd number of rows on the right effectively extending the number of columns
            we have to prepare the dataframes in such way that their columns have unique 
            names. This can happen by adding an id to each of the columns as suffix
        """            
        df_cols = df.columns.to_list()
        for col in df_cols:
            df.rename( {col : col + suffix}, axis='columns',inplace=True)



#####################################################################################################
#####################################################################################################


def generate_chunk_sizes(number_chunks:np.uint32, min_rows:np.uint32=100, max_rows:np.uint32=10000) -> List[np.uint32]:
    return np.random.randint(min_rows, max_rows, number_chunks, dtype=np.uint32)

def stage_chunks(lib: Library, symbol:str, cachedDF:CachedDFGenerator, start_index:TimestampNumber, 
                 array_chunk_number_rows:List[np.uint32], reverse_order:bool=False) -> pd.DataFrame:
    
    """
        Stages dataframes to specified symbol in specified library. Will use a cached dataframe to obtain as fast as possible
        random dataframes. They will be added in ascending or descending (reversed) order of the timestamps indexes based on 
        reverse_order value
    """
    total = start_index.get_value()
    num_rows_staged:int = 0
    iter:int = 1
    size = len(array_chunk_number_rows)
    total_rows_to_stage = sum(array_chunk_number_rows)
    final_index = total_rows_to_stage + total
    for chunk_size in array_chunk_number_rows:
        if (reverse_order):
            # In this case we start from the end of datetime range
            # And generate first the chunks with latest date time, then previous etc
            # in other words we will reverse the order of chunks creating the worst case scenario
            chunk_start_index = final_index - (chunk_size + num_rows_staged)
            df = cachedDF.generate_dataframe_timestamp_indexed(chunk_size, chunk_start_index, cachedDF.TIME_UNIT)
        else:
            df = cachedDF.generate_dataframe_timestamp_indexed(chunk_size, total, cachedDF.TIME_UNIT)
        lib.write(symbol, data=df, validate_index=True, staged=True)
        print()
        print(f"Staging iteration {iter} / {size}")
        print(f"Staged DataFrame has {df.shape[0]} rows {len(df.columns.to_list())} cols")
        print(f"Total number of rows staged {num_rows_staged}")
        num_rows_staged = num_rows_staged + chunk_size
        iter= iter + 1
        total = total + chunk_size

def test_finalize_monotonic_unique_chunks(arctic_library_lmdb):

    options = [
        {"chunks_descending" : False, "finalization_mode" : StagedDataFinalizeMethod.APPEND},
        {"chunks_descending" : True, "finalization_mode" : StagedDataFinalizeMethod.APPEND},
        {"chunks_descending" : False, "finalization_mode" : StagedDataFinalizeMethod.WRITE},
        {"chunks_descending" : True, "finalization_mode" : StagedDataFinalizeMethod.WRITE},
        ]

    # Will hold the results after each iteration:
    #  - iteration chunks
    #  - chunks staged for finalization
    #  - rows finalized for iteration 
    #  - time for finalization
    results = []

    lib : Library = arctic_library_lmdb

    # We would need to generate as fast as possible kind of random
    # dataframes. To do that we build a large cache and will 
    # sample rows from there as we need to run as fast as we can
    cachedDF = CachedDFGenerator(250000)

    total_number_rows_all_iterations: int = 0

    # This will serve us as a counter and at the same time it provides unique index for each row
    total_number_rows: TimestampNumber = TimestampNumber(0, cachedDF.TIME_UNIT) # Synchronize index frequency
    INITIAL_TIMESTAMP: TimestampNumber = TimestampNumber(0, cachedDF.TIME_UNIT) # Synchronize index frequency
    symbol="staged"

    num_rows_initially = 99999
    print(f"Writing to symbol initially {num_rows_initially} rows")
    df = cachedDF.generate_dataframe_timestamp_indexed(num_rows_initially, total_number_rows, cachedDF.TIME_UNIT)

    cnt = 0
    res = {}
    for iter in [1000, 1000, 1000, 1000 ,5000, 5000, 5000, 5000, 10000, 10000, 10000, 10000] :

        total_number_rows = INITIAL_TIMESTAMP + num_rows_initially
        lib.write(symbol, data=df, prune_previous_versions=True)

        print(f"Start staging chunks .... for iter {cnt} with {iter} chunks")
        print(f"Using options {options[cnt % 4]}")
        chunk_list = generate_chunk_sizes(iter, 9000, 11000)
        gc.collect()
        print(f"Chunks to stage {len(chunk_list)} ")
        stage_chunks(lib, symbol, cachedDF, total_number_rows, chunk_list, options[cnt % 4]["chunks_descending"])

        if (options[cnt % 4]["finalization_mode"] == StagedDataFinalizeMethod.APPEND):
            total_number_rows = total_number_rows + sum(chunk_list)
        else:
            total_number_rows = INITIAL_TIMESTAMP + sum(chunk_list)


        print("--" * 50)
        print(f"STAGED ROWS {total_number_rows.get_value()} after iteration {cnt}")
        print(f"SYMBOL ACTUAL ROWS before finalization - {lib._nvs.get_num_rows(symbol)} ")
        start_time = time.time()
        lib.finalize_staged_data(symbol=symbol, mode=options[cnt % 4]["finalization_mode"])
        finalization_time = time.time() - start_time
        gc.collect()
        print(f"SYMBOL ACTUAL ROWS after finalization {lib._nvs.get_num_rows(symbol)} ")
        print("--" * 50)

        assert total_number_rows == lib._nvs.get_num_rows(symbol)
        cnt = cnt + 1

        total_number_rows_all_iterations = total_number_rows_all_iterations + total_number_rows
        print(f"TOTAL ROWS INSERTED IN ALL ITERATIONS: {total_number_rows_all_iterations}")

        res["options"] = options[cnt % 4]
        res["iteration"] = cnt
        res["number_staged_chunks"] = iter
        res["total_rows_finalized"] = total_number_rows_all_iterations
        res["finalization time"] = finalization_time

        results.append(res)

        total_number_rows.to_zero() # next iteration start from 0

    for res in results:
        print("_" * 100)
        print(res)
