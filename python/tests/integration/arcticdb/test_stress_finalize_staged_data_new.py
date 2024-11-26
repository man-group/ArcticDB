import gc
import itertools
import multiprocessing
import os
import time
import numpy as np
import pandas as pd
from typing import Iterator, Union, List, Optional, Literal
import psutil
import pytest
from arcticdb import Arctic
from arcticdb.encoding_version import EncodingVersion
from arcticdb.storage_fixtures.api import StorageFixture
from arcticdb.storage_fixtures.lmdb import LmdbStorageFixture
from arcticdb.util.test import random_integers, random_string
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.version_store.library import Library, StagedDataFinalizeMethod


class TimestampUtils:

    SupportedFreqTypes = Literal['s','m','h']

    TIME_ZERO : pd.Timestamp = pd.Timestamp(0)
    
    @classmethod
    def calculate_timestamp_after_n_periods(cls, periods:int, freq:SupportedFreqTypes='s', 
            start_time: pd.Timestamp = TIME_ZERO) -> tuple[pd.Timestamp, pd.Timestamp]:
        """ 
            Calculates end timestamp, based on supplied start timestamp, by adding specified
            number of time periods denoted by 'freq' parameter ('s' - seconds, 'm' - minutes, 'h' - hours)
            If periods is negative the end timestamp will be prior to start timestamp

            returns first calculated timestamp and then sorted by time tuple of start time and end time
        """
        add=True
        if (periods < 0):
            periods:int = -periods
            add=False
        
        if (freq == 's'):
            if(add):
                end_time = start_time + pd.Timedelta(seconds=periods) 
            else:
                end_time = start_time - pd.Timedelta(seconds=periods) 
        elif  (freq == 'm'):     
            if(add):
                end_time = start_time + pd.Timedelta(minute=periods) 
            else:
                end_time = start_time - pd.Timedelta(minute=periods) 
        elif (freq == 'h'):        
            if(add):
                end_time = start_time + pd.Timedelta(hours=periods) 
            else:
                end_time = start_time - pd.Timedelta(hours=periods) 
        else:
            raise Exception("Not supported frequency")

        if (add):
            return end_time, (start_time, end_time)
        else:
            return end_time, end_time , start_time

    @classmethod
    def calculate_timestamp_since_timestamp_zero(cls, time_period: np.int64, freq:SupportedFreqTypes='s') -> pd.Timestamp: 
        time = cls.calculate_timestamp_after_n_periods(periods=time_period,freq=freq,start_time=cls.TIME_ZERO)
        return time
    
    @classmethod
    def calculate_range_since_timestamp_zero(cls, start_time: np.int64, end_time:np.int64, 
            freq:Literal['s','m','h']='s') -> tuple[pd.Timestamp, pd.Timestamp]:
        start_timestamp = cls.calculate_timestamp_since_timestamp_zero(start_time, freq)    
        end_timestamp = cls.calculate_timestamp_since_timestamp_zero(end_time, freq)    
        return start_timestamp, end_timestamp

def get_sample_dataframe(size=1000, str_size=10):
    df = pd.DataFrame(
        {
            "uint8": random_integers(size, np.uint8),
            "strings": [random_string(str_size) for _ in range(size)],
            "uint16": random_integers(size, np.uint16),
            "uint32": random_integers(size, np.uint32),
            "uint64": random_integers(size, np.uint64),
            "int8": random_integers(size, np.int8),
            "int16": random_integers(size, np.int16),
            "int32": random_integers(size, np.int32),
            "int64": random_integers(size, np.int64),
            "float32": np.random.randn(size).astype(np.float32),
            "float64": np.arange(size, dtype=np.float64),
            "bool": np.random.randn(size) > 0,
        }
    )
    return df

class DFUtils:

    TIME_UNIT='s'

    @classmethod
    def dataframe_add_suffix_to_column_name(cls, df: pd.DataFrame, suffix: str):
            df_cols = df.columns.to_list()
            for col in df_cols:
                df.rename( {col : col + suffix}, axis='columns',inplace=True)

    @classmethod
    def add_datetime_index(cls, df: pd.DataFrame, name_col:str, freq:TimestampUtils.SupportedFreqTypes, 
            start_time: pd.Timestamp = TimestampUtils.TIME_ZERO):
        periods = len(df)
        index = pd.date_range(start=start_time, periods=periods, freq=freq, name=name_col)
        df.index = index

    @classmethod
    def generate_xLarge_samples_dataframe(cls, num_rows:int, size_string_flds_array:List[int] = [10], seed=0) -> pd.DataFrame:
        np.random.seed(seed)
        df = None
        cnt = 0
        iterc = len(size_string_flds_array)
        print(f"Creating xLarge DF in {iterc} iterations")
        for str_size in size_string_flds_array:
            _df = get_sample_dataframe(size=num_rows, str_size=str_size)
            DFUtils.dataframe_add_suffix_to_column_name(_df, f"-{cnt}")
            print(f"DF of iteration {cnt} completed with {num_rows} rows")
            if (df is None):
                df = _df
            else:
                df = pd.concat([df,_df], axis=1)
                print(f"Concatenation of DF of iteration {cnt} completed. Result is DF with {len(df.columns.array)} cols")
            cnt = cnt + 1
        return df

class CachedDFGenerator:
    """
        Provides ability to generate dataframes based on sampling a larger
        pregenerated dataframe
    """
    TIME_UNIT='s'

    def __init__(self, max_size:int=1500000):
        """
            Define the number of rows for the cached dataframe
        """
        self.__cached_xlarge_dataframe:pd.DataFrame = None
        self.max_size = max_size

    def generate_dataframe(self, num_rows:int) -> pd.DataFrame:
        """
            Generate a dataframe having specified number of rows sampling the 
            cached dataframe
        """
        assert num_rows < self.max_size
        if (self.__cached_xlarge_dataframe is None):
            print(">>>> INITIAL PREPARATION OF LARGE DF")
            self.__cached_xlarge_dataframe = DFUtils.generate_xLarge_samples_dataframe(
                num_rows=self.max_size, size_string_flds_array=[25,1,5,56]) 
            print(">>>> COMPLETED")
        return self.__cached_xlarge_dataframe.sample(n=num_rows,axis=0)

    def generate_dataframe_timestamp_indexed(self, rows:int, start_time:int=0, freq:str=TIME_UNIT ) -> pd.DataFrame:
        df = self.generate_dataframe(rows)
        start_timestamp, *other = TimestampUtils.calculate_timestamp_after_n_periods(
            periods=start_time,
            freq=CachedDFGenerator.TIME_UNIT,
            start_time=TimestampUtils.TIME_ZERO)
        DFUtils.add_datetime_index(df, "timestamp", "s", start_timestamp)
        return df


#####################################################################################################
#####################################################################################################

@pytest.fixture
def arctic_client_lmdb_v1(lmdb_storage):
    storage_fixture: LmdbStorageFixture = lmdb_storage
    storage_fixture.arctic_uri = storage_fixture.arctic_uri + "?map_size=100GB"
    ac = storage_fixture.create_arctic(encoding_version=EncodingVersion.V1)
    assert not ac.list_libraries()
    return ac

@pytest.fixture
def arctic_library_lmdb(arctic_client_lmdb_v1, lib_name):
    return arctic_client_lmdb_v1.create_library(lib_name)

####################################################

def generate_chunk_sizes(number_chunks:np.uint32, min_rows:np.uint32=100, max_rows:np.uint32=10000) -> List[np.uint32]:
    return np.random.randint(min_rows, max_rows, number_chunks, dtype=np.uint32)


cashedDF = CachedDFGenerator()


stages_counter = multiprocessing.Value("i", 0)


def stage_chunk(lib, symbol, chunk_size, start_time):
    global stages_counter
    with stages_counter.get_lock():
        stages_counter.value += 1
        if stages_counter.value % 100 == 0:
            print(f"Staging chunk {stages_counter.value}")
    df = cashedDF.generate_dataframe_timestamp_indexed(chunk_size, start_time, cashedDF.TIME_UNIT)
    lib.write(symbol, data=df, validate_index=True, staged=True)


def stage_chunks(lib, symbol, chunk_sizes, start_times):
    inp = zip(itertools.repeat(lib), itertools.repeat(symbol), chunk_sizes, start_times)
    with multiprocessing.Pool(10) as p:
        p.starmap(stage_chunk, inp)


#ac = Arctic("lmdb:///home/alex/big_disk/part/lmdb_one?map_size=100GB")
ac = Arctic("s3://172.17.0.2:9000:aseaton?access=3SePAqKdc1O7JgeDIJob&secret=zhtHzQtQt7UZJVUHk3QtpShSeRYZozwEl0pVeq8A")
lib = ac.get_library("tst", create_if_missing=True)
lib._nvs.version_store.clear()


def test_finalize_monotonic_unique_chucks():

    #lib : Library = arctic_library_lmdb

    symbol="staged"

    num_rows_initially = 99999
    print(f"Writing to symbol initially {num_rows_initially} rows")
    df = cashedDF.generate_dataframe_timestamp_indexed(num_rows_initially, 0, cashedDF.TIME_UNIT)
    lib.write(symbol, data=df, prune_previous_versions=True)

    #for iter in [18000]:#, 20000, 220000] :
    for iter in [1000]:#, 20000, 220000] :
        chunk_list = generate_chunk_sizes(iter, 9000, 11000)
        gc.collect()
        print(f"Chunks to stage {len(chunk_list)} ")
        start_times = np.cumsum(chunk_list)
        start_times = np.roll(start_times, 1)
        start_times[0] = 0
        start_times += num_rows_initially
        print(f"chunk_list={chunk_list} start_times={start_times}")
        stage_chunks(lib, symbol, chunk_list, start_times)

        print("--" * 50)
        print(f"SYMBOL ACTUAL ROWS before finalization - {lib._nvs.get_num_rows(symbol)} ")
        # lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)
        # gc.collect()
        # print(f"SYMBOL ACTUAL ROWS after finalization {lib._nvs.get_num_rows(symbol)} ")
        # print("--" * 50)
        #
        # lib.write(symbol, data=df, prune_previous_versions=True)


"""
df1 = generate_xLarge_samples_dataframe(10000,[1,2,5,15,76,512])
size = len(df1)
dataframe_add_datetime_index(df1, "timestampe", "s")
print(df1)
df1.info()


print(calculate_timestamp_after_n_periods(-1,"h"))
print(calculate_timestamp_after_n_periods(0,"h"))
print(calculate_timestamp_after_n_periods(1,"h"))

generate_chunks([2,1,3,2])
"""