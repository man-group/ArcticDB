"""
Copyright 2024 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import List, Tuple, Union
import sys 
if sys.version_info >= (3, 8): 
    from typing import Literal 
else: 
    from typing_extensions import Literal
import numpy as np
import pandas as pd

from arcticdb.util.test import create_datetime_index, get_sample_dataframe
from arcticdb.version_store.library import Library

class  TimestampNumber:
    """
        Represents the timestamp as a typed number (can be seconds, minutes, hours).
        This allows considering timestamp index of type "s" or "m" or "h" as a autoincrement
        integer of specified type (original Timestamp is based on nanoseconds), 
        That further allowing simple arithmetic with numbers - adding and subtracting
        specified number of same type units results in increasing or decreasing the timestamp with same
        amount of time that type/freq represents. 

        In other words any numbers added, subtracted or compared with this type
        are implicitly considered as instances of the same type seconds, minutes, hours
        and operations are carried on naturally.

        Supported are int operation for increment and decrement

        For comparison you can do it with any number or Timestamp object

        0 is Timestamp(0) etc
    """
    SupportedFreqTypes = Literal['s','m','h']
    
    DEFAULT_FREQ:SupportedFreqTypes = 's'

    TIME_ZERO : pd.Timestamp = pd.Timestamp(0)

    def __init__(self, value:np.int64, type:SupportedFreqTypes=DEFAULT_FREQ) -> None: 
        self.init_value:np.int64 = value
        self.value:np.int64 = value
        self.__type:TimestampNumber.SupportedFreqTypes = type


    def get_type(self) -> SupportedFreqTypes:
        return self.__type


    def get_value(self) -> np.int64:
        """
            Returns the value as a number of specified units since Timestamp(0)
        """
        return self.value


    def to_timestamp(self) -> pd.Timestamp:
        result, *other = self.calculate_timestamp_after_n_periods(self.value, self.__type)
        return result


    def inc(self, add_number:np.int64) -> 'TimestampNumber':
        self.value = np.int64(self.value) + np.int64(add_number)
        return self


    def dec(self, add_number:np.int64) -> 'TimestampNumber':
        self.value = np.int64(self.value) - np.int64(add_number)
        return self


    def to_zero(self) -> 'TimestampNumber':
        '''
            To Timestamp(0)
        '''
        self.value = 0
        return self
    

    def to_initial_value(self) -> 'TimestampNumber':
        '''
            Revert to initial value
        '''
        self.value = self.init_value
        return self


    def get_initial_value(self) -> 'np.int64':
        '''
            Returns the initial value of the number.
            This allows you to serve like a reference
        '''
        return self.init_value

    
    @classmethod
    def calculate_timestamp_after_n_periods(cls, periods:int, freq:SupportedFreqTypes='s', 
            start_time: pd.Timestamp = TIME_ZERO) -> Tuple[pd.Timestamp, Tuple[pd.Timestamp, pd.Timestamp]]:
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
            return (end_time, (start_time, end_time))
        else:
            return (end_time, (end_time , start_time))


    @classmethod
    def from_timestamp(cls, timestamp:pd.Timestamp, freq:SupportedFreqTypes=DEFAULT_FREQ) -> 'TimestampNumber':
        """
            Creates object from Timestamp, but will round the the internal 
            value to the floor of the specified type. For instance if time
            on the time stamp was 13:45:22 and specified freq is 'h' the resulting object 
            will be 13:00:00 if converted back to timestamp (larger time units will not be touched)

            In other words the resulting object will not be equal to
        """
        if (freq == 's'):
            return TimestampNumber(timestamp.value // 1000000000, 's')
        if (freq == 'm'):
            return TimestampNumber(timestamp.value // (1000000000*60), 'm')
        if (freq == 'h'):
            return TimestampNumber(timestamp.value // (1000000000*60*60), 'h')
        raise NotImplemented(f"Not supported param {freq}. Supported are {TimestampNumber.SupportedFreqTypes}")
    
    def __radd__(self, other):
        return self.value + other

    def __rsub__(self, other):
        return other - self.value  
    
    def __lt__(self, other) -> 'TimestampNumber':
        if (isinstance(other, pd.Timestamp)):
            return self.to_timestamp() < other
        if (isinstance(other, np.int64) or isinstance(other, np.uint64) or isinstance(other, int) or isinstance(other, float)) :
            return self.value < other
        else:
            raise NotImplemented("Only supports operations with integers and floats")
    
    def __eq__(self, other) -> 'TimestampNumber':
        if (isinstance(other, pd.Timestamp)):
            return self.to_timestamp() == other
        if (isinstance(other, np.int64) or isinstance(other, np.uint64) or isinstance(other, int) or isinstance(other, float)) :
            return self.value == other
        else:
            raise NotImplemented("Only supports operations with integers and floats")

    def __gt__(self, other) -> 'TimestampNumber':
        if (isinstance(other, pd.Timestamp)):
            return self.to_timestamp() > other
        if (isinstance(other, np.int64) or isinstance(other, np.uint64) or isinstance(other, int) or isinstance(other, float)) :
            return self.value > other
        else:
            raise NotImplemented("Only supports operations with integers and floats")

    def __add__(self, other) -> 'TimestampNumber':
        copy = TimestampNumber(self.value, self.__type)
        copy.inc(other)
        return copy
        
    def __sub__(self, other) -> 'TimestampNumber':
        copy = TimestampNumber(self.value, self.__type)
        copy.dec(other)
        return copy
    
    def __repr__(self):
        return f"TimestampTyped('{self.value} {self.__type}', '{str(self.to_timestamp())}')"

    def __str__(self):
        return str(self.to_timestamp())


class CachedDFGenerator:
    """
        Provides ability to generate dataframes based on sampling a larger
        pregenerated dataframe
    """

    TIME_UNIT='s'

    def __init__(self, max_size:int=1500000, size_string_flds_array=[25,1,5,56]):
        """
            Define the number of rows for the cached dataframe through 'max_size'
            'size_string_flds_array' pass an array of sizes of the string columns
            in the dataframe. The length of the array will define how many times 
            a DF produced generate_sample_dataframe() will be invoked and the resulting
            X number of dataframes stitched together on the right of first will
            produce the XLarge dataframe
        """
        self.__cached_xlarge_dataframe:pd.DataFrame = None
        self.max_size = max_size
        self.size_string_flds_array = size_string_flds_array

    def get_dataframe(self) -> pd.DataFrame: 
        assert self.__cached_xlarge_dataframe, "invoke generate_dataframe() first"
        return self.__cached_xlarge_dataframe

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

