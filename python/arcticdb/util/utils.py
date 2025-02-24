"""
Copyright 2024 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from datetime import timedelta
import random
import string
import time
import sys 
if sys.version_info >= (3, 8): 
    from typing import Literal, Any, List, Tuple, Union, get_args
else: 
    from typing_extensions import Literal
    from typing import Any, List, Tuple, Union
import numpy as np
import pandas as pd

from arcticdb.util.test import create_datetime_index, get_sample_dataframe, random_integers, random_string
from arcticdb.version_store.library import Library

# Types supported by arctic
ArcticIntType = Union[np.uint8, np.uint16, np.uint32, np.uint64, np.int8, np.int16, np.int32, np.int64]
ArcticFloatType = Union[np.float64, np.float32]
ArcticTypes = Union[ArcticIntType, ArcticFloatType, str]

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

    def generate_dataframe(self, num_rows:int, verbose=False) -> pd.DataFrame:
        """
            Generate a dataframe having specified number of rows sampling the 
            cached dataframe
        """
        assert num_rows < self.max_size
        if (self.__cached_xlarge_dataframe is None):
            if verbose:
                print(">>>> INITIAL PREPARATION OF LARGE DF")
            self.__cached_xlarge_dataframe = self.generate_xLarge_samples_dataframe(
                num_rows=self.max_size, size_string_flds_array=self.size_string_flds_array) 
            if verbose:
                print(">>>> COMPLETED")
        else:
            if verbose:
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
                 array_chunk_number_rows:List[np.uint32], reverse_order:bool=False, verbose: bool = False) -> pd.DataFrame:
    
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
    print(f"Start staging {size} chunks")
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
        if verbose:
            print()
            print(f"Staging iteration {iter} / {size}")
            print(f"Staged DataFrame has {df.shape[0]} rows {len(df.columns.to_list())} cols")
            print(f"Total number of rows staged {num_rows_staged}")
        num_rows_staged = num_rows_staged + chunk_size
        iter= iter + 1
        total = total + chunk_size
    print(f"End staging {size} chunks")


class RandomStringPool:
    """
    A class that will return a list of randomly selected strings from a string pool
    with certain size of each string and limited number of strings in the pool
    """

    def __init__(self, str_length: int, pool_size: int):
        self.__pool = ListGenerators.generate_random_string_pool(str_length, pool_size)

    def get_list(self, size: int) -> List[str]:
        return [random.choice(self.__pool) for _ in range(size)]


class ListGenerators:
    """
    Specialized class of utility functions for creating random lists of various type
    """

    @classmethod
    def generate_random_floats(cls, dtype: ArcticFloatType, 
                      size: int, min_value: float = None, max_value: float = None, round_to: int = None,
                      seed = 1) -> List[ArcticFloatType]:
        # Higher numbers will trigger overflow in numpy uniform (-1e307 - 1e307)
        # Get the minimum and maximum values for np.float32
        info = np.finfo(np.float32)
        _max = info.max
        _min = info.min
        np.random.seed(seed)
        if min_value is None:
            min_value = max(-1e307, -sys.float_info.max, _min)
        if max_value is None:    
            max_value = min(1e307, sys.float_info.max, _max)
        if round_to is None:
            return np.random.uniform(min_value, max_value, size).astype(dtype)
        else :
            return np.round(np.random.uniform(min_value, max_value, size), round_to).astype(dtype)
        
    @classmethod
    def generate_random_string_pool(cls, str_length: int, pool_size: int, seed = 1) -> List[str]:
        np.random.seed(seed)
        unique_values = set()
        while len(unique_values) < pool_size:
            unique_values.add(ListGenerators.random_string(str_length))
        return list(unique_values)

    @classmethod
    def generate_random_strings(cls, str_size: int, length: int, seed = 1) -> List[str]:
        np.random.seed(seed)
        return [ListGenerators.random_string(str_size) for _ in range(length)]
    
    @classmethod
    def generate_random_ints(cls, dtype: ArcticIntType, 
                            size: int, min_value: int = None, max_value: int = None, seed = 1
                            ) -> List[ArcticIntType]:
        np.random.seed(seed)
        return random_integers(size=size, dtype=dtype, min_value=min_value, max_value=max_value)
    
    @classmethod
    def generate_random_bools(cls, size: int, seed = 1) -> List[bool]:
        np.random.seed(seed)
        return np.random.choice([True, False], size=size) 
    
    @classmethod
    def random_string(cls, length: int, seed = 1):
        np.random.seed(seed)
        return "".join(random.choice(string.ascii_uppercase 
                                       + string.digits + string.ascii_lowercase + ' ') for _ in range(length))
    
    @classmethod
    def generate_random_list_with_mean(cls, number_elements, specified_mean, value_range=(0, 100), 
                                       dtype: ArcticIntType = np.int64, seed = 345) -> List[int]:
        np.random.seed(seed)
        random_list = np.random.randint(value_range[0], value_range[1], number_elements)

        current_mean = np.mean(random_list)
        
        adjustment = specified_mean - current_mean
        adjusted_list = (random_list + adjustment).astype(dtype)
        
        return adjusted_list.tolist()


class DFGenerator:
    """
    Easy generation of DataFrames, via fluent interface
    """

    def __init__(self, size: int, seed = 1):
        self.__seed = seed
        self.__size = size
        self.__data = {}
        self.__types = {}
        self.__df = None
        self.__index = None

    def generate_dataframe(self) -> pd.DataFrame:
        if self.__df is None:
            if self.__size == 0:
                # Apply proper types for dataframe when size is 0
                self.__df = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in self.__types.items()})
            else:
                self.__df = pd.DataFrame(self.__data)
            if self.__index is not None:
                self.__df.index = self.__index
        return self.__df

    def add_int_col(self, name: str, dtype: ArcticIntType = np.int64, min: int = None, max: int = None) -> 'DFGenerator':
        list = ListGenerators.generate_random_ints(dtype, self.__size, min, max, self.__seed)
        self.__data[name] = list
        self.__types[name] = dtype
        return self
    
    def add_float_col(self, name: str, dtype: ArcticFloatType = np.float64, min: float = None, max: float = None, 
                      round_at: int = None ) -> 'DFGenerator':
        list = ListGenerators.generate_random_floats(dtype, self.__size, min, max, round_at, self.__seed)
        self.__data[name] = list
        self.__types[name] = dtype
        return self
    
    def add_string_col(self, name: str, str_size: int, num_unique_values: int = None) -> 'DFGenerator':
        """
        Generates a list of strings with length 'str_size', and if 'num_unique_values' values is None
        the list will be of unique values if 'num_unique_values' is a number then this will be the length
        pf the string pool of values
        """
        list = []
        if num_unique_values is None:
            list = ListGenerators.generate_random_strings(str_size, self.__size)
        else:
            list = RandomStringPool(str_size, num_unique_values).get_list(self.__size)
        self.__data[name] = list
        self.__types[name] = str
        return self
    
    def add_string_enum_col(self, name: str, pool = RandomStringPool) -> 'DFGenerator':
        """
        Generates a list of random values based on string pool, simulating enum
        """
        list = pool.get_list(self.__size)
        self.__data[name] = list
        self.__types[name] = str
        return self
    
    def add_bool_col(self, name: str) -> 'DFGenerator':
        list = ListGenerators.generate_random_bools(self.__size, self.__seed)
        self.__data[name] = list
        self.__types[name] = bool
        return self
    
    def add_timestamp_col(self, name: str, start_date = "2020-1-1", freq = 's') -> 'DFGenerator':
        list = pd.date_range(start=start_date, periods=self.__size, freq=freq) 
        self.__data[name] = list
        self.__types[name] = pd.Timestamp
        return self
    
    def add_col(self, name: str, dtype: ArcticTypes, list: List[ArcticTypes] ) -> 'DFGenerator':
        self.__data[name] = list
        self.__types[name] = dtype
        return self

    def add_timestamp_index(self, name_col:str, freq:Union[str , timedelta , pd.Timedelta , pd.DateOffset], 
                            start_time: pd.Timestamp = pd.Timestamp(0)) -> 'DFGenerator':
        self.__index = pd.date_range(start=start_time, periods=self.__size, freq=freq, name=name_col)
        return self
    
    def add_range_index(self, name_col:str, start:int = 0, step:int = 1, dtype: str = 'int') -> 'DFGenerator':
        stop = (self.__size + start) * step
        self.__index = pd.Index(range(start, stop, step), dtype=dtype, name=name_col)
        return self
    
    @classmethod
    def generate_random_dataframe(cls, rows: int, cols: int, indexed: bool = True, seed = 123):
        """
        Generates random dataframe with specified number of rows and cols.
        The column order is also random and chosen among arctic supported
        types
        `indexed` defined if the generated dataframe will have index
        """
        cols=int(cols)
        rows=int(rows)
        np.random.seed(seed)
        if sys.version_info >= (3, 8):
            dtypes = np.random.choice(list(get_args(ArcticTypes)), cols)
        else:
            dtypes = [np.uint8, np.uint16, np.uint32, np.uint64, 
                      np.int8, np.int16, np.int32, np.int64,
                      np.float32, np.float16, str]
        gen = DFGenerator(size=rows, seed=seed) 
        for i in range(cols):
                dtype = dtypes[i]
                if 'int' in str(dtype):
                    gen.add_int_col(f"col_{i}", dtype)
                    pass
                elif 'bool' in str(dtype):
                    gen.add_bool_col(f"col_{i}")
                elif 'float' in str(dtype):
                    gen.add_float_col(f"col_{i}", dtype)
                elif 'str' in str(dtype):
                    gen.add_string_col(f"col_{i}", 10)
                else:
                    return f"Unsupported type {dtype}"
        if indexed:
            gen.add_timestamp_index("index", "s", pd.Timestamp(0))
        return gen.generate_dataframe()


    @classmethod
    def generate_random_int_dataframe(seclslf, start_name_prefix: str, 
                                      num_rows:int, num_cols:int, 
                                      dtype: ArcticIntType = np.int64, min_value: int = None, max_value: int = None,
                                      seed: int = 3432) -> pd.DataFrame:
        """
        To be used to generate large number of same type columns, when generation time is
        critical
        """
        np.random.seed(seed=seed)
        platform_int_info = np.iinfo("int_")
        iinfo = np.iinfo(dtype)
        if min_value is None:
            min_value = max(iinfo.min, platform_int_info.min)
        if max_value is None:
            max_value = min(iinfo.max, platform_int_info.max)

        data = np.random.randint(min_value, max_value, size=(num_rows, num_cols), dtype= dtype)
        columns = [f"{start_name_prefix}_{n}" for n in range(num_cols)]

        return pd.DataFrame(data=data, columns=columns)

    @classmethod
    def generate_random_float_dataframe(cls, start_name_prefix: str, num_rows: int, num_cols: int, 
                                        dtype: ArcticFloatType = np.float64, 
                                        min_value: float = None, max_value: float = None, round_at: int = None,
                                        seed: int = 54675) -> 'DFGenerator':
        """
        To be used to generate large number of same type columns, when generation time is
        critical
        """
        # Higher numbers will trigger overflow in numpy uniform (-1e307 - 1e307)
        # Get the minimum and maximum values for np.float32
        info = np.finfo(np.float32)
        _max = info.max
        _min = info.min
        np.random.seed(seed)
        if min_value is None:
            min_value = max(-1e307, -sys.float_info.max, _min)
        if max_value is None:    
            max_value = min(1e307, sys.float_info.max, _max)
        if round_at is None:
            data = np.random.uniform(min_value, max_value, size=(num_rows, num_cols)).astype(dtype)
        else :
            data = np.round(np.random.uniform(min_value, max_value, 
                                              size=(num_rows, num_cols)), round_at).astype(dtype)

        columns = [f"{start_name_prefix}_{n}" for n in range(num_cols)]

        return pd.DataFrame(data=data, columns=columns)
    
    @classmethod
    def generate_random_strings_dataframe(cls, start_name_prefix: str, num_rows: int, num_cols: int,
                                          column_sizes=None, seed: int = 4543):
        """
        To be used to generate large number of same type columns, when generation time is
        critical
        If `column_sizes` not supplied default 10 will be used
        """
        if column_sizes is None:
            column_sizes =  [10] * num_cols
        np.random.seed(seed=seed)
        data = [[random_string(column_sizes[col]) 
                 for col in range(num_cols)] 
                 for _ in range(num_rows)]
        
        columns = [f"{start_name_prefix}_{n}" for n in range(num_cols)]
        return pd.DataFrame(data=data, columns=columns)

    @classmethod
    def generate_wide_dataframe(cls, num_rows: int, num_cols: int,  
                            num_string_cols: int,
                            start_time: pd.Timestamp = None,
                            freq: Union[str , timedelta , pd.Timedelta , pd.DateOffset] = 's',
                            seed = 23445):
        """
        Generates as fast as possible specified number of columns.
        Uses random arrays generation in numpy to do that
        As the strings generation is slowest always be mindful to pass number between 1-1000 max
        The generated dataframe will have also index starting at specified `start_time`
        """

        cols, mod = divmod(num_cols - num_string_cols, 10) # divide by number of unique frame types
        
        int_frame1 = cls.generate_random_int_dataframe("int8", num_rows=num_rows, num_cols=cols, 
                                                       dtype=np.int8, seed=seed)

        int_frame2 = cls.generate_random_int_dataframe("int16", num_rows=num_rows, num_cols=cols, 
                                                       dtype=np.int16, seed=seed)

        int_frame3 = cls.generate_random_int_dataframe("int32", num_rows=num_rows, num_cols=cols, 
                                                       dtype=np.int32, seed=seed)

        int_frame4 = cls.generate_random_int_dataframe("int64", num_rows=num_rows, num_cols=cols + mod, 
                                                       dtype=np.int64, seed=seed)

        uint_frame1 = cls.generate_random_int_dataframe("uint8", num_rows=num_rows, num_cols=cols, 
                                                        dtype=np.uint8, seed=seed)

        uint_frame2 = cls.generate_random_int_dataframe("uint16", num_rows=num_rows, num_cols=cols, 
                                                        dtype=np.uint16, seed=seed)

        uint_frame3 = cls.generate_random_int_dataframe("uint32", num_rows=num_rows, num_cols=cols, 
                                                        dtype=np.uint32, seed=seed)

        uint_frame4 = cls.generate_random_int_dataframe("uint64", num_rows=num_rows, num_cols=cols, 
                                                        dtype=np.uint64, seed=seed)

        float_frame1 = cls.generate_random_float_dataframe("float32", num_rows=num_rows, num_cols=cols, 
                                                           dtype=np.float32, seed=seed)

        float_frame2 = cls.generate_random_float_dataframe("float64", num_rows=num_rows, num_cols=cols, 
                                                           dtype=np.float64, seed=seed)

        str_frame = cls.generate_random_strings_dataframe("str", num_rows=num_rows, num_cols=num_string_cols)

        frame: pd.DataFrame = pd.concat([int_frame1, int_frame2, int_frame3, int_frame4,
                           uint_frame1, uint_frame2, uint_frame3, uint_frame4,
                           float_frame1, float_frame2, str_frame], axis=1) # Concatenate horizontally
        
        if start_time:
            range = pd.date_range(start=start_time, periods=frame.shape[0], freq=freq, name='index')
            frame.index = range

        return  frame
