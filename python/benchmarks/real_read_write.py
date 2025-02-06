"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
from typing import List
import numpy as np
import pandas as pd

from arcticdb.util.utils import DFGenerator
from benchmarks.real_storage.libraries_creation import LibrariesBase, Storage


#region Setup classes

class ReadBenchmarkLibraries(LibrariesBase):
    """
    Setup Read Tests Library for different storages.
    Its aim is to have at one place the responsibility for setting up any supported storage
    with proper symbols.

    It is also responsible for providing 2 libraries: 
    - one that will hold persistent data across runs
    - one that will will hold transient data for operations which data will be wiped out
    """

    def __init__(self, type: Storage = Storage.LMDB, arctic_url: str = None):
        super().__init__(type, arctic_url)
        self.ac = self.get_arctic_client()

    def get_library_names(self, num_symbols=1) -> List[str]:
        return ["PERM_READ", "MOD_READ"]        

    def get_parameter_list(self):
        """
            We might need to have different number of rows per different types of storages depending on the
            speed of storage. LMDB as fastest might be the only different than any other
        """
        # Initially tried with those values but turn out they are too slow for Amazon s3
        # Still they are avail in the storage
        # rows = [5_000_000, 10_000_000, 20_000_000]
        rows = [1_000_000, 2_000_000]
        if self.type == Storage.LMDB:
            rows = [2_500_000, 5_000_000]
        return rows
    
    def get_parameter_names_list(self):
        return ["num_rows"]

    def generate_df(self, row_num:int) -> pd.DataFrame:
        """
        Dataframe that will be used in read and write tests
        """
        st = time.time()
        print("Dataframe generation started.")
        df = (DFGenerator(row_num)
            .add_int_col("int8", np.int8)
            .add_int_col("int16", np.int16)
            .add_int_col("int32", np.int32)
            .add_int_col("int64", min=-26, max=31)
            .add_int_col("uint64", np.uint64, min=100, max=199)
            .add_float_col("float16",np.float32)
            .add_float_col("float2",min=-100.0, max=200.0, round_at=4)
            .add_string_col("string10", str_size=10)
            .add_string_col("string20", str_size=20, num_unique_values=20000)
            .add_bool_col("bool")
            .add_timestamp_indx("time", 's', pd.Timestamp("2000-1-1"))
            ).generate_dataframe()
        print(f"Dataframe {row_num} rows generated for {time.time() - st} sec")
        return df

    def setup_read_library(self, num_rows):
        """
        Sets up single read library
        """
        symbol = f"sym_{num_rows}_rows"
        df = self.generate_df(num_rows)
        print("Dataframe storage started.")
        st = time.time()
        lib = self.get_library()
        print("Library", lib)
        lib.write(symbol, df)
        print(f"Dataframe {num_rows} rows stored for {time.time() - st} sec")

    def setup_all(self):
        """
        Responsible for setting up all needed libraries for specific storage
        """
        for rows in self.get_parameter_list():
            self.setup_read_library(rows)

#endregion

class LMDB_ReadWrite:
    """
    This class is responsible for all checks on LMDB storage

        IMPORTANT: 
        - When we inherit from another test we inherit test, setup and teardown methods
        - setup_cache() method we inherit it AS IS, thus it will be executed only ONCE for
          all classes that inherit from the base class. Therefore it is perhaps best to ALWAYS
          provide implementation in the child class, no matter that it might look like code repeat

    """

    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 12000

    SETUP_CLASS: ReadBenchmarkLibraries = ReadBenchmarkLibraries(Storage.LMDB)

    params = SETUP_CLASS.get_parameter_list()
    param_names = SETUP_CLASS.get_parameter_names_list()

    def setup_cache(self):
        '''
        Always provide implementation of setup_cache in
        the child class

        And always return storage info which should 
        be first parameter for setup, tests and teardowns
        '''
        lmdb = LMDB_ReadWrite.SETUP_CLASS.setup_environment() 
        info = lmdb.get_storage_info()
        print("STORAGE INFO: ", info)
        return info

    def setup(self, storage_info, num_rows):
        '''
        This setup method for read and writes can be executed only once
        No need to be executed before each test. That is why we define 
        `repeat` as 1
        '''
        ## Construct back from arctic url the object
        self.lmdb = ReadBenchmarkLibraries.fromStorageInfo(storage_info)
        sym = self.lmdb.get_symbol_name(num_rows)
        print("STORAGE INFO: ", storage_info)
        print("ARCTIC :", self.lmdb.get_arctic_client())
        print("Library :", self.lmdb.get_library())
        print("Symbols :", self.lmdb.get_library().list_symbols())
        print("Looking for :", sym)
        self.to_write_df = self.lmdb.get_library().read(symbol=sym).data

    def time_read(self, storage_info, num_rows):
        sym = self.lmdb.get_symbol_name(num_rows)
        self.lmdb.get_library().read(symbol=sym)

    def peakmem_read(self, storage_info, num_rows):
        sym = self.lmdb.get_symbol_name(num_rows)
        self.lmdb.get_library().read(symbol=sym)

    def time_write(self, storage_info, num_rows):
        sym = self.lmdb.get_symbol_name(num_rows)
        self.lmdb.get_modifyable_library(1).write(symbol=sym, data=self.to_write_df)

    def peakmem_write(self, storage_info, num_rows):
        sym = self.lmdb.get_symbol_name(num_rows)
        self.lmdb.get_modifyable_library().write(symbol=sym, data=self.to_write_df)


class AWS_ReadWrite(LMDB_ReadWrite):
    """
    This class is responsible for all checks on AWS

    IMPORTANT: 
        - When we inherit from another test we inherit test, setup and teardown methods
        - setup_cache() method we inherit it AS IS, thus it will be executed only ONCE for
          all classes that inherit from the base class. Therefore it is perhaps best to ALWAYS
          provide implementation in the child class, no matter that it might look like code repeat
    """

    rounds = 1
    number = 3 # invoke X times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 12000

    SETUP_CLASS = ReadBenchmarkLibraries(Storage.AMAZON)

    params = SETUP_CLASS.get_parameter_list()
    param_names = SETUP_CLASS.get_parameter_names_list()

    def setup_cache(self):
        '''
        Always provide implementation of setup_cache in
        the child class

        And always return storage info which should 
        be first parameter for setup, tests and teardowns
        '''
        aws = AWS_ReadWrite.SETUP_CLASS.setup_environment() 
        return aws.get_storage_info()

