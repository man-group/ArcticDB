

import time
from typing import List
import numpy as np
import pandas as pd

from arcticdb.util.utils import DFGenerator
from arcticdb.version_store.library import Library
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

    From Sofia Wifi RealS3 speed
        Dataframe 1000000 rows generated for 3.289991617202759 sec
        Dataframe 1000000 rows stored for 25.995510578155518 sec
        Dataframe 2000000 rows generated for 6.295953989028931 sec
        Dataframe 2000000 rows stored for 47.125516176223755 sec
        Dataframe 5000000 rows generated for 17.274338960647583 sec
        Dataframe 5000000 rows stored for 117.21490359306335 sec
        Dataframe 10000000 rows generated for 35.046889543533325 sec
        Dataframe 10000000 rows stored for 228.07661938667297 sec
        Dataframe 20000000 rows generated for 72.01222062110901 sec
        Dataframe 20000000 rows stored for 456.6385509967804 sec
    """

    def __init__(self, type: Storage = Storage.LMDB, arctic_url: str = None):
        super().__init__(type, arctic_url)
        self.ac = self.get_arctic()
        self.lib = self.get_library(1)

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
            .add_int_col_ex("int64", -26, 31)
            .add_int_col_ex("uint64", 100, 199, np.uint64)
            .add_float_col("float16",np.float32)
            .add_float_col_ex("float2",-100.0, 200.0, 4)
            .add_string_col("string10", 10)
            .add_string_col("string20", 20, 20000)
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
        self.lib.write(symbol, df)
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
    """

    rounds = 1
    number = 1 
    repeat = 3 # On LMDB we repeat at least 3 times
    min_run_count = 1
    warmup_time = 0

    timeout = 12000

    SETUP_CLASS: ReadBenchmarkLibraries = ReadBenchmarkLibraries(Storage.LMDB)

    params = SETUP_CLASS.get_parameter_list()
    param_names = ["num_rows"]

    def get_creator(self):
        """
        Needed for inheritance, due to fact that ASV spawns many processes,
        and accessing SETUP_CLASS is not an option as it will be different in each one
        Thus the class that inherits must override that method with its own 
        library creation class
        """
        return LMDB_ReadWrite.SETUP_CLASS

    def setup_cache(self):
        lmdb = self.get_creator() 
        lmdb.delete_modifyable_library()
        if not lmdb.check_ok():
            lmdb.setup_all()
        # make sure that only the proc that sets up database
        # its arctic url will be used later in other threads
        return lmdb.arctic_url

    def setup(self, arctic_url, num_rows):
        ## Construct back from arctic url the object
        self.lmdb: ReadBenchmarkLibraries = ReadBenchmarkLibraries(arctic_url=arctic_url)

        ## Create write cache
        sym = self.lmdb.get_symbol_name(num_rows)
        self.to_write_df = self.lmdb.get_library().read(symbol=sym).data

    def time_read(self, arctic_url, num_rows):
        sym = self.lmdb.get_symbol_name(num_rows)
        self.lmdb.get_library().read(symbol=sym)

    def peakmem_read(self, arctic_url, num_rows):
        sym = self.lmdb.get_symbol_name(num_rows)
        self.lmdb.get_library().read(symbol=sym)

    def time_write(self, arctic_url, num_rows):
        sym = self.lmdb.get_symbol_name(num_rows)
        self.lmdb.get_modifyable_library(1).write(symbol=sym, data=self.to_write_df)

    def peakmem_write(self, arctic_url, num_rows):
        sym = self.lmdb.get_symbol_name(num_rows)
        self.lmdb.get_modifyable_library().write(symbol=sym, data=self.to_write_df)


class AWS_ReadWrite(LMDB_ReadWrite):
    """
    This class is responsible for all checks on AWS
    """

    rounds = 1
    number = 1 
    repeat = 1
    min_run_count = 1
    warmup_time = 0

    timeout = 12000

    SETUP_CLASS = ReadBenchmarkLibraries(Storage.AMAZON)

    params = SETUP_CLASS.get_parameter_list()
    param_names = ["num_rows"]

    def get_creator(self):
        return AWS_ReadWrite.SETUP_CLASS

