
import time
from typing import List
import numpy as np
import pandas as pd
from arcticdb.options import LibraryOptions
from arcticdb.util.utils import DFGenerator, RandomStringPool
from benchmarks.real_storage.libraries_creation import LibrariesBase, Storage


#region Setup classes

class VaryingSizeSymbolLibrary(LibrariesBase):
    """
    Setup Read Tests Library for different storages.
    Its aim is to have at one place the responsibility for setting up any supported storage
    with proper symbols.

    Main purpose is to provide setup of different size dataframes and store them in separate symbols
    Size can vary on columns and rows. The numbers are encoded at :func:`VaryingSizeSymbolLibrary.get_parameter_list`

    NOTE: Library options for row and column segments here are different!

    It is also responsible for providing 2 libraries: 
    - one that will hold persistent data across runs
    - one that will will hold transient data for operations which data will be wiped out
    """

    NUMBER_ROWS = 10_000

    def __init__(self, type: Storage = Storage.LMDB, arctic_url: str = None):
        super().__init__(type, arctic_url)
        self.ac = self.get_arctic_client()

    def get_library_options(self):
        return LibraryOptions(rows_per_segment=1000, columns_per_segment=1000)

    def get_library_names(self, num_symbols=1) -> List[str]:
        return ["PERM_VARYING", "MOD_VARYING"]     

    def get_symbol_name(self, sym_idx):
        return f"sym_{sym_idx}"   

    def get_parameter_list(self):
        """
            We might need to have different number of rows per different types of storages depending on the
            speed of storage. LMDB as fastest might be the only different than any other
        """
        cols = ["15000-cols__2500-rows", 
                "15000-cols__5000-rows",
                "30000-cols__2500-rows", 
                "30000-cols__5000-rows",]
        return cols
    
    def get_number_columns(self, param: str) -> int:
        """
        Extracts the number of columns from parameter
        """
        return self.get_parameter_from_string(param, 0, int)

    def get_number_rows(self, param: str) -> int:
        """
        Extracts the number of rows from parameter
        """
        return self.get_parameter_from_string(param, 1, int)

    def get_parameter_names_list(self):
        return ["params"]

    def generate_df(self, cols: int, rows: int) -> pd.DataFrame:
        """
        Dataframe generator that will be used in read and write tests
        """
        st = time.time()
        print("Dataframe generation started.")
        gen = DFGenerator(rows) 
        pool = RandomStringPool(100, 2000)
        st = time.time()
        # We have defined set of 10 different column types to replicate
        for i in range(int(cols/10)):
            gen = (gen.add_bool_col(f"bool_{i}")
                .add_float_col(f"float64_{i}", np.float64, -1e307, 1e307)
                .add_float_col(f"float32_{i}", np.float32, -10000, 10000, 4)
                .add_int_col(f"int64_{i}")
                .add_int_col(f"int32_{i}", np.int32, 0, 10000)
                .add_int_col(f"int16_{i}", np.int16)
                .add_int_col(f"int8_{i}", np.int8)
                .add_string_col(f"str4_{i}", 4)
                .add_string_enum_col(f"enum100_{i}", pool)
                .add_int_col(f"uint32_{i}", np.uint32)
                )
            #print(f"Iteration {i} completed")
        df = gen.add_timestamp_indx("index", "s", pd.Timestamp(0)).generate_dataframe()
        print(f"Dataframe rows {rows} cols {cols} generated for {time.time() - st} sec")
        return df

    def setup_read_library(self, param):
        """
        Sets up single read library with specified parameter set.
        """
        symbol = self.get_symbol_name(param)
        rows = self.get_number_rows(param)
        cols = self.get_number_columns(param)
        df = self.generate_df(cols=cols, rows=rows)
        print("Dataframe storage started.")
        st = time.time()
        lib = self.get_library()
        print("Library", lib)
        lib.write(symbol, df)
        print(f"Dataframe rows {rows} cols {cols} rows stored for {time.time() - st} sec")

    def setup_all(self):
        """
        Responsible for setting up all needed libraries for specific storage
        """
        for rows in self.get_parameter_list():
            self.setup_read_library(rows)            

#endregion

class AWS_GeneralReadWriteTests:
    """
    This class is responsible for all checks on AWS storage

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

    SETUP_CLASS = VaryingSizeSymbolLibrary(Storage.AMAZON)

    params = SETUP_CLASS.get_parameter_list()
    param_names = SETUP_CLASS.get_parameter_names_list()

    def setup_cache(self):
        '''
        Always provide implementation of setup_cache in
        the child class

        And always return storage info which should 
        be first parameter for setup, tests and teardowns
        '''
        lmdb = AWS_GeneralReadWriteTests.SETUP_CLASS.setup_environment() 
        info = lmdb.get_storage_info()
        print("STORAGE INFO: ", info)
        return info

    def setup(self, storage_info, params):
        '''
        This setup method for read and writes can be executed only once
        No need to be executed before each test. That is why we define 
        `repeat` as 1
        '''
        ## Construct back from arctic url the object
        self.lmdb = VaryingSizeSymbolLibrary.fromStorageInfo(storage_info)
        sym = self.lmdb.get_symbol_name(params)
        print("STORAGE INFO: ", storage_info)
        print("ARCTIC :", self.lmdb.get_arctic_client())
        print("Library :", self.lmdb.get_library())
        print("Symbols :", self.lmdb.get_library().list_symbols())
        print("Looking for :", sym)
        self.to_write_df = self.lmdb.get_library().read(symbol=sym).data

    def time_read_wide(self, storage_info, params):
        sym = self.lmdb.get_symbol_name(params)
        self.lmdb.get_library().read(symbol=sym)

    def peakmem_read_wide(self, storage_info, params):
        sym = self.lmdb.get_symbol_name(params)
        self.lmdb.get_library().read(symbol=sym)

    def time_write_wide(self, storage_info, params):
        sym = self.lmdb.get_symbol_name(params)
        self.lmdb.get_modifyable_library(1).write(symbol=sym, data=self.to_write_df)

    def peakmem_write_wide(self, storage_info, params):
        sym = self.lmdb.get_symbol_name(params)
        self.lmdb.get_modifyable_library().write(symbol=sym, data=self.to_write_df)        

    def time_write_staged(self, rows):
        self.fresh_lib.write(f"sym", self.df, staged=True)
        self.fresh_lib._nvs.compact_incomplete(f"sym", False, False)

    def peakmem_write_staged(self, rows):
        self.fresh_lib.write(f"sym", self.df, staged=True)
        self.fresh_lib._nvs.compact_incomplete(f"sym", False, False)     