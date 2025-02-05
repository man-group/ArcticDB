

import time
from typing import List
import numpy as np
import pandas as pd

from arcticdb.options import LibraryOptions
from arcticdb.util.utils import DFGenerator
from arcticdb.version_store.library import Library
from benchmarks.real_storage.libraries_creation import LibrariesBase, Storage


#region Setup classes

class SymbolLibraries(LibrariesBase):

    def __init__(self, type: Storage = Storage.LMDB, arctic_url: str = None):
        super().__init__(type, arctic_url)
        self.ac = self.get_arctic_client()
        self.df = self.generate_df(10)

    def get_parameter_list(self):
        return [500, 1000]
    
    def get_parameter_names_list(self):
        return ["num_syms"]
    
    def get_library_options(self):
        return LibraryOptions(rows_per_segment=20, columns_per_segment=20)
    
    def get_library_names(self, num_symbols) -> List[str]:
        return [f"PERM_LIST_OPS_SYMBOL_{num_symbols}", f"MOD_LIST_OPS_SYMBOL_{num_symbols}"]  
    
    def generate_df(self, row_num:int) -> pd.DataFrame:
        """
        Dataframe that will be used in list
        """
        st = time.time()
        print("Dataframe generation started.")
        df = (DFGenerator(row_num)
            .add_int_col("int8", np.int8)
            .add_int_col("int16", np.int16)
            .add_int_col("int32", np.int32)
            .add_int_col("int64")
            .add_int_col_ex("uint8", 1, 20, np.uint64)
            .add_int_col_ex("uint64", 100, 1999, np.uint64)
            .add_float_col("float16",np.float32)
            .add_float_col_ex("float64",-100.0, 200.0, 4)
            .add_string_col("string100", 100, 200)
            .add_string_col("string2", 2)
            .add_bool_col("bool")
            .add_timestamp_indx("time", 's', pd.Timestamp("2000-1-1"))
            ).generate_dataframe()
        print(f"Dataframe {row_num} rows generated for {time.time() - st} sec")
        return df

    def setup_library_with_symbols(self, num_syms):
        """
        Sets up required number of symbols per that library
        """
        self.lib = self.get_library(num_syms) 
        print(f"Started creating library for {num_syms} symbols.")
        st = time.time()
        for num in range(num_syms):
            sym = self.get_symbol_name(num)
            self.lib.write(sym, self.df, metadata=self.df)
            print(f"created {sym}")
        print(f"Library '{self.lib}' created {num_syms} symbols COMPLETED for :{time.time() - st} sec")

    def reset_symbols_cache(self):
        pass #TBD

    def setup_all(self):
        st = time.time()
        for sym in self.get_parameter_list():
            self.setup_library_with_symbols(sym)
        print(f"Total time {time.time() - st}")

    def check_ok(self) -> bool:
        for num in self.get_parameter_list():
            lib = self.get_library(num)
            list = lib.list_symbols()
            print(f"number symbols {len(list)} in library {lib}")
            if len(list) != num:
                return False
        return True
        

class VersionLibraries(SymbolLibraries):
    """
    A library for list versions testings is composed of X symbols.
    Symbols are sym_name_[0,X)
    Each symbol has as many versions as the index in the name
    Thus first symbol will have 1, 2nd - 2, 3rd - 3 etc
    Total version expected for N symbols:
        Sn = n(n+1)/2
        which is SUM(1..N)
    """

    def __init__(self, type: Storage = Storage.LMDB, arctic_url: str = None):
        super().__init__(type, arctic_url)
        self.ac = self.get_arctic_client()

    def get_parameter_list(self):
        return [25, 50]
    
    def get_library_names(self, num_symbols) -> List[str]:
        return [f"PERM_LIST_OPS_VERSION_{num_symbols}", f"MOD_LIST_OPS_VERSION_{num_symbols}"]  

    def setup_symbols_with_versions(self, num_symbols):
        self.lib = self.get_library(num_symbols)
        for sym_num in range(num_symbols):
            sym = self.get_symbol_name(sym_num)
            print(f"Generating {sym_num} versions for symbol [{sym}].")
            st = time.time()
            if sym_num > 0:
                for num in range(sym_num):
                    self.lib.write(sym, self.df, metadata=self.df)
            print(f"Generating {sym_num} versions for symbol [{sym}]. COMPLETED for :{time.time() - st} sec")
    
    def setup_all(self):
        st = time.time()
        for sym in self.get_parameter_list():
            self.setup_library_with_symbols(sym)
            self.setup_symbols_with_versions(sym)
        print(f"Total time {time.time() - st}")
        
    def check_ok(self) -> bool:

        def expected_version(n):
            return  int((n*(n+1)) /2)
        
        if not super().check_ok():
            return False
        for num in self.get_parameter_list():
            lib = self.get_library(num)
            vers = len(lib.list_versions())
            #for v in lib.list_versions():
            #    print(v)
            exp = expected_version(num)
            print(f"There are {vers} in library {lib} , expected {exp}")
            if vers != exp:
                return False
        return True

#endregion

class AWS_ListSymbols:
    """
    This class is responsible for all checks on AWS storage

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

    SETUP_CLASS = SymbolLibraries(Storage.AMAZON)

    params = SETUP_CLASS.get_parameter_list()
    param_names = SETUP_CLASS.get_parameter_names_list()

    def setup_cache(self):
        '''
        Always provide implementation of setup_cache in
        the child class

        And always return the arctic url which should 
        be first parameter for setup, tests and teardowns
        '''
        aws = AWS_ListSymbols.SETUP_CLASS
        if not aws.check_ok():
            aws.setup_all()
        else:
            aws.reset_symbols_cache()
        return aws.get_storage_info()

    def setup(self, storage_info, num_syms):
        self.aws = SymbolLibraries.fromStorageInfo(storage_info)
        self.lib = self.aws.get_library(num_syms)

    def time_list_symbols(self, storage_info, num_syms):
        self.lib.list_symbols()

    def time_has_symbol_nonexisting(self, storage_info, num_syms):
        self.lib.has_symbol("250_sym")        

    def peakmem_list_symbols(self, storage_info, num_syms):
        self.lib.list_symbols()


class AWS_VersionSymbols:
    """
    This class is responsible for all checks on AWS storage

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

    SETUP_CLASS = VersionLibraries(Storage.AMAZON)

    params = SETUP_CLASS.get_parameter_list()
    param_names = SETUP_CLASS.get_parameter_names_list()

    def setup_cache(self):
        '''
        Always provide implementation of setup_cache in
        the child class

        And always return the arctic url which should 
        be first parameter for setup, tests and teardowns
        '''
        aws = AWS_VersionSymbols.SETUP_CLASS.setup_environment() 
        return aws.get_storage_info()

    def setup(self, storage_info, num_syms):
        self.aws = VersionLibraries.fromStorageInfo(storage_info)
        self.lib = self.aws.get_library(num_syms)

    def time_list_versions___sum_all(self, storage_info, num_syms):
        '''
        Returns all versions in the library, which is SUM(1..num_syms)
        '''
        self.lib.list_versions()

    def time_list_versions_one_symbol__max_number(self, storage_info, num_syms):
        '''
        Obtains versions only per one symbol having the same number of versions
        as the number suggest
        '''
        sym=self.aws.get_symbol_name(num_syms-1)
        self.lib.list_versions(sym)

    def peakmem_list_versions___sum_all(self, storage_info, num_syms):
        '''
        Returns all versions in the library, which is SUM(1..num_syms)
        '''
        self.lib.list_versions()
