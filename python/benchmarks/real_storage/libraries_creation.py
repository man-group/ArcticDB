
from abc import ABC, abstractmethod
from datetime import timedelta
import datetime
from enum import Enum
import os
import random
import string
import sys
import tempfile
import time
from typing import Any, List, Union
import numpy as np
import pandas as pd


from arcticdb.arctic import Arctic
from arcticdb.version_store.library import Library


## Amazon s3 storage URL
AWS_URL_TEST = 's3s://s3.eu-west-1.amazonaws.com:arcticdb-asv-real-storage?aws_auth=true'


class SetupConfig:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetupConfig, cls).__new__(cls, *args, **kwargs)
            cls._instance.value = "Initialized"

            ## AWS S3 variable setup
            ## Copy needed environments variables from GH Job env vars
            ## Will only throw exceptions if real tests are executed
            os.environ['AWS_ACCESS_KEY_ID'] = os.environ['ARCTICDB_REAL_S3_ACCESS_KEY']
            os.environ['AWS_SECRET_ACCESS_KEY'] = os.environ['ARCTICDB_REAL_S3_SECRET_KEY']
            os.environ['AWS_DEFAULT_REGION'] = os.environ['ARCTICDB_REAL_S3_BUCKET']
        return 


class Storage(Enum):
    AMAZON = 1
    LMDB = 2


class LibrariesBase(ABC):

    def __init__(self, type: Storage = Storage.LMDB, arctic_url: str = None):
        """
        Should be initialized with type or arctic url
        """
        self.arctic_url = arctic_url
        if self.arctic_url is not None:
            if "lmdb" in self.arctic_url.lower():
                self.type = Storage.LMDB
            if "s3s://" in self.arctic_url.lower():
                self.type = Storage.AMAZON
        else:     
            self.type = type
        self.ac = None
        self.__libraries = set()
        SetupConfig()
    
    def remove_libraries(self):
        ## Remove only LMDB libs as they need to
        if self.type == Storage.LMDB:
            ac = self.get_arctic()
            for lib in self.__libraries:
                ac.delete_library(lib)

    def get_arctic(self):
        if self.ac is None:
            if self.arctic_url is None:
                if (self.type == Storage.AMAZON):
                    self.arctic_url = AWS_URL_TEST
                elif (self.type == Storage.LMDB):
                    ## We create fpr each object unique library in temp dir
                    self.arctic_url = f"lmdb://{tempfile.gettempdir()}/benchmarks_{int(time.time() * 1000)}" 
                    #home_dir = os.path.expanduser("~")
                    #self.ac = Arctic(f"lmdb://{home_dir}/benchmarks")
                else:
                    raise Exception("Unsupported storage type :", self.type)
            self.ac = Arctic(self.arctic_url)
        return self.ac
    
    def get_library_options(self):    
        """
        Override to create non-default lib opts
        """
        return None
    
    def get_library_names(self, num_symbols: int = 1) -> List[str]:
        """
        In case more than one library is needed define such method.
        In case one is needed just pass 1 as constant or use default value
        """
        lib_names = [f"PERM_XXX_{num_symbols}", f"MOD_XXX_{num_symbols}"]        
        raise Exception(f"Override to return proper library names {lib_names}")
    
    def get_library(self, num_symbols: int = 1) -> Library:
        """
        Returns one time setup library (permanent)
        """
        ac = self.get_arctic()
        lib_opts = self.get_library_options()
        lib_name = self.get_library_names(num_symbols)[0]
        self.__libraries.add(lib_name)
        if lib_opts is None:
            return ac.get_library(lib_name, create_if_missing=True)
        else:
            return ac.get_library(lib_name, create_if_missing=True, 
                                  library_options=lib_opts)
    
    def get_modifyable_library(self, num_symbols: int = 1) -> Library:
        """
        Returns library to read write and delete after done.
        """
        ac = self.get_arctic()
        lib_opts = self.get_library_options()
        lib_name = self.get_library_names(num_symbols)[1]
        self.__libraries.add(lib_name)
        if lib_opts is None:
            return ac.get_library(lib_name, create_if_missing=True)
        else:
            return ac.get_library(lib_name, create_if_missing=True, 
                                  library_options=lib_opts)

    def delete_modifyable_library(self, num_symbols: int = 1):
        ac = self.get_arctic()
        ac.delete_library(self.get_library_names(num_symbols)[1])

    def get_symbol_name(self, sym_idx):
        """
        Constructs the symbol name based on an index, which could be 
        the number of rows etc.
        """
        return f"sym_{sym_idx}_rows"
    
    @abstractmethod
    def get_parameter_list(self):
        """
        Defines parameter list for benchmarks. Can be used for number of rows, symbols etc.

        NOTE: it might return different list of values depending on the storage type due to speed
        """
        pass

    def check_ok(self):
        """
        Checks if library contains all needed data to run tests.
        if OK, setting things up can be skipped

        Default implementation just checks if all symbols that should be there are there
        """
        symbols = self.get_library(1).list_symbols()
        for rows in self.get_parameter_list():
            symbol = self.get_symbol_name(rows)
            print(f"Check symbol {symbol}")
            if not symbol in symbols:
                return False
        return True
    
    def delete_libraries(self, nameStarts: str, confirm=False):
        """
        Deletes all libraries starting with specified string
        """
        assert confirm, "deletion not confirmed!"
        ac = self.get_arctic()
        list = ac.list_libraries()
        for lib in list:
            if nameStarts in lib:
                print(f"Deleteing {lib}")
                ac.delete_library(lib)





