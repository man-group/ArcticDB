import copy
from abc import ABC, abstractmethod
from enum import Enum
import logging
import os
import socket
import tempfile
import time
import re
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Union

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.storage_fixtures.s3 import BaseS3StorageFixtureFactory, real_s3_from_environment_variables
from arcticdb.util.utils import DFGenerator, ListGenerators, TimestampNumber
from arcticdb.version_store.library import Library


## Amazon s3 storage bucket dedicated for ASV performance tests
AWS_S3_DEFAULT_BUCKET = 'arcticdb-asv-real-storage'
PERSISTENT_LIBS_PREFIX = "PERMANENT_LIBRARIES" 
MODIFIABLE_LIBS_PREFIX = 'MODIFIABLE_LIBRARIES' 
TEST_LIBS_PREFIX = 'TESTS_LIBRARIES' 

def get_logger_for_asv(bencmhark_cls = None):
    logLevel = logging.INFO
    if bencmhark_cls:
        logger = logging.getLogger(bencmhark_cls.__name__)
    else:
        logger = logging.getLogger("ASV")
    logger.setLevel(logLevel)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logLevel)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


class Storage(Enum):
    AMAZON = 1
    LMDB = 2


class LibraryType(Enum):
    PERSISTENT = "PERMANENT"
    MODIFIABLE = "MODIFIABLE"


class StorageSetup:
    '''
    Defined special one time setup for real storages.
    Place here what is needed for proper initialization
    of each storage
    '''
    _instance = None
    _aws_default_factory: BaseS3StorageFixtureFactory = None
    _fixture_cache = {}

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(StorageSetup, cls).__new__(cls, *args, **kwargs)
            cls._instance.value = "Initialized"

            ## AWS S3 variable setup
            ## Copy needed environments variables from GH Job env vars
            ## Will only throw exceptions if real tests are executed
            cls._aws_default_factory = real_s3_from_environment_variables(shared_path=True)
            cls._aws_default_factory.default_prefix = None
            cls._aws_default_factory.default_bucket = AWS_S3_DEFAULT_BUCKET
            cls._aws_default_factory.clean_bucket_on_fixture_exit = False
   
    def get_machine_id():
        """
        Returns machine id, or id specified through environments variable (for github)
        """
        id = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX", None)
        if id is None:
            id = socket.gethostname()
        return  id

    @classmethod
    def get_aws_s3_arctic_uri(cls, prefix: str = MODIFIABLE_LIBS_PREFIX, confirm_persistent_storage_need: bool = False) -> str:
        """
        Persistent libraries should be created in `prefix` None!
        Temporary should be created in prexies other than None
        """
        assert cls._aws_default_factory, "Environment variables not initialized (ARCTICDB_REAL_S3_ACCESS_KEY,ARCTICDB_REAL_S3_SECRET_KEY)"
        assert (prefix is not None) and (prefix.strip() != ""), "None or empty string prefix is not supported!"
        if prefix == PERSISTENT_LIBS_PREFIX:
            assert confirm_persistent_storage_need, f"Use of persistent store not confirmed!"
        cls._aws_default_factory.default_prefix = prefix
        return cls._aws_default_factory.create_fixture().arctic_uri


class StorageAccess:

    def __init__(self, storage: Storage, name_benchmark: str, library_options: LibraryOptions = None) :
        """
        Populate `name_benchamrk` to get separate modifiable space for 
        """
        self.storage: Storage = storage
        self.library_options: LibraryOptions = library_options
        self.name_benchmark: str = name_benchmark
        self._test_mode = False
        self._uris_cache: List[str] = []
        self._ac_cache = {}
        StorageSetup()

    def log_info(self):
        logger = get_logger_for_asv()
        logger.info(f"{self} information:")
        for key in self._ac_cache.keys():
            logger.info(f"Arctic URI: {key}")

    # Currently we're using the same arctic client for both persistant and modifiable libraries.
    # We might decide that we want different arctic clients (e.g. different buckets) but probably not needed for now.
    def _get_arctic_client(self) -> Arctic:
        prefix = PERSISTENT_LIBS_PREFIX
        if self._test_mode == True:
            prefix = TEST_LIBS_PREFIX
        return self.__get_arctic_client_internal(prefix, confirm_persistent_storage_need = True)

    def _get_arctic_client_modifiable(self) -> Arctic:
        prefix = f"{MODIFIABLE_LIBS_PREFIX}_{StorageSetup.get_machine_id()}/{self.name_benchmark}"
        return self.__get_arctic_client_internal(prefix, confirm_persistent_storage_need = False)

    def __get_arctic_client_internal(self, prefix, confirm_persistent_storage_need: bool = False) -> Arctic:
        if self.storage == Storage.AMAZON:
            arctic_url = StorageSetup.get_aws_s3_arctic_uri(prefix, confirm_persistent_storage_need)
        elif self.storage == Storage.LMDB:
            ## We create fpr each object unique library in temp dir
            arctic_url = f"lmdb://{tempfile.gettempdir()}/benchmarks_{prefix}" 
        else:
            raise Exception("Unsupported storage type :", self.storage)
        ac =  self._ac_cache.get(arctic_url, None)
        if ac is None:
            ac = Arctic(arctic_url)    
        self._ac_cache[arctic_url] = ac
        return ac    

    def get_library_name(self, library_type, lib_name_suffix):
        if library_type == LibraryType.PERSISTENT:
            return f"{library_type}_{self.name_benchmark}_{lib_name_suffix}"
        if library_type == LibraryType.MODIFIABLE:
            # We want the modifiable libraries to be unique per process/ benchmark class. We embed this deep in the name
            return f"{library_type}_{self.name_benchmark}_{os.getpid()}_{lib_name_suffix}"

    def get_library(self, library_type : LibraryType, lib_name_suffix : str) -> Library:
        lib_name = self.get_library_name(library_type, lib_name_suffix)
        if library_type == LibraryType.PERSISTENT:
           return self._get_arctic_client().get_library(lib_name, create_if_missing=True)
        elif library_type == LibraryType.MODIFIABLE:
           return self._get_arctic_client_modifiable().get_library(lib_name, create_if_missing=True, 
                                                        library_options= self.library_options)
        else:
            raise Exception(f"Unsupported library type: {library_type}")

    def has_library(self, library_type : LibraryType, lib_name_suffix : str) -> Library:
        lib_name = self.get_library_name(library_type, lib_name_suffix)
        if library_type == LibraryType.PERSISTENT:
           return self._get_arctic_client().has_library(lib_name)
        elif library_type == LibraryType.MODIFIABLE:
           return self._get_arctic_client_modifiable().has_library(lib_name)        
        else:
            raise Exception(f"Unsupported library type: {library_type}")

    # Not sure how useful is this, one can always just keep track of the libraries created and clear them
    def clear_all_modifiable_libs_from_this_process(self):
        ac = self._get_arctic_client_modifiable()
        lib_names = set(ac.list_libraries())
        to_deletes = [lib_name for lib_name in lib_names 
                      if f"_{os.getpid()}_" in lib_name]
        for to_delete in to_deletes:
            ac.delete_library(to_delete)


# It is quite clear what is this responsible for: only dataframe generation
# Using such an abstraction can help us deduplicate the dataframe generation code between the different `EnvironmentSetup`s
# Note: We use a class instead of a generator function to allow caching of dataframes in the state
class DataFrameGenerator(ABC):
    @abstractmethod
    def get_dataframe(self, **kwargs):
        pass

# The population policy is tightly paired with the `populate_library`
# Using a separate abstraction for the population policy can allow us to be flexible with how we populate libraries.
# E.g. One benchmark can use one population policy for read operations, a different for updates and a third for appends
# Note: Using a kwargs generator to be passed to the df_generator allows customization of dataframe generation params (e.g. num_rows, num_cols, use_string_columns?)
class LibraryPopulationPolicy(ABC):

    def __init__(self, num_symbols: int, df_generator: DataFrameGenerator):
        self.num_symbols = num_symbols
        self.df_generator = df_generator

    @abstractmethod
    def get_symbol_name(self, ind: int):
        pass

    @abstractmethod
    def get_generator_kwargs(self, ind: int) -> Dict[str, Any]:
        pass

    def populate_library(self, lib: Library, population_policy: 'LibraryPopulationPolicy'):
        num_symbols = population_policy.num_symbols
        df_generator = population_policy.df_generator
        for i in range(num_symbols):
            sym = population_policy.get_symbol_name(i)
            kwargs = population_policy.get_generator_kwargs(i)
            df = df_generator.get_dataframe(**kwargs)
            lib.write(sym, df)

    # This is the only API to populate a persistent library. If we deem useful we can also add a check whether library is valid (e.g. has the correct num_symbols)
    # As discussed, ideally this will be called in completely separate logic from ASV tests to avoid races, but for simplicity
    # for now we can just rely on setup_cache to populate the persistant libraries if they are missing.
    def populate_persistent_library_if_missing(ac, benchmark_cls, lib_name_suffix, population_policy : 'LibraryPopulationPolicy'):
        """
        lib_name = get_library_name(LibraryType.PERSISTENT, benchmark_cls, lib_name_suffix)
        if ac.has_library(lib_name):
            lib = ac.create_library(lib_name)
            populate_library(lib, population_policy)            
        """


sa = StorageAccess(Storage.AMAZON, "MY_NAME")
print(sa._get_arctic_client().list_libraries())
sa._test_mode = True
sa.get_library(LibraryType.PERSISTENT, "haho")
print(sa._get_arctic_client().list_libraries())
sa.get_library(LibraryType.MODIFIABLE, "bebo")
print(sa._get_arctic_client_modifiable().list_libraries())
sa.log_info()
sa.clear_all_modifiable_libs_from_this_process()
print(sa._get_arctic_client_modifiable().list_libraries())








