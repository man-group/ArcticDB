import copy
from abc import ABC, abstractmethod
from enum import Enum
import inspect
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

loggers:Dict[str, logging.Logger] = {}

def get_console_logger(bencmhark_cls: Union[str, Any] = None):
    """
    Creates logger instance with associated console handler.
    The logger name can be either passed as string or class,
    or if not automatically will assume the caller module name
    """
    logLevel = logging.INFO
    if bencmhark_cls:
        if isinstance(bencmhark_cls, str):
            value = bencmhark_cls
        else:
            value = type(bencmhark_cls).__name__
        name = value
    else:
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        name = module.__name__

    logger = loggers.get(name, None)
    if logger :
        return logger
    logger = logging.getLogger(name)    
    logger.setLevel(logLevel)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logLevel)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    loggers[name] = logger
    return logger


class Storage(Enum):
    AMAZON = 1
    LMDB = 2


class StorageSpace(Enum):
    """
    Defines the type of storage space.
    Will be used as prefixes to separate shared storage 
    """
    PERSISTENT = "PERMANENT_LIBRARIES"
    MODIFIABLE = "MODIFIABLE_LIBRARIES"
    TEST = "TESTS_LIBRARIES"


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
    def _create_prefix(cls, storage_space: StorageSpace, add_to_prefix: str ) -> str:
        def is_valid_string(s: str) -> bool:
            return bool(s and s.strip())
        
        def create_prefix(mandatory_part:str, optional:str) -> str:
            if is_valid_string(add_to_prefix):
                return f"{mandatory_part}/{optional if optional is not None else ''}"
            else:
                return mandatory_part
            
        if storage_space == StorageSpace.PERSISTENT:
            prefix = create_prefix(StorageSpace.PERSISTENT.value, add_to_prefix)
        elif storage_space == StorageSpace.MODIFIABLE:
            assert is_valid_string(add_to_prefix), "Empty string prefix for modifiable library is not supported!"
            prefix = create_prefix(StorageSpace.MODIFIABLE.value, add_to_prefix)
        else:
            prefix = create_prefix(StorageSpace.TEST.value, add_to_prefix)   

        return prefix     
    
    @classmethod
    def _check_persistance_access_asked(cls, storage_space: StorageSpace, confirm_persistent_storage_need: bool = False) -> str:
        assert cls._aws_default_factory, "Environment variables not initialized (ARCTICDB_REAL_S3_ACCESS_KEY,ARCTICDB_REAL_S3_SECRET_KEY)"
        if storage_space == StorageSpace.PERSISTENT:
            assert confirm_persistent_storage_need, f"Use of persistent store not confirmed!"
    
    @classmethod
    def _get_aws_s3_arctic_uri(cls, storage_space: StorageSpace, add_to_prefix: str = None, 
                               confirm_persistent_storage_need: bool = False) -> str:
        """
        Constructs correct AWS s3 URI for accessing specified type of storage space
        For test storage space pass storage_space None
        """
        StorageSetup._check_persistance_access_asked(storage_space, confirm_persistent_storage_need)
        cls._aws_default_factory.default_prefix = StorageSetup._create_prefix(storage_space, add_to_prefix)
        return cls._aws_default_factory.create_fixture().arctic_uri

    @classmethod
    def _get_lmdb_arctic_uri(cls, storage_space: StorageSpace, add_to_prefix: str = None, confirm_persistent_storage_need: bool = False) -> str:
        """
        Constructs correct LMDB URI for accessing specified type of storage space
        For test storage space pass lib_type None
        """
        StorageSetup._check_persistance_access_asked(storage_space, confirm_persistent_storage_need)
        prefix = StorageSetup._create_prefix(storage_space, add_to_prefix)
        return f"lmdb://{tempfile.gettempdir()}/benchmarks_{prefix}" 

    @classmethod
    def get_arctic_uri(cls, storage: Storage, storage_space: StorageSpace, add_to_prefix: str = None, confirm_persistent_storage_need: bool = False) -> str:
        if storage == Storage.AMAZON:
            arctic_url = StorageSetup._get_aws_s3_arctic_uri(storage_space, add_to_prefix, confirm_persistent_storage_need)
        elif storage == Storage.LMDB:
            arctic_url = StorageSetup._get_lmdb_arctic_uri(storage_space, add_to_prefix, confirm_persistent_storage_need)
        else:
            raise Exception("Unsupported storage type :", self.storage)
        return arctic_url


class LibraryManager:

    def __init__(self, storage: Storage, name_benchmark: str, library_options: LibraryOptions = None) :
        """
        Populate `name_benchamrk` to get separate modifiable space for each benchmark
        """
        self.storage: Storage = storage
        self.library_options: LibraryOptions = library_options
        self.name_benchmark: str = name_benchmark
        self._test_mode = False
        self._uris_cache: List[str] = []
        self._ac_cache = {}
        StorageSetup()

    def log_info(self):
        logger = get_console_logger()
        mes = f"{self} information: \n"
        for key in self._ac_cache.keys():
            mes += f"Arctic URI: {key}"
        logger.info(mes) 

    # Currently we're using the same arctic client for both persistant and modifiable libraries.
    # We might decide that we want different arctic clients (e.g. different buckets) but probably not needed for now.
    def _get_arctic_client(self) -> Arctic:
        lib_type = StorageSpace.PERSISTENT
        if self._test_mode == True:
            lib_type = StorageSpace.TEST
        return self.__get_arctic_client_internal(lib_type, None, 
                                                  confirm_persistent_storage_need = True)

    def _get_arctic_client_modifiable(self) -> Arctic:
        add_to_prefix = f"{self.name_benchmark}/{StorageSetup.get_machine_id()}"
        return self.__get_arctic_client_internal(StorageSpace.MODIFIABLE, add_to_prefix, 
                                                  confirm_persistent_storage_need = False)

    def __get_arctic_client_internal(self, storage_space: StorageSpace, add_to_prefix: str, 
                                      confirm_persistent_storage_need: bool = False) -> Arctic:
        arctic_url = StorageSetup.get_arctic_uri(self.storage, storage_space, add_to_prefix,confirm_persistent_storage_need)
        ac =  self._ac_cache.get(arctic_url, None)
        if ac is None:
            ac = Arctic(arctic_url)    
        self._ac_cache[arctic_url] = ac
        return ac    


    def get_library_name(self, library_type: LibraryType, lib_name_suffix):
        if library_type == LibraryType.PERSISTENT:
            return f"{library_type.value}_{self.name_benchmark}_{lib_name_suffix}"
        if library_type == LibraryType.MODIFIABLE:
            # We want the modifiable libraries to be unique per process/ benchmark class. We embed this deep in the name
            return f"{library_type.value}_{self.name_benchmark}_{os.getpid()}_{lib_name_suffix}"

    def get_library(self, library_type : LibraryType, lib_name_suffix : str = "") -> Library:
        lib_name = self.get_library_name(library_type, lib_name_suffix)
        if library_type == LibraryType.PERSISTENT:
           return self._get_arctic_client().get_library(lib_name, create_if_missing=True)
        elif library_type == LibraryType.MODIFIABLE:
           return self._get_arctic_client_modifiable().get_library(lib_name, create_if_missing=True, 
                                                        library_options= self.library_options)
        else:
            raise Exception(f"Unsupported library type: {library_type}")

    def has_library(self, library_type : LibraryType, lib_name_suffix : str = "") -> Library:
        lib_name = self.get_library_name(library_type, lib_name_suffix)
        if library_type == LibraryType.PERSISTENT:
           return self._get_arctic_client().has_library(lib_name)
        elif library_type == LibraryType.MODIFIABLE:
           return self._get_arctic_client_modifiable().has_library(lib_name)        
        else:
            raise Exception(f"Unsupported library type: {library_type}")

    def clear_all_modifiable_libs_from_this_process(self):
        ac = self._get_arctic_client_modifiable()
        lib_names = set(ac.list_libraries())
        to_deletes = [lib_name for lib_name in lib_names 
                      if f"_{os.getpid()}_" in lib_name]
        for to_delete in to_deletes:
            ac.delete_library(to_delete)

    def clear_all_modifiable_libs(self):
        ac = self._get_arctic_client_modifiable()
        lib_names = set(ac.list_libraries())
        for to_delete in lib_names:
            ac.delete_library(to_delete)

    @classmethod
    def clear_all_test_libs(cls, storage_type: Storage):
        lm = LibraryManager(storage_type, "not needed")
        lm._test_mode = True
        ac = lm._get_arctic_client()
        lib_names = set(ac.list_libraries())
        for to_delete in lib_names:
            ac.delete_library(to_delete)            



# It is quite clear what is this responsible for: only dataframe generation
# Using such an abstraction can help us deduplicate the dataframe generation code between the different `EnvironmentSetup`s
# Note: We use a class instead of a generator function to allow caching of dataframes in the state
class DataFrameGenerator(ABC):

    def __init__(self):
        super().__init__()
        self.initial_timestamp = pd.Timestamp("1-1-2000")
        self.freq = 's'

    @abstractmethod
    def get_dataframe(self, number_rows, number_columns, **kwargs) -> pd.DataFrame:
        pass


class VariableSizeDataframe(DataFrameGenerator):

    def __init__(self):
        super().__init__()
        self.wide_dataframe_generation_threshold = 400
    
    def get_dataframe(self, number_rows, number_columns):
        if number_columns < self.wide_dataframe_generation_threshold:
            df = (DFGenerator.generate_normal_dataframe(num_rows=number_rows, num_cols=number_columns,
                                                      freq = self.freq, start_time=self.initial_timestamp))
        else:
            # The wider the dataframe the more time it needs to generate per row
            # This algo is much better for speed with wide dataframes
            df = (DFGenerator.generate_wide_dataframe(num_rows=number_rows, num_cols=number_columns,
                                                      num_string_cols=200, 
                                                      freq = self.freq, start_time=self.initial_timestamp))
        return df


class LibraryPopulationPolicy:

    def __init__(self, parameters: List[int], logger: logging.Logger, df_generator: DataFrameGenerator = VariableSizeDataframe()):
        """
        By default library population policy uses a list of number of rows per symbol, where numbers would be unique. 
        It will generate same number of symbols as the length of the list and each symbol will have the same number of 
        rows as the index of the number.

        It is possible to also define a custom DataFrameGenerator specific for test needs. Default one is generating dataframe 
        with random data and you can specify any number of columns and rows

        It is possible to also configure through methods snapshots and versions to be created and metadata to be set to them or not

        Example A:
            LibraryPopulationPolicy([10,20], some_logger).set_columns(5)
            This configures generation of 2 symbols with 10 and 20 rows. The number of rows can later be used to get symbol name.
            Note that this defined that all symbols will have fixed number of columns = 5

        Example B:
            LibraryPopulationPolicy([10,20], some_logger).use_parameters_are_columns().set_rows(3) - 
            This configures generation of 2 symbols with 10 and 20 columns. The number columns can later be used to get symbol name.
            Note that this defined that all symbols will have fixed number of rows = 3

        Example C: Populating library with many identical symbols
            LibraryPopulationPolicy([10] * 10, some_logger).use_auto_increment_index().set_columns(30) - 
            This configures generation of 10 symbols with 10 rows each. Also instructs that the symbol names will be constructed 
            with auto incrementing index - you can access each symbol using its index 0-9
        """
        self.logger = logger
        self.df_generator = df_generator
        self.parameters = parameters
        # defines if parameters is list of row numbers or column numbers
        self.parameters_is_number_rows_list = True 
        self.number_rows = 1
        self.number_columns = 1
        self.with_metadata = False
        self.versions_max = 1
        self.mean = 1
        self.with_snapshot = False
        self.symbol_fixed_str = ""
        self.index_is_auto_increment = False

    def set_parameters(self, parameters: List[int]) -> 'LibraryPopulationPolicy':
        """
        Useful for passing different sets of parameters populating many libraries
        with unique number of symbols
        """
        self.parameters = parameters
        return self

    def set_symbol_fixed_str(self, symbol_fixed_str) -> 'LibraryPopulationPolicy':
        """
        Whenever you want to use one library and have different policies creating symbols
        in it specify unique meaningful fixed string that will become part of the name
        of the generated symbol
        """
        self.symbol_fixed_str = symbol_fixed_str
        return self

    def use_parameters_are_columns(self) -> 'LibraryPopulationPolicy':
        """
        By default the parameter list is considered as 
        """
        self.parameters_is_number_rows_list = False
        return self

    def set_rows(self, number_rows) -> 'LibraryPopulationPolicy':
        """
        Sets number of rows if we are using fixed number of rows
        """
        self.number_rows = number_rows
        return self

    def set_columns(self, number_columns) -> 'LibraryPopulationPolicy':
        """
        Sets number of columns if we are using fixed number of columns
        """
        self.number_columns = number_columns
        return self
    
    def generate_versions(self, versions_max, mean) -> 'LibraryPopulationPolicy':
        """
        For each symbol maximum `versions_max` version and mean value `mean`
        """
        self.versions_max = versions_max
        self.mean = mean
        return self
    
    def generate_snapshots(self) -> 'LibraryPopulationPolicy':
        """
        Will create snapshots for each symbol. For each version of a symbol 
        will be added one snapshot
        """
        self.with_snapshot = True
        return self
    
    def generate_metadata(self) -> 'LibraryPopulationPolicy':
        """
        All snapshots and symbols will have metadata
        """
        self.with_metadata = True
        return self
    
    def use_auto_increment_index(self) -> 'LibraryPopulationPolicy':
        """
            During population of symbols will use auto increment index
            for symbol names instead of using the current value of the 
            parameters list
        """
        self.index_is_auto_increment = True
        return self

    def get_symbol_name(self, index: int, optional_fixed_str: str = None) -> str:
        """
        This method is used during population and should be used by readers also
        Normally the index will be one of:
         - autoincrement - when many symbols of same size are generated
         - number of rows (columns) - when parameters is list of row/column symbol sizes
        """
        fixed_part = self.symbol_fixed_str if optional_fixed_str is None else optional_fixed_str
        return f"symbol_{fixed_part}_{index}"

    def populate_library(self, lib: Library):
        assert lib is not None
        self.logger.info(f"Populating library {lib}")
        start_time = time.time()
        df_generator = self.df_generator
        meta = None if not self.with_metadata else self._generate_metadata()
        versions_list = self._get_versions_list(len(self.parameters))
        index = 0
        for param_value in self.parameters:
            versions = versions_list[index]
            
            if self.index_is_auto_increment:
                symbol = self.get_symbol_name(index)
            else:
                symbol = self.get_symbol_name(param_value)

            if self.parameters_is_number_rows_list:
                df = df_generator.get_dataframe(number_rows=param_value, number_columns=self.number_columns)
            else:
                df = df_generator.get_dataframe(number_rows=self.number_rows, number_columns=param_value)
            self.logger.info(f"Dataframe generated [{df.shape}]")

            for ver in range(versions):
                lib.write(symbol=symbol, data=df, metadata=meta)

                if self.with_snapshot:
                    snapshot_name = f"snap_{symbol}_{ver}"
                    lib.snapshot(snapshot_name, metadata=meta)
                
            index += 1
        self.logger.info(f"Population completed for: {time.time() - start_time}")

    def _get_versions_list(self, number_symbols: int) -> List[np.int64]:
        if self.versions_max == 1:
            versions_list = [1] * number_symbols
        else:
            versions_list = ListGenerators.generate_random_list_with_mean(
                number_elements=number_symbols,
                specified_mean=self.mean, 
                value_range=(1, self.versions_max), 
                seed=365)
        return versions_list

    def _generate_metadata(self):
        return DFGenerator.generate_random_dataframe(rows=3, cols=10).to_dict()                 


def populate_library_if_missing(manager: LibraryManager, policy: LibraryPopulationPolicy, lib_type: LibraryType, lib_name_suffix: str = ""):
    assert manager is not None
    name = manager.get_library_name(lib_type, lib_name_suffix)
    if not manager.has_library(lib_type, lib_name_suffix):
        populate_library(manager=manager, policy=policy, lib_type=lib_type, lib_name_suffix=lib_name_suffix)
    else:
        policy.logger.info(f"Existing library has been found {name}. Will be reused")      

def populate_library(manager: LibraryManager, policy: LibraryPopulationPolicy, lib_type: LibraryType, lib_name_suffix: str = ""):
    assert manager is not None
    lib = manager.get_library(lib_type, lib_name_suffix)
    policy.populate_library(lib)
