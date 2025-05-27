import copy
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
import inspect
import logging
import multiprocessing
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
GCP_S3_DEFAULT_BUCKET = 'arcticdb-asv-real-storage'


class GitHubSanitizingHandler(logging.StreamHandler):
    """
    The handler sanitizes messages only when execution is in GitHub
    """

    def emit(self, record: logging.LogRecord):
        # Sanitize the message here
        record.msg = self.sanitize_message(record.msg)
        super().emit(record)

    @staticmethod
    def sanitize_message(message: str) -> str:
        if (os.getenv("GITHUB_ACTIONS") == "true") and isinstance(message, str):
            # Use regex to find and replace sensitive access keys
            sanitized_message = re.sub(r'(secret=)[^\s&]+', r'\1***', message)
            sanitized_message = re.sub(r'(access=)[^\s&]+', r'\1***', sanitized_message)
            return sanitized_message
        return message


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
    console_handler = GitHubSanitizingHandler()
    console_handler.setLevel(logLevel)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    loggers[name] = logger
    return logger


class Storage(Enum):
    AMAZON = 1
    LMDB = 2
    GOOGLE = 3


class StorageSpace(Enum):
    """
    Defines the type of storage space.
    Will be used as prefixes to separate shared storage 

    In the bucket this class defined through prefixes 2 shared spaces:
     - persistent
     - test
    
    then for each client machine there will be separate space for temporary
    modifiable libraries, and the prefix will be machine id (see how it is produced below)
    """
    PERSISTENT = "PERMANENT_LIBRARIES"
    MODIFIABLE = "MODIFIABLE_LIBRARIES"
    TEST = "TESTS_LIBRARIES"


class LibraryType(Enum):
    # READ_ONLY or READ_WRITE
    PERSISTENT = "PERMANENT"
    MODIFIABLE = "MODIFIABLE"


class StorageSetup:
    '''
    Defined special one time setup for real storages.
    Place here what is needed for proper initialization
    of each storage

    Abstracts storage space allocation from how user access it
    '''
    _instance = None
    _aws_default_factory: BaseS3StorageFixtureFactory = None

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

            # GCP initialization (quick&dirty)
            cls._gcp_secret = os.getenv("ARCTICDB_REAL_GCP_SECRET_KEY")
            cls._gcp_access = os.getenv("ARCTICDB_REAL_GCP_ACCESS_KEY")
            cls._gcp_bucket = GCP_S3_DEFAULT_BUCKET
   
    @classmethod
    def get_machine_id(cls):
        """
        Returns machine id, or id specified through environments variable (for github)
        """
        return os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX", socket.gethostname())

    @classmethod
    def _create_prefix(cls, storage_space: StorageSpace, add_to_prefix: str ) -> str:
        def is_valid_string(s: str) -> bool:
            return bool(s and s.strip())
        
        def create_prefix(mandatory_part:str, optional:str) -> str:
            if is_valid_string(add_to_prefix):
                return f"{mandatory_part}/{optional if optional is not None else ''}"
            else:
                return mandatory_part
            
        if storage_space == StorageSpace.MODIFIABLE:
            return create_prefix(cls.get_machine_id(), add_to_prefix)
        else:
            return create_prefix(storage_space.value, add_to_prefix)   

    
    @classmethod
    def _check_persistance_access_asked(cls, storage_space: StorageSpace, confirm_persistent_storage_need: bool = False) -> str:
        assert cls._aws_default_factory, "Environment variables not initialized (ARCTICDB_REAL_S3_ACCESS_KEY,ARCTICDB_REAL_S3_SECRET_KEY)"
        if storage_space == StorageSpace.PERSISTENT:
            assert confirm_persistent_storage_need, f"Use of persistent store not confirmed!"
    
    @classmethod
    def get_arctic_uri(cls, storage: Storage, storage_space: StorageSpace, add_to_prefix: str = None, 
                       confirm_persistent_storage_need: bool = False) -> str:
        StorageSetup._check_persistance_access_asked(storage_space, confirm_persistent_storage_need)
        prefix = StorageSetup._create_prefix(storage_space, add_to_prefix)
        if storage == Storage.AMAZON:
            cls._aws_default_factory.default_prefix = prefix
            return cls._aws_default_factory.create_fixture().arctic_uri
        elif storage == Storage.LMDB:
            return f"lmdb://{tempfile.gettempdir()}/benchmarks_{prefix}" 
        elif storage == Storage.GOOGLE:
            s = cls._gcp_secret
            a = cls._gcp_access
            return f"gcpxml://storage.googleapis.com:{cls._gcp_bucket}?access={a}&secret={s}&path_prefix={prefix}"
        else:
            raise Exception("Unsupported storage type :", storage)


class TestLibraryManager:
    """
    This class is a thin wrapper around Arctic class. Its goal is to provide natural user
    experience while hiding the specifics associated with management of a shared storage
    space. It does that by separating a common storage in 2 main parts:

     - persistent storage space - where all client have access to each other's libraries.
       That is the space of shared libraries among all instances. We could refer it as production
       space for libraries. As such it needs to be protected only for code that is production ready.
       Therefore a persistent client can be set to be in test mode. In this mode the development
       and troubleshooting process should happen. In that mode, the client will work not in
       the production shared space but in test shared space which is having same characteristics with 
       production one

    - modifiable (or client private space). This space is a separate space from persistent one. In this 
      space each physical machine has private subspace which isolates its work from others. Thus all work 
      there is seen only by this machine. That allows easy management of this space - the machine can 
      easily manage its data - creation and deletion of libraries. Still this machine space is shared among
      different tests on the same machine. In order not to conflict with each other each test/benchmark can
      and in fact should create its unique label. This label will be part of the prefix of the library.
      Thus each test in fact should have access to only its libs. One test can spawn multiple process.
      Thus each process needs isolation from other processes - create/access/delete its own libraries. 
      Therefore the library prefix carries also process id. 

    As this structure is build on one shared storage space there needs to be enough protection - such that
    no client have access to other space unintentionally. Therefore this wrapper object provides
    all basic operations for libraries. Some of arctic methods are hidden or not implemented intentionally 
    as their would be no practical need of them yet. Once such need arises they would need to be implemented
    providing same user experience and philosophy

    The class provides limited set of functions which are more than enough to make any 
    end2end tests with ASV or other frameworks. The only thing it discourages is use of
    Arctic directly. That is with single goal to protect shared storage from unintentional damage.
    All work could and should be done through `get_library` function. There are methods for setting 
    library options, and additional `has_library` method that would eliminate the need of direct 
    use of Arctic object.

    As there could be very few cases that could require use of Arctic object directly, such protected 
    methods do exist, but their use makes any test potentially either unsafe or one that should be 
    handled with extra care

    The class provides additional 2 class methods for removing data from storage, which should be handled 
    with care. As they create always new connection any concurrent modifications with them and running 
    tests would most probably end with errors. 

    """

    def __init__(self, storage: Storage, name_benchmark: str, library_options: LibraryOptions = None) :
        """
        Populate `name_benchamrk` to get separate modifiable space for each benchmark
        """
        self.storage: Storage = storage
        self.library_options: LibraryOptions = library_options
        self.name_benchmark: str = name_benchmark
        self._test_mode = False
        self._ac_cache = {}
        StorageSetup()

    def log_info(self):
        logger = get_console_logger()
        if len(self._ac_cache) < 2:
            self._get_arctic_client_persistent() # Forces uri generation
            self._get_arctic_client_modifiable() # Forces uri generation
        mes = f"{self} arcticdb URI information for this test: \n"
        for key in self._ac_cache.keys():
            mes += f"arcticdb URI: {key}"
        logger.info(mes) 

    # Currently we're using the same arctic client for both persistant and modifiable libraries.
    # We might decide that we want different arctic clients (e.g. different buckets) but probably not needed for now.
    def _get_arctic_client_persistent(self) -> Arctic:
        storage_space = StorageSpace.PERSISTENT
        if self._test_mode == True:
            storage_space = StorageSpace.TEST
        return self.__get_arctic_client_internal(storage_space, 
                                                  confirm_persistent_storage_need = True)

    def _get_arctic_client_modifiable(self) -> Arctic:
        return self.__get_arctic_client_internal(StorageSpace.MODIFIABLE, 
                                                  confirm_persistent_storage_need = False)

    def __get_arctic_client_internal(self, storage_space: StorageSpace, 
                                     confirm_persistent_storage_need: bool = False) -> Arctic:
        arctic_url = StorageSetup.get_arctic_uri(self.storage, storage_space, None, confirm_persistent_storage_need)
        ac =  self._ac_cache.get(arctic_url, None)
        if ac is None:
            ac = Arctic(arctic_url)    
        self._ac_cache[arctic_url] = ac
        return ac    
    
    def set_test_mode(self) -> 'TestLibraryManager':
        self._test_mode = True
        return self

    def get_library_name(self, library_type: LibraryType, lib_name_suffix: str = ""):
        if library_type == LibraryType.PERSISTENT:
            return f"{library_type.value}_{self.name_benchmark}_{lib_name_suffix}"
        if library_type == LibraryType.MODIFIABLE:
            # We want the modifiable libraries to be unique per process/ benchmark class. We embed this deep in the name
            return f"{library_type.value}_{self.name_benchmark}_{os.getpid()}_{lib_name_suffix}"

    def get_library(self, library_type : LibraryType, lib_name_suffix : str = "") -> Library:
        lib_name = self.get_library_name(library_type, lib_name_suffix)
        if library_type == LibraryType.PERSISTENT:
           # TODO comment to make the persistent library read only.
           # It is possible to expose this from the C++ layer and it would make working with them much safer. 
           # (We would only need to add an overwrite flag for populate_library_if_missing)
           return self._get_arctic_client_persistent().get_library(lib_name, create_if_missing=True)
        elif library_type == LibraryType.MODIFIABLE:
           return self._get_arctic_client_modifiable().get_library(lib_name, create_if_missing=True, 
                                                        library_options= self.library_options)
        else:
            raise Exception(f"Unsupported library type: {library_type}")

    def has_library(self, library_type : LibraryType, lib_name_suffix : str = "") -> Library:
        lib_name = self.get_library_name(library_type, lib_name_suffix)
        if library_type == LibraryType.PERSISTENT:
           return self._get_arctic_client_persistent().has_library(lib_name)
        elif library_type == LibraryType.MODIFIABLE:
           return self._get_arctic_client_modifiable().has_library(lib_name)        
        else:
            raise Exception(f"Unsupported library type: {library_type}")

    def clear_all_modifiable_libs_from_this_process(self):
        """
        This method is the one to use primarily in `teardown` methods
        """
        self.__clear_all_libs(f"{LibraryType.MODIFIABLE.value}_{self.name_benchmark}_{os.getpid()}_")

    def clear_all_benchmark_libs(self):
        """
        Clears only libraries for this benchmark.
        Not advised to use in `teardown`, but very wise for `setup_cache`
        """
        self.__clear_all_libs()

    def __clear_all_libs(self, name_starts_with: str = None):
        ac = self._get_arctic_client_modifiable()
        libs_to_delete = set(ac.list_libraries())
        if name_starts_with is not None:
            libs_to_delete = [lib_name for lib_name in libs_to_delete 
                      if lib_name.startswith(name_starts_with)]
        for lib_name in libs_to_delete:
            ac.delete_library(lib_name)

    @classmethod
    def remove_all_modifiable_libs_for_machine(cls, storage_type: Storage):
        """
        Eventually will be used for wiping out scripts after all tests on machine

        MOTE: Potentially dangerous operation, invoke only when no other
              test processes run on the shared storage
        """
        lm = TestLibraryManager(storage_type, "not needed")
        ac = lm._get_arctic_client_modifiable()
        cls.__remove_all_test_libs(ac, StorageSetup.get_machine_id())
            
    @classmethod
    def remove_all_test_libs(cls, storage_type: Storage):
        """
        A scheduled job for wiping out test storage space over weekends is good candidate 

        MOTE: Potentially dangerous operation, invoke only when no other
              test processes run on the shared storage
        """
        # The following call makes persistent library test library
        lm = TestLibraryManager(storage_type, "not needed").set_test_mode() 
        ac = lm._get_arctic_client_persistent()
        cls.__remove_all_test_libs(ac, StorageSpace.TEST.value)

    @classmethod
    def __remove_all_test_libs(cls, ac: Arctic, uri_str_to_confirm: str):
        assert uri_str_to_confirm in ac.get_uri(), f"Expected string [{uri_str_to_confirm}] not found in uri : {ac.get_uri()}"
        lib_names = set(ac.list_libraries())
        for to_delete in lib_names:
            ac.delete_library(to_delete)  
            get_console_logger().info(f"Delete library [{to_delete}] from storage space having [{uri_str_to_confirm}]")          
        assert len(ac.list_libraries()) == 0, f"All libs for storage space [{uri_str_to_confirm}] deleted"        

    def remove_all_persistent_libs_for_this_test(self):
        """
        This will remove all persistent libraries for this test from the persistent storage
        Therefore use wisely only when needed (like change of parameters for tests)
        """
        name_prefix = f"{LibraryType.PERSISTENT.value}_{self.name_benchmark}"
        ac = self._get_arctic_client_persistent()
        lib_names = set(ac.list_libraries())
        for to_delete in lib_names:
            if to_delete.startswith(name_prefix):
                ac.delete_library(to_delete)  
                get_console_logger().info(f"Delete library [{to_delete}]")          


class DataFrameGenerator(ABC):

    def __init__(self):
        super().__init__()
        self.initial_timestamp = pd.Timestamp("1-1-2000")
        self.freq = 's'

    @abstractmethod
    def get_dataframe(self, number_rows: int, number_columns:int, **kwargs) -> pd.DataFrame:
        pass


class VariableSizeDataframe(DataFrameGenerator):

    def __init__(self):
        super().__init__()
        self.wide_dataframe_generation_threshold = 400
        
    def get_dataframe(self, number_rows:int, number_columns:int, 
                            start_timestamp: pd.Timestamp = None,
                            freq: Union[str , timedelta , pd.Timedelta , pd.DateOffset] = None, seed = 888):
        start_timestamp = self.initial_timestamp if start_timestamp is None else start_timestamp
        freq = self.freq if freq is None else freq
        if number_columns < self.wide_dataframe_generation_threshold:
            df = (DFGenerator.generate_normal_dataframe(num_rows=number_rows, num_cols=number_columns,
                                                      freq = freq, start_time=start_timestamp, seed=seed))
        else:
            # The wider the dataframe the more time it needs to generate per row
            # This algo is much better for speed with wide dataframes
            df = (DFGenerator.generate_wide_dataframe(num_rows=number_rows, num_cols=number_columns,
                                                      num_string_cols=200, 
                                                      freq = freq, start_time=start_timestamp, seed=seed))
        return df


class LibraryPopulationPolicy:
    """
    By default library population policy uses a list of number of rows per symbol, where numbers would be unique. 
    It will generate same number of symbols as the length of the list and each symbol will have the same number of 
    rows as the index of the number.

    It is possible to also define a custom DataFrameGenerator specific for test needs. Default one is generating dataframe 
    with random data and you can specify any number of columns and rows

    It is possible to also configure through methods snapshots and versions to be created and metadata to be set to them or not

    Example A:
        LibraryPopulationPolicy(some_logger).set_parameters([10,20], 5)
        This configures generation of 2 symbols with 10 and 20 rows. The number of rows can later be used to get symbol name.
        Note that this defined that all symbols will have fixed number of columns = 5

    Example B:
        LibraryPopulationPolicy(some_logger).set_parameters(3, [10,20]) - 
        This configures generation of 2 symbols with 10 and 20 columns. The number columns can later be used to get symbol name.
        Note that this defined that all symbols will have fixed number of rows = 3

    Example C: Populating library with many identical symbols
        LibraryPopulationPolicy(some_logger).use_auto_increment_index().set_parameters([10] * 10, 30) - 
        This configures generation of 10 symbols with 10 rows each. Also instructs that the symbol names will be constructed 
        with auto incrementing index - you can access each symbol using its index 0-9
    """

    """
        TODO: if this class needs to be inherited or changed significantly consider this task:9098760503
    """
    

    def __init__(self, logger: logging.Logger, df_generator: DataFrameGenerator = VariableSizeDataframe()):
        self.logger: logging.Logger = logger
        self.df_generator = df_generator
        self.number_rows: Union[int, List[int]] = [10]
        self.number_columns: Union[int, List[int]] = 10
        self.with_metadata: bool = False
        self.versions_max: int = 1
        self.mean: int = 1
        self.with_snapshot: bool = False
        self.symbol_fixed_str: str = ""
        self.index_is_auto_increment: bool = False

    def set_parameters(self, number_rows: Union[int, List[int]], 
                       number_columns: Union[int, List[int]] = 10) -> 'LibraryPopulationPolicy':
        """
        Set one of the parameter to a fixed value (rows or cols).
        The other parameter should be list of the sizes (rows or cols) of each of the symbols
        that will be created.
        The number of symbols will be exactly the same as the length of the given list
        """
        assert isinstance(number_rows, list) ^ isinstance(number_columns, list), "Only one of parameters can be list"
        self.number_rows = number_rows
        self.number_columns = number_columns
        return self

    def set_symbol_fixed_str(self, symbol_fixed_str: str) -> 'LibraryPopulationPolicy':
        """
        Whenever you want to use one library and have different policies creating symbols
        in it specify unique meaningful fixed string that will become part of the name
        of the generated symbol
        """
        self.symbol_fixed_str = symbol_fixed_str
        return self

    def generate_versions(self, versions_max: int, mean: int) -> 'LibraryPopulationPolicy':
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

    def log(self, message):
        if self.logger is not None:
            self.logger.info(message)
    
    def populate_library(self, lib: Library):

        def is_number_rows_list():
            return isinstance(self.number_rows, list)

        assert lib is not None
        self.log(f"Populating library {lib}")
        start_time = time.time()
        df_generator = self.df_generator
        meta = None if not self.with_metadata else self._generate_metadata()
        if is_number_rows_list():
            list_parameter =  self.number_rows
            fixed_parameter = self.number_columns
        else:
            list_parameter =  self.number_columns
            fixed_parameter = self.number_rows
        versions_list = self._get_versions_list(len(list_parameter))
        for index, param_value in enumerate(list_parameter):
            versions = versions_list[index]
            
            if self.index_is_auto_increment:
                symbol = self.get_symbol_name(index)
            else:
                symbol = self.get_symbol_name(param_value)

            if is_number_rows_list():
                df = df_generator.get_dataframe(number_rows=param_value, number_columns=fixed_parameter)
            else:
                df = df_generator.get_dataframe(number_rows=fixed_parameter, number_columns=param_value)
            self.log(f"Dataframe generated [{df.shape}]")

            for ver in range(versions):
                lib.write(symbol=symbol, data=df, metadata=meta)

                if self.with_snapshot:
                    snapshot_name = f"snap_{symbol}_{ver}"
                    lib.snapshot(snapshot_name, metadata=meta)
                
        self.log(f"Population completed for: {time.time() - start_time}")

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


def populate_library_if_missing(manager: TestLibraryManager, policy: LibraryPopulationPolicy, lib_type: LibraryType, lib_name_suffix: str = ""):
    assert manager is not None
    name = manager.get_library_name(lib_type, lib_name_suffix)
    if not manager.has_library(lib_type, lib_name_suffix):
        populate_library(manager=manager, policy=policy, lib_type=lib_type, lib_name_suffix=lib_name_suffix)
    else:
        policy.log(f"Existing library has been found {name}. Will be reused")      


def populate_library(manager: TestLibraryManager, policy: LibraryPopulationPolicy, lib_type: LibraryType, lib_name_suffix: str = ""):
    assert manager is not None
    lib = manager.get_library(lib_type, lib_name_suffix)
    policy.populate_library(lib)


class SequentialDataframesGenerator:

    def __init__(self, df_generator: DataFrameGenerator = VariableSizeDataframe()):
        self.df_generator = df_generator

    def generate_sequential_dataframes(self, 
                               number_data_frames: int, 
                               number_rows: int, 
                               number_columns: int = 10,
                               start_timestamp: pd.Timestamp = None,
                               freq: str = 's') -> List[pd.DataFrame]:
        """
        Generates specified number of data frames each having specified number of rows and columns
        The dataframes are in chronological order one after the other. Date range starts with the specified 
        initial timestamp setup and frequency
        """
        cache = []
        timestamp_number = TimestampNumber.from_timestamp(start_timestamp, freq)

        for i in range(number_data_frames):
            df = self.df_generator.get_dataframe(number_rows=number_rows, number_columns=number_columns,
                                                 start_timestamp= timestamp_number.to_timestamp(),
                                                 freq = freq)
            cache.append(df)
            timestamp_number.inc(df.shape[0])
        
        return cache
    
    def get_first_and_last_timestamp(self, sequence_df_list: List[pd.DataFrame]) -> List[pd.Timestamp]:
        """
        Returns first and last timestamp of the list of indexed dataframes
        """
        assert len(sequence_df_list) > 0
        start = sequence_df_list[0].index[0]
        last = sequence_df_list[-1].index[-1]
        return (start, last)
    
    def get_next_timestamp_number(self, sequence_df_list: List[pd.DataFrame], freq: str) -> TimestampNumber:
        """
        Returns next timestamp after the last timestamp in passed sequence of 
        indexed dataframes.
        """
        last = self.get_first_and_last_timestamp(sequence_df_list)[1]
        next = TimestampNumber.from_timestamp(last, freq) + 1
        return next


class TestsForTestLibraryManager:
    """
    This class contains tests for the framework. All changes to the framework
    should be done with running those tests at the end
    """

    @classmethod
    def test_test_mode(cls):
        """
        Examines workflow of operations when test mode is set.
        In that mode all persistent space storage requests are executed
        in the test space, protecting persistent space from damage
        """
        symbol = "symbol"
        storage = Storage.AMAZON
        logger = get_console_logger()
        tlm = TestLibraryManager(storage, "TEST_TEST_MODE").set_test_mode()
        df = DFGenerator(10).add_int_col("int").generate_dataframe()
        TestLibraryManager.remove_all_test_libs(storage)
        TestLibraryManager.remove_all_modifiable_libs_for_machine(storage)
        ac = tlm._get_arctic_client_persistent()
        assert StorageSpace.TEST.value in ac.get_uri()
        logger.info(f"Arctic uri: {ac.get_uri()}")
        assert len(ac.list_libraries()) == 0
        lib = tlm.get_library(LibraryType.PERSISTENT) # This is actually going to be test lib
        lib.write(symbol, df)
        assert symbol in lib.list_symbols()
        # This is going to be modifiable lib.
        assert len(tlm._get_arctic_client_modifiable().list_libraries()) == 0

    @classmethod
    def test_modifiable_access(cls):
        """
        Examines operations for modifiable workflow. When storage operation 
        is requested there it is executed in special space unique for each machine/github runner
        This space hosts the libraries created for all benchmarks and all process on that that machine
        Part of name of each library is the name of benchmark and process that created it.
        There are 2 operations to clear such libraries - to clear all libraries for
        certain benchmark and to clear all libraries for certain benchmark and process
        """
        symbol = "symbol"
        storage = Storage.AMAZON
        logger = get_console_logger()
        tlm = TestLibraryManager(storage, "TEST_MODIFIABLE_ACCESS").set_test_mode()
        df = DFGenerator(10).add_int_col("int").generate_dataframe()

        def create_lib(suffix : str = "") -> Library:
            lib = tlm.get_library(LibraryType.MODIFIABLE, suffix) # This is actually going to be test lib
            lib.write(symbol, df)
            return (lib, tlm.get_library_name(LibraryType.MODIFIABLE, suffix))

        ac = tlm._get_arctic_client_modifiable()
        assert StorageSetup.get_machine_id() in ac.get_uri()
        logger.info(f"Arctic uri: {ac.get_uri()}")
        logger.info(f"Modifiable libraries {tlm._get_arctic_client_modifiable().list_libraries()}")
        TestLibraryManager.remove_all_test_libs(storage)
        TestLibraryManager.remove_all_modifiable_libs_for_machine(storage)
        assert len(ac.list_libraries()) == 0, "No modifiable libs"
        lib, lib_name = create_lib()
        assert symbol in lib.list_symbols(), "Symbol created"
        assert lib_name in ac.list_libraries(), "Library name found among others in modifiable space"
        assert lib_name not in tlm._get_arctic_client_persistent().list_libraries(), "Library name not in persistent space"
        # Following operation is unsafe as it creates another client
        # Thus `tlm` object library manager will be out of sync. Therefore all new libraries that will create
        # will be real new libraries
        TestLibraryManager.remove_all_modifiable_libs_for_machine(storage)
        assert lib_name not in ac.list_libraries(), "Library name not anymore in modifiable space"
        # We could not create library with same suffix, because another client has deleted 
        # the original and LibraryManager in original connection is not notified for that
        # so we create library with different suffix
        lib, lib_name = create_lib("2")
        assert lib_name in ac.list_libraries(), "Library name found among others in modifiable space"
        tlm.clear_all_benchmark_libs()
        assert lib_name not in tlm._get_arctic_client_persistent().list_libraries(), "Library name not in persistent space"
        assert lib_name not in ac.list_libraries(), "Library name not anymore in modifiable space"
        # The creation of library with same suffix is now possible as client connections are cached
        # for `tlm` object
        lib, lib_name = create_lib("2") 
        assert lib_name in ac.list_libraries(), "Library name found among others in modifiable space"
        tlm.clear_all_modifiable_libs_from_this_process()
        assert lib_name not in tlm._get_arctic_client_persistent().list_libraries(), "Library name not in persistent space"
        assert lib_name not in ac.list_libraries(), "Library name not anymore in modifiable space"

    @classmethod
    def test_library_populator(cls):
        storage = Storage.GOOGLE
        logger = get_console_logger()
        tlm = TestLibraryManager(storage, "Library_populator").set_test_mode()
        lib_name_suffix = "mylib"

        TestLibraryManager.remove_all_test_libs(storage)

        policy = LibraryPopulationPolicy(None).set_parameters([2, 3], 5)
        populate_library(tlm, policy, LibraryType.PERSISTENT, lib_name_suffix)
        lib = tlm.get_library(LibraryType.PERSISTENT, lib_name_suffix)
        df: pd.DataFrame = lib.read(policy.get_symbol_name(2)).data
        logger.info(f"{df}")
        assert df.shape[0] == 2
        assert df.shape[1] == 5
        df: pd.DataFrame = lib.read(policy.get_symbol_name(3)).data
        logger.info(f"{df}")
        assert df.shape[0] == 3
        assert df.shape[1] == 5

        policy = LibraryPopulationPolicy(None).set_parameters(10, [6, 7])
        populate_library(tlm, policy, LibraryType.PERSISTENT, lib_name_suffix)
        lib = tlm.get_library(LibraryType.PERSISTENT, lib_name_suffix)
        df: pd.DataFrame = lib.read(policy.get_symbol_name(6)).data
        logger.info(f"{df}")
        assert df.shape[0] == 10
        assert df.shape[1] == 6
        df: pd.DataFrame = lib.read(policy.get_symbol_name(7)).data
        logger.info(f"{df}")
        assert df.shape[0] == 10
        assert df.shape[1] == 7

        policy = LibraryPopulationPolicy(None).set_parameters([10] * 3, 1).use_auto_increment_index()
        populate_library(tlm, policy, LibraryType.PERSISTENT, lib_name_suffix)
        lib = tlm.get_library(LibraryType.PERSISTENT, lib_name_suffix)
        for i in range(3):
            df: pd.DataFrame = lib.read(policy.get_symbol_name(i)).data
            logger.info(f"{df}")
            assert df.shape[0] == 10
            assert df.shape[1] == 1

        TestLibraryManager.remove_all_test_libs(storage)
        assert len(tlm._get_arctic_client_persistent().list_libraries()) == 0

    @classmethod
    def test_multiprocessing(cls):
        """
        Do all process clear their modifiable storage space and do they 
        clear only their data not other's?
        """

        num_processes = 7
        storage = Storage.AMAZON
        logger = get_console_logger()
        benchmark_name = "MULTIPROCESSING"

        df =  DFGenerator.generate_random_dataframe(10, 10)

        def worker_process():

            def string_list_has_number(strings, number):
                target = str(number)  
                for s in strings:
                    if target in s:  
                        return True
                return False

            symbol = "s"
            tlm = TestLibraryManager(storage, benchmark_name)
            lib = tlm.get_library(LibraryType.MODIFIABLE)
            lib.write(symbol, df)
            logger.info(f"Process [{os.getppid()}] written at library: {lib}")
            assert string_list_has_number(tlm._get_arctic_client_modifiable().list_libraries(),
                                          os.getpid()), "There is a library with that pid"
            tlm.clear_all_modifiable_libs_from_this_process()
            assert not string_list_has_number(tlm._get_arctic_client_modifiable().list_libraries(),
                                          os.getpid()), "There is NO library with that pid"

        tlm = TestLibraryManager(storage, benchmark_name)
        ac = tlm._get_arctic_client_modifiable()
        for lib in ac.list_libraries():
            logger.info(f"Library delete: {lib}")
            ac.delete_library(lib)

        processes = []
        manager = multiprocessing.Manager()
        result_list = manager.list()

        for _ in range(num_processes):
            process = multiprocessing.Process(target=worker_process)
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

        for process in processes:
            assert process.exitcode == 0, f"Process failed with exit code {process.exitcode}"                
        
        assert len(ac.list_libraries()) == 0, "All libraries from child processes deleted"

        print("All processes completed successfully:", list(result_list))        