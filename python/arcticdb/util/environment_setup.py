"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from abc import ABC, abstractmethod
from enum import Enum
import logging
import os
import tempfile
import time
import re
import pandas as pd
import numpy as np
from typing import List, Union 

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.storage_fixtures.s3 import BaseS3StorageFixtureFactory, real_s3_from_environment_variables
from arcticdb.util.utils import DFGenerator, ListGenerators 
from arcticdb.version_store.library import Library

#ASV captures console output thus we create console handler
logLevel = logging.INFO
logger = logging.getLogger(__name__)
logger.setLevel(logLevel)
console_handler = logging.StreamHandler()
console_handler.setLevel(logLevel)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

## Amazon s3 storage bucket dedicated for ASV performance tests
AWS_S3_DEFAULT_BUCKET = 'arcticdb-asv-real-storage'

## The tests will use shared/persistent storage with 3 different prefixes, creating 3 important STORAGE SPACES:
## First - (PERSISTENT/PERMANENT STORAGE SPACE) This prefix is for production code - the libraries written there will 
##    be SHARED across environments and product versions. Therefore there you place mainly READ ONLY libraries.
##    Benefits:
##       - created only once - no need to recreate them and waste time (this way we have time that is saved)
##       - all read operations will be able ok to persist once their libraries
##       - there is always a procedure that can recreate the store if needed
##       - implicitly since those stores are generated ONCE we get coverage for risks of data not being 
##         able to be read in future versions
##       - as there should be always check data method we always assure things are there to recreate the storage
##
##    NOTE: What you should avoid is any modifications on his storage type
##
##    OPPORTUNITY: AGING TESTS - this is one class of tests that are now possible with shared store. The tests use
##       the permanent store to append data each time they are executed, to add new symbols also. This gives 
##       opportunity to have new writes, new appends in actual condition over time. Those types of tests will not 
##       be flat line on graphs but will show how well or unwell we age on SPECIFIC environment - storage, because 
##       the tests are always executed on same environment
PERSISTENT_LIBS_PREFIX = "PERMANENT_LIBRARIES" 


## Second - (MODIFIABLE STORAGE SPACE) When a test requires to write something to measure performance it needs
##    a storage space different from the main permanent space. This is the modifyable space. There each test will
##    need own library to run write and any other library modification tests.
## 
##    IMPORTANT: your test MIGHT run in several processes competing (or writing) for same library! As arcticdb has
##          no locking or any other mechanism to prevent that you might end up with completely mixed results or
##          or broken lib (which might go unnoticed also). Therefore you MUST:
##            - create writable libraries for each process!
##            - the test must reuse only the library of its own process!
MODIFIABLE_LIBS_PREFIX = 'MODIFIABLE_LIBRARIES' 


## Third - (MODIFIABLE STORAGE SPACE) How to test your test before going into production? How to experiment with 
##    different values while debugging or researching safely without polluting or hurting the permanent libraries?
##    this storage space resolves all such problems. A setup environment class in test mode will always write
##    data to this storage space instead on permanent storage space. All that will be needed is to set 
##    this class in test mode
TEST_LIBS_PREFIX = 'TESTS_LIBRARIES' 

class SetupConfig:
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
            cls._instance = super(SetupConfig, cls).__new__(cls, *args, **kwargs)
            cls._instance.value = "Initialized"

            ## AWS S3 variable setup
            ## Copy needed environments variables from GH Job env vars
            ## Will only throw exceptions if real tests are executed
            cls._aws_default_factory = real_s3_from_environment_variables(shared_path=True)
            cls._aws_default_factory.default_prefix = None
            cls._aws_default_factory.default_bucket = AWS_S3_DEFAULT_BUCKET
            cls._aws_default_factory.clean_bucket_on_fixture_exit = False
   
    def get_aws_enforced_prefix():
        """
        Defined for test executors machines or individually
        """
        return os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX", None)

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


class Storage(Enum):
    AMAZON = 1
    LMDB = 2


class StorageInfo:    

    def __init__(self, storage: Storage, uris_cache: List[str], library_options: LibraryOptions, prefix: str) :
        self.storage: str = storage
        self.uris_cache: List[str] = uris_cache
        self.library_options: LibraryOptions = library_options
        self.prefix: str = prefix

    def __str__(self):
        value = super().__str__()
        for key, val in vars(self).items():
            value = f"{value}\n {key}: {val}"
        return value


class EnvConfigurationBase(ABC):
    """
    Defines base class for benchmark scenario setup and teardown
    You need to create your own class inheriting from this one for each ASV benchmark class.
    It will be responsible for setting up environment and providing tear down

    Override in child class:
     (A) :func:`setup_all` - to setup required libraries and symbols (on empty storage that non exist/ do no deleted here)
     (B) :func:`check_ok` - if the test will delete each time previous libraries and start from scratch 
               then simply return False. Otherwise consider implementing logic that will check if needed
               config exist on persistent store and if yes setup of configuration will be skipped
     (C) OPTIONAL! :func:`setup_environment` this function should be used for `setup_cache` method of ASV test.
               Should you need different behavior skip
    
    Use default :func:`setup_environment` for setup (primarily in setup_cache method of ASV class)

    See :class:`GeneralSetupLibraryWithSymbols` for implementation of general logic 
    See :class:`LMDBReadWrite` and :class:`AWSReadWrite` for test implementation and usage of environment configuration class

    """

    def __init__(self, storage: Storage, prefix: str = None, library_options: LibraryOptions = None, uris_cache: List[str] = None):
        """
        Should be initialized with type and arctic url
        """
        self.storage = storage
        if uris_cache :
            self.ac_cache = {keys: None for keys in uris_cache}
        else:
            self.ac_cache = {}
        self.__storage_test_env_prefix: str = prefix
        self.__libraries = set()
        self.__library_options = library_options
        self.__set_test_mode = False
        SetupConfig()

    def get_persistent_storage_prefix(self):
        return PERSISTENT_LIBS_PREFIX # This should be same across environments - git, ec2, local etc

    def get_modifiable_storage_prefix(self):
        return self._combine_prefix(MODIFIABLE_LIBS_PREFIX)

    def get_test_storage_prefix(self):
        return self._combine_prefix(TEST_LIBS_PREFIX)

    def _combine_prefix(self, type_prefix: str):
        """
        Constructs actual prefix based on enforced via 
        environment variable + defined in the module type prefixes + defined by tests prefixes
        all that makes unique combination
        """
        result = None
        if SetupConfig.get_aws_enforced_prefix():
            result = SetupConfig.get_aws_enforced_prefix()
        if type_prefix:
            if result is None:
                result = type_prefix
            else:
                result = f"{result}_{type_prefix}"
        if self.__storage_test_env_prefix:
                result = f"{result}_{self.__storage_test_env_prefix}"
        return result

    def set_test_mode(self):
        """
        Makes the setup run in test mode. Will write data instead on the default persistent storage
        in a separate test persistent storage space. This allows you to do test of production code 
        and not pollute the default persistent store with unnecessary information and also protect data
        there from deletion or corruption.
        Always use test mode for new tests!
        """
        self.__set_test_mode=True
        return self
    
    def is_test_mode(self) -> bool:
        """
        Checks if setup to work in test mode (in separate test storage space)
        """
        return TEST_LIBS_PREFIX in self.get_arctic_client_persistent().get_uri()

    def logger(self) -> logging.Logger:
        return logger
        
    @classmethod
    def from_storage_info(cls, data: StorageInfo):
        """
        Constructs the class out of serialized data
        """
        return cls(storage=data.storage, uris_cache=data.uris_cache, library_options=data.library_options, prefix=data.prefix)
    
    def get_storage_info(self) -> StorageInfo:
        """
        Serializes the class information in order to be reconstructed back later
        """
        return StorageInfo(storage=self.storage, 
                           uris_cache=list(self.ac_cache.keys()), library_options=self.__library_options, prefix=self.__storage_test_env_prefix)
    
    def _get_arctic_client(self, prefix, confirm_persistent_storage_need: bool = False):
        """
        Obtains specified client on specified persistent shared storage prefix,
        allows creationg of test and modifiable etc clients
        NOTE: prefix = `None` - the persistent storage where all persistent libraries will be created
        """
        if self.storage == Storage.AMAZON:
            arctic_url = SetupConfig.get_aws_s3_arctic_uri(prefix, confirm_persistent_storage_need)
        elif self.storage == Storage.LMDB:
            ## We create fpr each object unique library in temp dir
            arctic_url = f"lmdb://{tempfile.gettempdir()}/benchmarks_{prefix}" 
            ## 
            # Home dir will be shared dir and will not work well with LMDB actually
            #home_dir = os.path.expanduser("~")
            #self.ac = Arctic(f"lmdb://{home_dir}/benchmarks")
        else:
            raise Exception("Unsupported storage type :", self.storage)
        ac =  self.ac_cache.get(arctic_url, None)
        if ac is None:
            ac = Arctic(arctic_url)    
        self.ac_cache[arctic_url] = ac
        return ac
    
    def get_arctic_client_persistent(self) -> Arctic:
        if self.__set_test_mode:
            return self._get_arctic_client(self.get_test_storage_prefix())
        return self._get_arctic_client(self.get_persistent_storage_prefix(), confirm_persistent_storage_need=True)
    
    def get_arctic_client_modifiable(self) -> Arctic:
        if self.__storage_test_env_prefix is None:
            return self._get_arctic_client(self.get_modifiable_storage_prefix())
        else:
            return self._get_arctic_client(self.get_modifiable_storage_prefix())
    
    def remove_all_modifiable_libraries(self, confirm=False):
        """
        Removes all libraries created on modifiable storage space
        """
        assert confirm, "Deletion of all libraries must be confirmed"
        ac = self.get_arctic_client_modifiable()
        libs = self.get_arctic_client_modifiable().list_libraries()
        for lib in libs:
            ac.delete_library(lib)
    
    def get_symbol_name_template(self, sym_suffix: Union[str, int]) -> List[str]:
        """
        Defines how the symbol name would be constructed
        """
        return f"sym_{sym_suffix}"

    def get_library_names(self, lib_suffix: Union[str, int] = None) -> List[str]:
        """
        Redefine the way the names of libraries are constructed.
        `lib_suffix` can be None indicating no lib suffix added,
        or number - for instance number of symbols in library,
        or a composite string
        """
        if self.__storage_test_env_prefix is None:
            lib_mid_part_name = self.__class__.__name__
        else: 
            lib_mid_part_name = self.__storage_test_env_prefix
        lib_names = [f"PERM_{lib_mid_part_name}_{lib_suffix}", 
                     f"MOD_{lib_mid_part_name}_{lib_suffix}"]        
        return lib_names
    
    def _get_lib(self, ac: Arctic, lib_name: str) -> Library:
        lib_opts = self.__library_options
        self.__libraries.add(lib_name)
        return ac.get_library(lib_name, create_if_missing=True, 
                                library_options=lib_opts)
    
    def get_library(self, library_suffix: Union[str, int] = None) -> Library:
        """
        Returns one time setup library (permanent)
        """
        return self._get_lib(self.get_arctic_client_persistent(), 
                             self.get_library_names(library_suffix)[0])
    
    def get_modifiable_library(self, library_suffix: Union[str, int] = None) -> Library:
        """
        Returns library to read write and delete after done.
        """
        return self._get_lib(self.get_arctic_client_modifiable(), 
                             self.get_library_names(library_suffix)[1])

    def delete_modifiable_library(self, library_suffix: Union[str, int] = None):
        """
        Use this method to delete previously created library on modifiable storage 
        space
        NOTE: will assert if library is not delete
        """
        ac = self.get_arctic_client_modifiable()
        name = self.get_library_names(library_suffix)[1]
        logger.info(f"Deleting modifiable library {name}")
        ac.delete_library(self.get_library_names(library_suffix)[1])
        assert name not in ac.list_libraries(), f"Library successfully deleted {name}"

    def set_params(self, params):
        """
        Sets parameters for environment generator class. 
        The expectations for parameters is same as ASV class.
        To be used in concrete classes
        """
        self._params = params
        return self

    def get_parameter_list(self):
        """
        Returns the parameter list. Use it to set the `params` variable of ASV test
        """
        return self._params

    def is_multi_params_list(self):
        """
        Checks if the parameters are list of 2+ parameter lists
        """
        return all(isinstance(i, list) for i in self._params)

    @abstractmethod
    def check_ok() -> bool:
        """
        Define mechanism to check if permanent setup is available
        to skip the inital setup
        """
        pass
    
    @abstractmethod
    def setup_all(self) -> 'EnvConfigurationBase':
        '''
        Provide implementation that will setup needed data assuming storage does not contain 
        any previous data for this scenario
        '''
        pass

    def setup_environment(self, library_sufixes_list: List[Union[str | int]] = None) -> 'EnvConfigurationBase':
        """
        Responsible for setting up environment.
        Will first delete any pre-existing modifiable libraries
        Then will check if persistent environment is there
        If absent will create it, deleting firs any libraries that might exist initially        
        if test is creating multiple libraries, provide 
        `library_sufixes_list` to make sure all previous libs are cleaned

        Use at :func:`setup_cache` method of ASV test classes
        """
        indexes = [None] # by default there will be one lib
        if library_sufixes_list:
            indexes = library_sufixes_list
        for i in indexes:
            self.delete_modifiable_library(i)
        if not self.check_ok():
            ac = self.get_arctic_client_persistent()
            ## Delete all PERSISTENT libraries before setting up them
            for i in indexes:
                lib = self.get_library_names(i)[0]
                logger.info(f"REMOVING LIBRARY: {lib}")
                ac.delete_library(lib)
            self.setup_all()
        return self
    
    @classmethod
    def get_parameter_from_string(cls, string: str, param_indx: int, param_type: Union[int, str] = int) -> Union[int, str]:
        """
        A way to handle composite parameter list.

        Instead of having multiple sets of parameter we keep the parameter to just one list
        of string with encoded several values for 2 or more parameters delimited with "__"

        For example following value contains 3 parameters: "w180__h200__value1234"

        We can treat first parameter either as string or as integer with description:
        - get_parameter_from_string('w180__h200__value1234', 0, str) will return 'w180' string
        - get_parameter_from_string('w180__h200__value1234', 0, int) will extract int and return '180' as int value

        """
        params = string.split("__")
        value = params[param_indx]
        if param_type is str:
            pass
        elif param_type is int:
            match = re.search(r'\d+', value)
            if match:
                value = int(match.group())
        else:
            raise Exception(f"Unsupported type {param_type}")
        return value


class GeneralSetupLibraryWithSymbols(EnvConfigurationBase):
    """
    This class provides abstract logic for generation of a set of symbols
    that have certain number of rows and optionally columns.

    You have to use :func:`set_params` to define the exact profile of the symbols
    to be created. The expectations are same as with ASV `params`: 
         - a) a list of 2 lists = desired rows sizes (first) and column sizes (second)
         - b) a list of 1 list - desired rows sizes (default column size to be used)

    While in default implementation this class will generate list of random column types,
    you can override that behavior in parent classes for specific needs    

    """

    def set_params(self, params) -> 'GeneralSetupLibraryWithSymbols':
        super().set_params(params)
        return self

    def generate_dataframe(self, rows: int, cols: int) -> pd.DataFrame:
        """
        Dataframe generator that will be used for wide dataframe generation
        """
        if cols is None:
            cols = 1
        st = time.time()
        self.logger().info("Dataframe generation started.")
        df: pd.DataFrame = DFGenerator.generate_random_dataframe(rows=rows, cols=cols)
        self.logger().info(f"Dataframe rows {rows} cols {cols} generated for {time.time() - st} sec")
        return df
    
    def get_symbol_name(self, rows, cols) -> str:
        '''
        Maps rows and cols to proper symbol name
        This method should be used in ASV tests obtain symbol name based on parameters
        '''
        sym_suffix = ""
        if cols is None:
            sym_suffix = f"rows{rows}"
        else:
            sym_suffix = f"rows{rows}_cols{cols}"
        return self.get_symbol_name_template(sym_suffix)
    
    def _get_symbol_bounds(self):
        """
        Extracts rows and cols from the parameter structure
        """

        assert len(self._params) > 0, "No parameters defined"

        if (self.is_multi_params_list()):
            assert len(self._params) <= 2, "Supports 2 parameters maximum - rows and cols lists"
            list_rows = self._params[0]
            if len(self._params) == 2:
                list_cols = self._params[1]
            return (list_rows, list_cols)
        else:
            return (self._params, [ None ])

    def setup_all(self) -> 'GeneralSetupLibraryWithSymbols':
        """
        Sets up in default library a number of symbols which is combination of parameters
        for number of rows and columns
        For each of symbols it will generate dataframe based on `generate_dataframe` function
        """
        (list_rows, list_cols) = self._get_symbol_bounds()
        start = time.time()
        lib = self.get_library()
        self.logger().info(f"Library: {lib}")
        for row in list_rows:
            for col in list_cols:
                st = time.time()
                df = self.generate_dataframe(row, col)
                symbol = self.get_symbol_name(row, col)
                lib.write(symbol, df)
                self.logger().info(f"Dataframe stored at {symbol} for {time.time() - st} sec")
        self.logger().info(f"TOTAL TIME (setup of one library with specified rows and column symbols): {time.time() - start} sec")
        return self
    
    def check_ok(self):
        """
        Checks if library contains all needed data to run tests.
        if OK, setting things up can be skipped
        """
        (list_rows, list_cols) = self._get_symbol_bounds()
        lib = self.get_library()
        symbols = lib.list_symbols()
        self.logger().info(f"Symbols {lib.list_symbols()}")
        for rows in list_rows:
            for cols in list_cols:
                symbol = self.get_symbol_name(rows, cols)
                self.logger().info(f"Check symbol {symbol}")
                if not symbol in symbols:
                    return False
        return True


class GeneralSetupSymbolsVersionsSnapshots(EnvConfigurationBase):
    """
    Will create several libraries each containing specified number of symbols.
    Each symbol will have by default 1 version (use `set_max_number_versions` and
    `set_mean_number_versions_per_sym` to override that) and will have no metadata
    (use `set_with_metadata_for_each_version` to override that).

    By default no snapshot will be generated for each version. This can be overridden with
    `set_with_snapshot_for_each_version`
    """

    def __init__(self, storage, prefix = None, library_options = None, uris_cache = None):
        super().__init__(storage, prefix, library_options, uris_cache)
        self.with_metadata = False
        self.versions_max = 1
        self.mean = 1
        self.with_snapshot = False
        self.last_snapshot = None # Will hold the name of last snapshot created
        self.first_snapshot = None # Will hold the name of first snapshot created
        self.first_snapshot_taken = False

    def set_params(self, params) -> 'GeneralSetupSymbolsVersionsSnapshots':
        super().set_params(params=params)
        return self

    def set_mean_number_versions_per_sym(self, mean) -> 'GeneralSetupSymbolsVersionsSnapshots':
        self.mean = mean
        return self

    def set_max_number_versions(self, versions_max) -> 'GeneralSetupSymbolsVersionsSnapshots':
        self.versions_max = versions_max
        return self

    def set_with_metadata_for_each_version(self, with_medatada: bool = True) -> 'GeneralSetupSymbolsVersionsSnapshots':
        self.with_metadata = with_medatada
        return self
    
    def set_with_snapshot_for_each_version(self, with_snapshot: bool = True) -> 'GeneralSetupSymbolsVersionsSnapshots':
        self.with_snapshot = with_snapshot
        return self

    def generate_dataframe(self, num_rows):
        return DFGenerator.generate_random_dataframe(rows=num_rows, cols=20)
    
    def generate_metadata(self):
        return DFGenerator.generate_random_dataframe(rows=3, cols=10).to_dict()

    def get_symbol_name(self, number_symbols: int, number_versions:int, with_metadata:bool, with_snapshot: bool) -> str:
        '''
        Maps rows and cols to proper symbol name
        This method should be used in ASV tests obtain symbol name based on parameters
        '''
        if with_metadata:
            meta = "with-meta"
        else:
            meta = "no-meta"
        if with_snapshot:
            snap = "with-snap"
        else:
            snap = "no-snap"
        return self.get_symbol_name_template(f"{number_symbols}_vers-{number_versions}_{meta}_{snap}")
    
    def get_versions_list(self, number_symbols: int) -> List[np.int64]:
        if self.versions_max == 1:
            versions_list = [1] * number_symbols
        else:
            versions_list = ListGenerators.generate_random_list_with_mean(
                number_elements=number_symbols,
                specified_mean=self.mean, 
                value_range=(1, self.versions_max), 
                seed=365)
        return versions_list

    def setup_library(self, number_symbols) -> 'GeneralSetupSymbolsVersionsSnapshots':
        """
        Sets a library with specified number of symbols with a snapshot and metadata if specified
        """
        lib = self.get_library(number_symbols)
        df = self.generate_dataframe(10)
        meta = self.generate_metadata()
        self.logger().info(f"METADATA: {meta}")
        versions_list = self.get_versions_list(number_symbols)
        index = 0
        for sym_num in range(number_symbols):
            versions = versions_list[index]
            symbol = self.get_symbol_name(number_symbols=sym_num,
                                       number_versions=versions,
                                       with_metadata=self.with_metadata,
                                       with_snapshot=self.with_snapshot)
            self.logger().info(f"Generating symbol {symbol}.")
            st = time.time()
            for num in range(versions):
                if self.with_metadata:
                    lib.write(symbol, df, metadata=meta)
                else:
                    lib.write(symbol, df)
            if self.with_snapshot:
                snap = f"snap_{symbol}"
                if self.with_metadata:
                    lib.snapshot(snap, metadata=meta)
                    self.last_snapshot = snap # Last snapshot created
                    if not self.first_snapshot_taken:
                        self.first_snapshot = snap # Remember first snapshot
                        self.first_snapshot_taken = True
                else: 
                    lib.snapshot(snap)
            self.logger().info(f"Generation of {symbol} symbol COMPLETED for :{time.time() - st} sec")
            index += 1
        return self

    def setup_all(self):
        assert not self.is_multi_params_list(), "One parameters list expected"
        st = time.time()
        for num_symbols in self.get_parameter_list():
            self.setup_library(num_symbols)
        self.logger().info(f"Total time {time.time() - st}")    

    def check_ok(self) -> bool:

        for num_symbols in self.get_parameter_list():
            lib = self.get_library(num_symbols)
            symbols = lib.list_symbols()
            self.logger().info(f"Check library: {lib}")
            versions_list = self.get_versions_list(num_symbols)
            index = 0
            for sym_num in range(num_symbols):
                symbol = self.get_symbol_name(number_symbols=sym_num,
                            number_versions=versions_list[index],
                            with_metadata=self.with_metadata,
                            with_snapshot=self.with_snapshot)
                if not (symbol in symbols):
                    self.logger().info(f"Symbol {symbol} not found")
                    return False
                index += 1

        return True        
    
    def clear_symbols_cache(self):
        for num_symbols in self.get_parameter_list():
            lib = self.get_library(num_symbols)
            lib._nvs.version_store._clear_symbol_list_keys()


class GeneralSetupLibraryWithSymbolsTests:
    """
    This set of tests allows to test the setup / check / teardown logic of setup environment 
    building block classes 
    """

    PREFIX_FOR_TEST = "SOME_PREFIX"

    @classmethod
    def get_general_modifyanle_client(cls):
        setup = (GeneralSetupLibraryWithSymbols(Storage.AMAZON, 
                                                prefix=GeneralSetupLibraryWithSymbolsTests.PREFIX_FOR_TEST))
        assert GeneralSetupLibraryWithSymbolsTests.PREFIX_FOR_TEST in setup.get_arctic_client_modifiable().get_uri()
        return setup


    @classmethod
    def get_general_setup_test_mode(cls):
        setup = (GeneralSetupLibraryWithSymbols(Storage.LMDB)
                 .set_test_mode())
        assert setup.is_test_mode()
        return setup

    @classmethod
    def delete_test_store(cls, setup: EnvConfigurationBase):
        ac = setup.get_arctic_client_persistent()
        assert setup.is_test_mode()
        libs = ac.list_libraries()
        for lib in libs:
            ac.delete_library(lib)
        libs = ac.list_libraries()
        assert len(libs) < 1, f"All libs should be deleted: {libs}"

    @classmethod
    def test_setup_with_rows_and_cols(cls):
        setup = cls.get_general_setup_test_mode()
        setup.set_params([[20,30],[40,50]])
        df = setup.generate_dataframe(10,20)
        assert df.shape[0] == 10
        assert df.shape[1] == 20
        logger.info(df)
        cls.delete_test_store(setup)
        if not setup.check_ok():
            setup.setup_all()
        assert setup.check_ok()
        cls.delete_test_store(setup)
        setup.setup_environment()
        assert setup.check_ok()
        cls.delete_test_store(setup)

    @classmethod
    def test_modifiable_client_workflow(cls):
        """
        Test for general functions for obtaining and 
        cleaning modifiable libraries
        """
        setup = cls.get_general_modifyanle_client()
        symbol = "test_symbol"
        lib = setup.get_modifiable_library()
        lib.write(symbol, setup.generate_dataframe(10,10))
        ac = setup.get_arctic_client_modifiable()
        assert ac.get_library(setup.get_library_names()[1])
        setup.delete_modifiable_library()
        assert len(ac.list_libraries()) == 0
        assert len(setup.get_arctic_client_modifiable().list_libraries()) == 0

    @classmethod
    def test_setup_versions_and_snapshots(cls):
        setup = (GeneralSetupSymbolsVersionsSnapshots(storage=Storage.LMDB, prefix="LIST_SYMBOLS")
        .set_with_metadata_for_each_version()
        .set_with_snapshot_for_each_version()
        .set_params([10, 20])
        .set_test_mode())
        assert setup.is_test_mode()

    	#Delete-setup-all-check
        cls.delete_test_store(setup)
        setup.setup_environment()
        assert setup.check_ok()

        #Changing parameters should trigger not ok for setup
        setup.set_with_metadata_for_each_version()
        setup.set_with_snapshot_for_each_version()
        setup.set_params([2, 3])
        assert not setup.check_ok()
        
