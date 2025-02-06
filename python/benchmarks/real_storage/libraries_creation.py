"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from abc import ABC, abstractmethod
from enum import Enum
import tempfile
import time
import re
from typing import List, Union

from arcticdb.arctic import Arctic
from arcticdb.storage_fixtures.s3 import S3Bucket, real_s3_from_environment_variables
from arcticdb.version_store.library import Library


## Amazon s3 storage URL
AWS_S3_DEFAULT_BUCKET = 'arcticdb-asv-real-storage'


class SetupConfig:
    '''
    Defined special one time setup for real storages.
    Place here what is needed for proper initialization
    of each storage
    '''
    _instance = None
    _aws_s3_bucket: S3Bucket = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetupConfig, cls).__new__(cls, *args, **kwargs)
            cls._instance.value = "Initialized"

            ## AWS S3 variable setup
            ## Copy needed environments variables from GH Job env vars
            ## Will only throw exceptions if real tests are executed
            factory = real_s3_from_environment_variables(shared_path=True)
            factory.default_prefix = None
            factory.default_bucket = AWS_S3_DEFAULT_BUCKET
            factory.clean_bucket_on_fixture_exit = False
            SetupConfig._aws_s3_bucket = factory.create_fixture()
        return 
    
    @classmethod
    def get_aws_s3_arctic_uri(cls) -> str:
        assert SetupConfig._aws_s3_bucket, "Environment variables not initialized (ARCTICDB_REAL_S3_ACCESS_KEY,ARCTICDB_REAL_S3_SECRET_KEY)"
        return SetupConfig._aws_s3_bucket.arctic_uri


class Storage(Enum):
    AMAZON = 1
    LMDB = 2


class StorageInfo:    

    def __init__(self, type: Storage, url: str):
        self.type = type
        self.url = url

class LibrariesBase(ABC):
    """
    Defines base class for benchmark scenario setup and teardown
    You need to create your own class inheriting from this one for each ASV benchmark class.
    It will be responsible for setting up environment and providing tear down

    The base class supports most common scenarios for arcticdb tests:
     (A) :func:`LibrariesBase.get_parameter_list` is list of row numbers in the needed library
        (A.1) in this case :func:`get_library` may not use in the lib names passed index number
        (A.2) default behavior of method is for this usage scenario
     (B) :func:`LibrariesBase.get_parameter_list` is list of symbols sizes for needed libraries
        (A.2) in this case :func:`get_library` may not use in the lib names passed index number
        (A.2) this is mot default behavior and you might want to override some methods later

    Implement all abstract methods. Follow instructions in docs strings

    Consider generating UNIQUE dataframes for each new child class. Dataframes generation is now much simpler 
    and each class should own the logic of generating its dataframe. That creates code duplication,
    but eliminates accidental change in common dataframe that will affect others 

    Use default :func:`LibrariesBase.setup_environment` for setup (primarily in setup_cache method of ASV class)

    See :class:`SymbolLibraries` and :class:`ReadBenchmarkLibraries` for examples
    Also check how each of those classes is bound exactly to one ASV class that contains tests

    """

    def __init__(self, type: Storage = Storage.LMDB, arctic_url: str = None):
        """
        Should be initialized with type or arctic url
        """
        if type is not None:
            self.type = type
        self.arctic_url = arctic_url
        if self.arctic_url is not None:
            if type is None:
                if "lmdb" in self.arctic_url.lower():
                    self.type = Storage.LMDB
                if "s3s://" in self.arctic_url.lower() or "s3://" in self.arctic_url.lower():
                    self.type = Storage.AMAZON
                raise Exception(f"unsupported storage type: {type}")
        self.ac = None
        self.__libraries = set()
        SetupConfig()

    @classmethod
    def fromStorageInfo(cls, data: StorageInfo):
        return cls(type=data.type, arctic_url=data.url)
    
    def get_storage_info(self) -> StorageInfo:
        return StorageInfo(self.type, self.arctic_url)
    
    def remove_libraries(self):
        ## Remove only LMDB libs as they need to
        if self.type == Storage.LMDB:
            ac = self.get_arctic_client()
            for lib in self.__libraries:
                ac.delete_library(lib)

    def get_arctic_client(self):
        if self.ac is None:
            if self.arctic_url is None:
                if (self.type == Storage.AMAZON):
                    self.arctic_url = SetupConfig.get_aws_s3_arctic_uri()
                elif (self.type == Storage.LMDB):
                    ## We create fpr each object unique library in temp dir
                    self.arctic_url = f"lmdb://{tempfile.gettempdir()}/benchmarks_{int(time.time() * 1000)}" 
                    ## 
                    # Home dir will be shared dir and will not work well with LMDB actually
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
    
    @abstractmethod
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
        ac = self.get_arctic_client()
        lib_opts = self.get_library_options()
        lib_name = self.get_library_names(num_symbols)[0]
        self.__libraries.add(lib_name)
        return ac.get_library(lib_name, create_if_missing=True, 
                                library_options=lib_opts)
    
    def get_modifyable_library(self, num_symbols: int = 1) -> Library:
        """
        Returns library to read write and delete after done.
        """
        ac = self.get_arctic_client()
        lib_opts = self.get_library_options()
        lib_name = self.get_library_names(num_symbols)[1]
        self.__libraries.add(lib_name)
        return ac.get_library(lib_name, create_if_missing=True, 
                              library_options=lib_opts)

    def delete_modifyable_library(self, num_symbols: int = 1):
        ac = self.get_arctic_client()
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

    @abstractmethod
    def get_parameter_names_list(self):
        """
        Defines parameter names list for benchmarks. Can be used for number of rows, symbols etc.

        NOTE: it might return different list of values depending on the storage type due to speed.
        Always synchronize with :func:`LibrariesBase.get_parameter_list`
        """
        pass    

    @abstractmethod
    def setup_all(self) -> 'LibrariesBase':
        '''
        Provide implementation that will setup needed data assuming storage does not contain 
        any previous data for this scenario
        '''
        pass

    def setup_environment(self, parameters_is_index_for_lib_name=False) -> 'LibrariesBase':
        """
        Responsible for setting up environment if not set for persistent libraries.

        Provides default implementation that can be overridden
        """
        indexes = [1] # by default there will be one lib
        if parameters_is_index_for_lib_name:
            indexes = self.get_parameter_list()
        for i in indexes:
            self.delete_modifyable_library(i)
        if not self.check_ok():
            ac = self.get_arctic_client()
            ## Delete all PERSISTENT libraries before setting up them
            for i in indexes:
                lib = self.get_library_names(i)[0]
                print("REMOVING LIBRARY: ", lib)
                ac.delete_library(lib)
            self.setup_all()
        return self

    def check_ok(self):
        """
        NOTE: Override as needed by ht logic!
        Checks if library contains all needed data to run tests.
        if OK, setting things up can be skipped

        Default implementation assumes that parameter list is list of rows
        and thus check that the lbrary has the symbols with specified rows
        """
        symbols = self.get_library(1).list_symbols()
        for rows in self.get_parameter_list():
            symbol = self.get_symbol_name(rows)
            print(f"Check symbol {symbol}")
            if not symbol in symbols:
                return False
        return True
    
    #region Helper Methods

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

    def setup_library_with_symbols(self, num_syms: int, data_frame = None, metadata = None):
        """
        Sets up the labrary with index defined by `num_syms` such amount of symbols.
        Each symbol will have the dataframe and metadata specified.
        Can be used in child classes at :func:`LibrariesBase.setup_all` call
        NOTE: assumes parameter list is list of number of symbols per library
        """
        print(f"Started creating library for {num_syms} symbols.")
        lib = self.get_library(num_syms)
        st = time.time()
        for num in range(num_syms):
            sym = self.get_symbol_name(num)
            lib.write(sym, data=data_frame, metadata=metadata)
            print(f"created {sym}")
        print(f"Library '{lib}' created {num_syms} symbols COMPLETED for :{time.time() - st} sec")

    def check_libraries_have_specified_number_symbols(self) -> bool:
        """
        Predefined check to assure each library has been created with specified number os symbols.
        You can use it in :func:`LibrariesBase.check_ok` child methods to replace default logic
        NOTE: assumes parameter list is list of number of symbols per library
        """
        for num in self.get_parameter_list():
            lib = self.get_library(num)
            list = lib.list_symbols()
            print(f"number symbols {len(list)} in library {lib}")
            if len(list) != num:
                return False
        return True
    
    def delete_libraries(self, nameStarts: str, confirm=False):
        """
        Deletes all libraries starting with specified string.
        Dangerous method use with care
        """
        assert confirm, "deletion not confirmed!"
        assert len(nameStarts) > 4, "name starts should be > 4 symbols for safety reasons"
        ac = self.get_arctic_client()
        lib_list = ac.list_libraries()
        lib_list = [lib for lib in ac.list_libraries() if lib.startswith(nameStarts)]
        for lib in lib_list:
            print(f"Deleteing {lib}")
            ac.delete_library(lib)
    #endregion





