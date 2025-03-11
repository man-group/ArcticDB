import copy
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
from arcticdb.util.utils import DFGenerator, ListGenerators, TimestampNumber
from arcticdb.version_store.library import Library

def get_logger_for_asv(bencmhark_cls):
    logLevel = logging.INFO
    logger = logging.getLogger(bencmhark_cls.__name__)
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


# Currently we're using the same arctic client for both persistant and modifiable libraries.
# We might decide that we want different arctic clients (e.g. different buckets) but probably not needed for now.
def get_arctic_client(storage: Storage) -> Arctic:
    pass

def get_library_name(library_type, benchmark_cls, lib_name_suffix):
    if library_type == LibraryType.PERSISTENT:
        return f"{library_type}_{benchmark_cls.__name__}_{lib_name_suffix}"
    if library_type == LibraryType.MODIFIABLE:
        # We want the modifiable libraries to be unique per process/ benchmark class. We embed this deep in the name
        return f"{library_type}_{benchmark_cls.__name__}_{os.getpid()}_{lib_name_suffix}"

def get_library(ac : Arctic, library_type : LibraryType, benchmark_cls, lib_name_suffix : str) -> Library:
    lib_name = get_library_name(library_type, benchmark_cls, lib_name_suffix)
    if library_type == LibraryType.PERSISTENT:
        # TODO: Change to OpenMode=READ for persistent libraries
        # This however is not available in the c++ library manager API (it currently hardcodes OpenMode=DELETE).
        # We can expose it just for these tests if we decide so. but this is probably for a separate PR.
        pass
    return ac.get_library(lib_name, create_if_missing=True)


def populate_library(lib, population_policy : LibraryPopulationPolicy):
    num_symbols = population_policy.num_symbols
    df_generator = population_policy.df_generator
    for i in range(num_symbols):
        sym = population_policy.get_symbol(i)
        kwargs = population_policy.get_generator_kwargs(i)
        df = df_generator.get_dataframe(**kwargs)
        lib.write(sym, df)

# This is the only API to populate a persistent library. If we deem useful we can also add a check whether library is valid (e.g. has the correct num_symbols)
# As discussed, ideally this will be called in completely separate logic from ASV tests to avoid races, but for simplicity
# for now we can just rely on setup_cache to populate the persistant libraries if they are missing.
def populate_persistent_library_if_missing(ac, benchmark_cls, lib_name_suffix, population_policy : LibraryPopulationPolicy):
    lib_name = get_library_name(LibraryType.PERSISTENT, benchmark_cls, lib_name_suffix)
    if ac.has_library(lib_name):
        lib = ac.create_library(lib_name)
        populate_library(lib, population_policy)

# Not sure how useful is this, one can always just keep track of the libraries created and clear them
def clear_all_modifiable_libs_from_this_process(ac, benchmark_cls):
    lib_names = ac.list_libraries()
    to_deletes = [lib_name for lib_name in lib_names if lib_name.startswith(f"{LibraryType.MODIFIABLE}_{benchmark_cls.__name__}_{os.getpid()}")]
    for to_delete in to_deletes:
        ac.delete_library(to_delete)




