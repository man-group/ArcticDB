"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from enum import Enum
import enum
from typing import Callable, Dict, List, Optional
import pytest
from arcticdb.arctic import Arctic
import pandas as pd

from arcticdb.encoding_version import EncodingVersion
from arcticdb.storage_fixtures.api import StorageFixture
from arcticdb.storage_fixtures.in_memory import InMemoryStorageFixture
from arcticdb.storage_fixtures.lmdb import LmdbStorageFixture
from arcticdb.storage_fixtures.s3 import S3Bucket
from tests.util.mark import STORAGE_AWS_S3, STORAGE_LMDB, STORAGE_MEM


class StorageType(Enum):
    """Storage types become enum"""
    LMDB = "lmdb"
    REAL_S3 = "real_s3"
    MEM = "mem"

    ## Now we define all possible sets as static methods 
    ## So class covers all needs

    @classmethod
    def all(cls) -> List["StorageType"]:
        return list(cls)

    @classmethod
    def local(cls) -> List["StorageType"]:
        return [cls.LMDB, cls.MEM]

    @classmethod
    def real(cls) -> List["StorageType"]:
        return [cls.REAL_S3]
    
    # A centralized way to check is storage is enabled or disables
    # It will be used in factory fixtures
    @classmethod
    def is_enabled(storage: "StorageType") -> bool:
        if storage == StorageType.LMDB:
            return STORAGE_LMDB 
        if storage == StorageType.REAL_S3:
            return STORAGE_AWS_S3 
        if storage == StorageType.MEM:
            return STORAGE_MEM
        else:
            raise TypeError(f"Unsupported type {storage}")
    
@enum.unique
class EncodingVersions(enum.IntEnum):
    V1 = EncodingVersion.V1
    V2 = EncodingVersion.V2

    @classmethod
    def all(cls) -> List[EncodingVersion]:
        return list(EncodingVersions)

    @classmethod
    def default(cls) -> List[EncodingVersion]:
        return [cls.V1]
    
    @classmethod
    def only_v1(cls) -> List[EncodingVersion]:
        return [cls.V1]

    @classmethod
    def is_enabled(cls, version: EncodingVersion) -> bool:
        return version == cls.V1


# A centralized way to check is storage is enabled or disables
# It will be used in factory fixtures
def is_enabled(storage: StorageType) -> bool:
    if storage == StorageType.LMDB:
        return STORAGE_LMDB 
    if storage == StorageType.REAL_S3:
        return STORAGE_AWS_S3 
    if storage == StorageType.MEM:
        return STORAGE_MEM

_storage_mappings:Dict[StorageType, StorageFixture] = {}

@pytest.fixture(scope= 'session',
                params=[s for s in StorageType if is_enabled(s)])
def storage_type(request):
    return request.param

StoreSelector = Callable[[StorageType, EncodingVersion], Arctic]

@pytest.fixture
def client_factory(lmdb_storage: LmdbStorageFixture, 
                   mem_storage: InMemoryStorageFixture, 
                   real_s3_storage: S3Bucket) -> StoreSelector:
    def get_factory(store_type: StorageType, encoding_version: EncodingVersion) -> Arctic:
        global _storage_mappings
        if not _storage_mappings:
            mapping: Dict[StorageType, Optional[EncodingVersion]] = {
                StorageType.LMDB: lmdb_storage if is_enabled(StorageType.LMDB) else None,
                StorageType.MEM: mem_storage if is_enabled(StorageType.MEM) else None,
                StorageType.REAL_S3: real_s3_storage if is_enabled(StorageType.REAL_S3) else None,
            }
        if mapping[store_type] is None: 
            pytest.skip(f"Storage type {store_type} is not enabled")
        return mapping[store_type].create_arctic(encoding_version=encoding_version)
    return get_factory

# A Traditional test
def test_some_test(arctic_client, lib_name):
    ac: Arctic = arctic_client
    lib = ac.create_library(lib_name)
    lib.write("s", pd.DataFrame({'column': [1, 2]}, index=pd.date_range("2023-01-01", periods=2)))

def test_some_test1(client_factory, lib_name):
    ac: Arctic = client_factory(StorageType.REAL_S3, EncodingVersion.V1)
    lib = ac.create_library(lib_name)
    lib.write("s", pd.DataFrame({'column': [1, 2]}, index=pd.date_range("2023-01-01", periods=2)))    

@pytest.mark.parametrize("storage_type", StorageType.local())
@pytest.mark.parametrize("encoding_version", EncodingVersions.all())
def test_some_test2(client_factory, lib_name, storage_type, encoding_version):
    ac: Arctic = client_factory(storage_type, encoding_version)
    lib = ac.create_library(lib_name)
    lib.write("s", pd.DataFrame({'column': [1, 2]}, index=pd.date_range("2023-01-01", periods=2)))        

# We can see that there is lot of repeating code that can potentially become too much 
# Can we make it better:
# 

class Param:
    """ The class serves for common parameter types
    """
    DEFAULT_ENCODINGS = EncodingVersions.default()
    DEFAULT_STORAGES = StorageType.local()

    @staticmethod
    def encoding_version(values=None):
        return pytest.mark.parametrize("encoding_version", values or Param.DEFAULT_ENCODINGS)

    @staticmethod
    def storage_type(values=None):
        return pytest.mark.parametrize("storage_type", values or Param.DEFAULT_STORAGES)

@Param.encoding_version()
@Param.storage_type(StorageType.all())
def test_some_test3(client_factory, lib_name, storage_type, encoding_version):
    ac: Arctic = client_factory(storage_type, encoding_version)
    lib = ac.create_library(lib_name)
    lib.write("s", pd.DataFrame({'column': [1, 2]}, index=pd.date_range("2023-01-01", periods=2)))        
