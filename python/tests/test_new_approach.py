"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


from enum import Enum
from typing import Callable, Dict
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
    LMDB = "lmdb"
    REAL_S3 = "real_s3"
    MEM = "mem"

def is_enabled(storage: StorageType) -> bool:
    if storage == StorageType.LMDB:
        return STORAGE_LMDB 
    if storage == StorageType.REAL_S3:
        return STORAGE_AWS_S3 
    if storage == StorageType.MEM:
        return STORAGE_MEM

@pytest.fixture(params=[s for s in StorageType if is_enabled(s)])
def storage_type(request):
    return request.param

StoreSelector = Callable[[StorageType, EncodingVersion], Arctic]

@pytest.fixture
def client_factory(lmdb_storage: LmdbStorageFixture, 
                   mem_storage: InMemoryStorageFixture, 
                   real_s3_storage: S3Bucket) -> StoreSelector:
    def get_factory(store_type: StorageType, encoding_version: EncodingVersion) -> Arctic:
        mapping: Dict[StorageType, StorageFixture] = {
            StorageType.LMDB: lmdb_storage,
            StorageType.MEM: mem_storage,
            StorageType.REAL_S3: real_s3_storage,
        }
        return mapping[store_type].create_arctic(encoding_version=encoding_version)
    return get_factory

def test_some_test(arctic_client, lib_name):
    ac: Arctic = arctic_client
    lib = ac.create_library(lib_name)
    lib.write("s", pd.DataFrame({'column': [1, 2]}, index=pd.date_range("2023-01-01", periods=2)))

def test_some_test1(client_factory, lib_name):
    ac: Arctic = client_factory(StorageType.REAL_S3, EncodingVersion.V1)
    lib = ac.create_library(lib_name)
    lib.write("s", pd.DataFrame({'column': [1, 2]}, index=pd.date_range("2023-01-01", periods=2)))    