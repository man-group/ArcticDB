"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import atexit
from enum import Enum
import random
import shutil
import tempfile


from arcticdb.arctic import Arctic
from arcticdb.storage_fixtures.lmdb import LmdbStorageFixture
from arcticdb.storage_fixtures.s3 import real_gcp_from_environment_variables, real_s3_from_environment_variables
from datetime import datetime
from tests.util.mark import LOCAL_STORAGE_TESTS_ENABLED, REAL_GCP_TESTS_MARK, REAL_S3_TESTS_MARK
from tests.util.storage_test import get_real_s3_uri, get_real_gcp_uri


__temp_paths = []
## Session scoped clients
__ARCTIC_CLIENT_AWS_S3: Arctic = None
__ARCTIC_CLIENT_GPC: Arctic = None


def get_temp_path():
    """Creates and returns a temporary directory path."""
    temp_dir = tempfile.mkdtemp() 
    __temp_paths.append(temp_dir)   
    return temp_dir


def __cleanup_temp_paths():
    """Deletes all temporary paths created during work."""
    for path in __temp_paths:
        shutil.rmtree(path, ignore_errors=True)  
    __temp_paths.clear()


atexit.register(__cleanup_temp_paths)    


class StorageTypes(Enum):
    LMDB = 1,
    REAL_AWS_S3 = 2,
    REAL_GCP = 3,


def is_storage_enabled(storage_type: StorageTypes) -> bool:
    if storage_type == StorageTypes.LMDB:
        return LOCAL_STORAGE_TESTS_ENABLED
    elif storage_type == StorageTypes.REAL_GCP:
        return REAL_GCP_TESTS_MARK.args[0] == False
    elif storage_type == StorageTypes.REAL_AWS_S3:
        return REAL_S3_TESTS_MARK.args[0] == False
    else:
        raise ValueError(f"Invalid storage type: {storage_type}")


def create_arctic_client(storage: StorageTypes, **extras) -> Arctic:
    if storage == StorageTypes.LMDB and is_storage_enabled(storage):
        return Arctic("lmdb://" + str(get_temp_path()), **extras)
    elif storage == StorageTypes.REAL_GCP and is_storage_enabled(storage):
        global __ARCTIC_CLIENT_GPC
        if __ARCTIC_CLIENT_GPC is None:
            __ARCTIC_CLIENT_GPC = Arctic(get_real_gcp_uri(shared_path=False), **extras)
        return __ARCTIC_CLIENT_GPC
    elif storage == StorageTypes.REAL_AWS_S3 and is_storage_enabled(storage):
        global __ARCTIC_CLIENT_AWS_S3
        if __ARCTIC_CLIENT_AWS_S3 is None:
            __ARCTIC_CLIENT_AWS_S3 = Arctic(get_real_s3_uri(shared_path=False), **extras)
        return __ARCTIC_CLIENT_AWS_S3
    return None
