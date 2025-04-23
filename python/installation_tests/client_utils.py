"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import atexit
from enum import Enum
import os
import random
import shutil
import tempfile

from arcticdb.arctic import Arctic
from datetime import datetime


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
    persistent_storage = os.getenv("ARCTICDB_PERSISTENT_STORAGE_TESTS", "0") == "1"
    if not persistent_storage:
        return False
    if storage_type == StorageTypes.LMDB:
        if os.getenv("ARCTICDB_LOCAL_STORAGE_TESTS_ENABLED", "1") == "1":
            return True
        else:
            return False
    elif storage_type == StorageTypes.REAL_GCP:
        if os.getenv("ARCTICDB_STORAGE_GCP", "0") == "1":        
            return True
        else:
            return False
    elif storage_type == StorageTypes.REAL_AWS_S3:
        if os.getenv("ARCTICDB_STORAGE_AWS_S3", "0") == "1":        
            return True
        else:
            return False
    else:
        raise ValueError(f"Invalid storage type: {storage_type}")
    

def real_s3_credentials(shared_path: bool = True):
    endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
    bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
    region = os.getenv("ARCTICDB_REAL_S3_REGION")
    access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
    if shared_path:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    else:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX")

    clear = str(os.getenv("ARCTICDB_REAL_S3_CLEAR")).lower() in ("true", "1")

    return endpoint, bucket, region, access_key, secret_key, path_prefix, clear


def real_gcp_credentials(shared_path: bool = True):
    endpoint = os.getenv("ARCTICDB_REAL_GCP_ENDPOINT")
    if endpoint is not None and "://" in endpoint:
       endpoint = endpoint.split("://")[1] 
    bucket = os.getenv("ARCTICDB_REAL_GCP_BUCKET")
    region = os.getenv("ARCTICDB_REAL_GCP_REGION")
    access_key = os.getenv("ARCTICDB_REAL_GCP_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_GCP_SECRET_KEY")
    if shared_path:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    else:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX")

    clear = str(os.getenv("ARCTICDB_REAL_GCP_CLEAR")).lower() in ("true", "1")

    return endpoint, bucket, region, access_key, secret_key, path_prefix, clear


def get_real_s3_uri(shared_path: bool = True):
    (
        endpoint,
        bucket,
        region,
        access_key,
        secret_key,
        path_prefix,
        _,
    ) = real_s3_credentials(shared_path)
    aws_uri = (
        f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix={path_prefix}"
    )
    return aws_uri


def real_gcp_credentials(shared_path: bool = True):
    endpoint = os.getenv("ARCTICDB_REAL_GCP_ENDPOINT")
    if endpoint is not None and "://" in endpoint:
       endpoint = endpoint.split("://")[1] 
    bucket = os.getenv("ARCTICDB_REAL_GCP_BUCKET")
    region = os.getenv("ARCTICDB_REAL_GCP_REGION")
    access_key = os.getenv("ARCTICDB_REAL_GCP_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_GCP_SECRET_KEY")
    if shared_path:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    else:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX")

    clear = str(os.getenv("ARCTICDB_REAL_GCP_CLEAR")).lower() in ("true", "1")

    return endpoint, bucket, region, access_key, secret_key, path_prefix, clear


def get_real_gcp_uri(shared_path: bool = True):
    (
        endpoint,
        bucket,
        region,
        acs_key,
        sec_key,
        path_prefix,
        _,
    ) = real_gcp_credentials(shared_path)
    aws_uri = (
        f"gcpxml://{endpoint}:{bucket}?access={acs_key}&secret={sec_key}&path_prefix={path_prefix}"
    )
    return aws_uri


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
