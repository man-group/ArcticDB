"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import atexit
from enum import Enum
import os
import shutil
import sys
import tempfile
from typing import Dict

import arcticdb
from arcticdb.arctic import Arctic
from datetime import datetime
from packaging import version


from logger import get_logger

MACOS = sys.platform.lower().startswith("darwin")
LINUX = sys.platform.lower().startswith("linux")
WINDOWS = sys.platform.lower().startswith("win32")

logger = get_logger()


CONDITION_GCP_AVAILABLE = (
    True if "dev" in arcticdb.__version__ else version.Version(arcticdb.__version__) >= version.Version("5.3.0")
)

CONDITION_AZURE_AVAILABLE = (
    True if "dev" in arcticdb.__version__ else version.Version(arcticdb.__version__) >= version.Version("4.0.0")
)

__temp_paths = []
## Session scoped clients
__ARCTIC_CLIENT_AWS_S3: Arctic = dict()
__ARCTIC_CLIENT_AZURE: Arctic = dict()
__ARCTIC_CLIENT_GPC: Arctic = dict()
__ARCTIC_CLIENT_LMDB: Arctic = dict()


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


if CONDITION_GCP_AVAILABLE and CONDITION_AZURE_AVAILABLE:
    logger.info("VERSION with AZURE and GCP")
    class StorageTypes(Enum):
        LMDB = 1,
        REAL_AWS_S3 = 2,
        REAL_GCP = 3,
        REAL_AZURE = 4,
elif CONDITION_AZURE_AVAILABLE:
    logger.info("VERSION with AZURE")
    class StorageTypes(Enum):
        LMDB = 1,
        REAL_AWS_S3 = 2,
        REAL_AZURE = 4,
else:
    logger.info("NO GCP")
    class StorageTypes(Enum):
        LMDB = 1,
        REAL_AWS_S3 = 2,


def is_storage_enabled(storage_type: StorageTypes) -> bool:
    persistent_storage = os.getenv("ARCTICDB_PERSISTENT_STORAGE_TESTS", "0") == "1"
    if not persistent_storage:
        return False
    
    if CONDITION_GCP_AVAILABLE:
        if storage_type == StorageTypes.REAL_GCP:
            if os.getenv("ARCTICDB_STORAGE_GCP", "0") == "1":        
                return True
            else:
                return False
            
    if CONDITION_AZURE_AVAILABLE:
        if storage_type == StorageTypes.REAL_AZURE:
            if os.getenv("ARCTICDB_STORAGE_AZURE", "0") == "1":        
                return True
            else:
                return False
        
    if storage_type == StorageTypes.LMDB:
        if os.getenv("ARCTICDB_STORAGE_LMDB", "1") == "1":
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

def real_azure_credentials(shared_path: bool = True):
    if shared_path:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    else:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX", "")
    connection_str = os.getenv("ARCTICDB_REAL_AZURE_CONNECTION_STRING")
    container = os.getenv("ARCTICDB_REAL_AZURE_CONTAINER")
    return container, connection_str, path_prefix


def get_real_azure_uri(shared_path: bool = True):
    (
        container,
        connection_str,
        path_prefix,
    ) = real_azure_credentials(shared_path)
    ca_certs = ""
    if LINUX:
        ca_certs_file = "/etc/ssl/certs/ca-certificates.crt"
        os.path.exists(ca_certs_file), f"CA file: {ca_certs_file} not found!"
        ca_certs = f";CA_cert_path={ca_certs_file}"
    azure_uri = f"azure://Container={container};Path_prefix={path_prefix};{connection_str}{ca_certs}"
    return azure_uri


def create_arctic_client(storage: StorageTypes, **extras) -> Arctic:
    """ A base function that should be use to create client in fixtures and in tests.

    Fixtures are not always optimal ways to serve as common code for client creation.

    There are 2 general problems with that
     - Fixtures are tightly coupled with pytest. Thus they make sense only when used in 
       tests with pytest. They cannot be easily and freely reused otherwise. And in testing 
       we do need to have common code that can be used in tests and outside of tests
     - Fixtures trigger overuse of bundling their parameters with the fixture, tightly coupling 
       a test with HOW and potentially WHERE it is executed. That at first glance is good because 
       it is associated with easier management of tests. You just have to make a fix at one place.
       That however is the way the tests should be developed. A test can serve multiple purposes and
       can be executed against many environments that may not exist at the time of the test writing.
       Thus one and the same test may serve as pre-chekin fast test where the storage could be local like
       lmdb, or could be part of larger test suite that executes overnight over all supported storages
       That cannot be modeled with fixtures which has 100% of the options bundeled with them. 
       And that is over 95% of tests purposes

    With this client factory the aim is to address all those weaknesses. For installation tests we have 
    common code - one place at which the logic for creating clients is placed. 

    Since this is the place where clients are created we have to account the specific for arctic 
    working with real storages = ie the recommendation to use one Arctic client per storage/library

    Therefore the code creates a hash of clients. The key to the hash is the URL + extras.
    That change is similar to "session" scope of a fixture.
    """

    sorted_extras = dict(sorted(extras.items()))

    def create_arctic(dct: Dict[str, Arctic], uri: str, extras) -> Arctic:
        key = f"{uri}{extras}"
        if key not in dct:
            dct[key] = Arctic(uri, **extras)
        return dct[key]

    if CONDITION_GCP_AVAILABLE:
        if storage == StorageTypes.REAL_GCP and is_storage_enabled(storage):
            global __ARCTIC_CLIENT_GPC
            uri = get_real_gcp_uri(shared_path=False)
            return create_arctic(__ARCTIC_CLIENT_GPC, uri, sorted_extras)

    if CONDITION_AZURE_AVAILABLE:
        if storage == StorageTypes.REAL_AZURE and is_storage_enabled(storage):
            global __ARCTIC_CLIENT_AZURE
            uri = get_real_azure_uri(shared_path=False)
            return create_arctic(__ARCTIC_CLIENT_AZURE, uri, sorted_extras)

    if storage == StorageTypes.LMDB and is_storage_enabled(storage):
        global __ARCTIC_CLIENT_LMDB
        uri = f"lmdb://{str(get_temp_path())}_{str((sorted_extras))}"
        return create_arctic(__ARCTIC_CLIENT_AZURE, uri, sorted_extras)
    
    elif storage == StorageTypes.REAL_AWS_S3 and is_storage_enabled(storage):
        global __ARCTIC_CLIENT_AWS_S3
        uri = get_real_s3_uri(shared_path=False)
        return create_arctic(__ARCTIC_CLIENT_AWS_S3, uri, sorted_extras)
    return None


def delete_library(ac: Arctic, lib_name: str):
    try:
        ac.delete_library(lib_name)
    except Exception as e:
        logger.warning(f"Error while deleting library: {e}. \n url: {ac.get_uri()}")