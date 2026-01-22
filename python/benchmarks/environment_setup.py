"""
Copyright 2025 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random
from enum import Enum
import os
from typing import List, Optional, Iterable, Dict

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.storage_fixtures.s3 import real_s3_from_environment_variables
from arcticdb.util.test import random_ascii_string
from arcticdb.util.test_utils import DFGenerator, ListGenerators, TimestampNumber
from arcticdb.util.utils import strtobool
from arcticdb.version_store.library import Library


class Storage(Enum):
    AMAZON = 1
    LMDB = 2
    GOOGLE = 3
    AZURE = 4


def is_storage_enabled(storage: Storage) -> bool:
    """Decide which storages to use for ASV testing based on environment variables.

    ARCTICDB_STORAGE_LMDB (default: True): Test against LMDB
    ARCTICDB_STORAGE_AWS_S3 (default: False): Test against AWS
    """
    if storage == Storage.LMDB:
        return strtobool(os.getenv("ARCTICDB_STORAGE_LMDB", "1"))
    elif storage == Storage.AMAZON:
        return strtobool(os.getenv("ARCTICDB_STORAGE_AWS_S3", "0"))
    else:
        raise RuntimeError(f"Only LMDB and AMAZON storages are supported, received {storage}")


def create_libraries(
    storage: Storage, library_names: List[str], library_options: LibraryOptions = LibraryOptions()
) -> List[Optional[Library]]:
    """
    Create a library for a given storage test, for use in ASV benchmarking runs.

    Notes

    LMDB: ASV creates a temp directory for each run and then sets current working directory to it
    After the end of the test it removes all files and subfolders created there.

    AWS:

    Example local setup:

    `aws s3 mb s3://<bucket-name> --region eu-west-2`

    Then write,

    ```aws.env
    ARCTICDB_STORAGE_AWS_S3=1
    ARCTICDB_REAL_S3_ACCESS_KEY=<redacted>
    ARCTICDB_REAL_S3_BUCKET=<bucket-name>
    ARCTICDB_REAL_S3_CLEAR=0
    ARCTICDB_REAL_S3_ENDPOINT=https://s3.eu-west-2.amazonaws.com
    ARCTICDB_REAL_S3_REGION=eu-west-2
    ARCTICDB_REAL_S3_SECRET_KEY=<redacted>
    ```

    then `export $(cat aws.env | xargs)`. Your environment is now ready to run the AWS ASV tests.

    We rely on cleanup scripts in the CI to clean up, by dropping the `ARCTICDB_REAL_S3_BUCKET`. You need to do this
    manually when working locally to save AWS costs.

    Tear down command:

    `aws s3 rb s3://<bucket-name> --region eu-west-2`
    """
    if not is_storage_enabled(storage):
        return [None] * len(library_names)

    if storage == Storage.LMDB:
        ac = Arctic(f"lmdb://tmp/{random.randint(0, 100_000)}")
    elif storage == Storage.AMAZON:
        factory = real_s3_from_environment_variables(shared_path=True, validate=True, log=True)
        ac = factory.create_fixture().create_arctic()
    else:
        raise RuntimeError(f"storage {storage} not implemented for benchmark {__file__}")

    res = []
    for l in library_names:
        ac.delete_library(l)
        res.append(ac.create_library(l, library_options=library_options))

    return res


def create_library(storage: Storage, library_options: LibraryOptions = LibraryOptions()) -> Optional[Library]:
    """
    Create a library for a given storage test, for use in ASV benchmarking runs.

    Returns None if the storage is not enabled.

    See create_libraries docstring for notes on running locally.
    """
    if not is_storage_enabled(storage):
        return None

    lib_name = random_ascii_string(10)
    return create_libraries(storage, [lib_name], library_options)[0]


def create_libraries_across_storages(
    storages: Iterable[Storage], library_options: LibraryOptions = LibraryOptions()
) -> Dict[Storage, Optional[Library]]:
    """Create libraries for benchmarking on each of storages. If the storage not is_storage_enabled, the value
    for that storage will be None."""
    return {s: create_library(s, library_options) for s in storages}
