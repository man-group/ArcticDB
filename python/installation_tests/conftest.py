"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from datetime import datetime
import os
import random
import re
import threading
from typing import Generator
import uuid
import pytest

import arcticdb as adb
from arcticdb.version_store.library import Library
from arcticdb.arctic import Arctic
from client_utils import StorageTypes, create_arctic_client


@pytest.fixture()
def lib_name(request: "pytest.FixtureRequest") -> str:
    name = re.sub(r"[^\w]", "_", request.node.name)[:30]
    pid = os.getpid()
    thread_id = threading.get_ident()
    return f"{name}.{random.randint(0, 9999999)}_{pid}_{thread_id}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}_{uuid.uuid4()}"[:200]


@pytest.fixture(scope="function", params=StorageTypes)
def ac_client(request) -> Generator[Arctic, None, None]:
    extras = {}
    if hasattr(request, "param") and request.param:
        if isinstance(request.param, StorageTypes):
            storage = request.param
        else:
            storage, extras = request.param
    print("CREATES ARCTIC:", storage)
    ac = create_arctic_client(storage, **extras)            
    if ac is None:
        pytest.skip("Storage not activated")
    yield ac


@pytest.fixture(scope="function")
def ac_library_factory(request, ac_client, lib_name) -> Library:
    def create_library(library_options=None, name: str = lib_name):
        ac_client.create_library(name, library_options)
        lib = ac_client.get_library(name)
        return lib 

    return create_library


@pytest.fixture(scope="function")
def ac_library(request, ac_client, lib_name) -> Generator[Library, None, None]:
    config = {}
    if hasattr(request, "param") and request.param:
        config = request.param
    ac: Arctic = ac_client
    if ac is None: pytest.skip()
    ac.create_library(lib_name, **config)
    lib = ac.get_library(lib_name)
    yield lib
    ac.delete_library(lib_name)    


