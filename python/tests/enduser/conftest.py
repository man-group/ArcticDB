"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import Generator
import pytest

import arcticdb as adb
from arcticdb.version_store.library import Library

from arcticdb.arctic import Arctic
from tests.conftest import filter_out_unwanted_mark
from tests.enduser.client_utils import StorageTypes, create_arctic_client


@pytest.fixture
def arctic_cl(tmp_path, lib_name) -> Generator[Library, None, None]:
    ac = adb.Arctic("lmdb://" + str(tmp_path)) 
    lib = ac.create_library(lib_name)
    yield lib
    ac.delete_library(lib_name)


@pytest.fixture(scope="function", params=StorageTypes)
def ac_client(request) -> Generator[Arctic, None, None]:
    filter_out_unwanted_mark(request, request.param)
    extras = {}
    if hasattr(request, "param") and request.param:
        if isinstance(request.param, StorageTypes):
            storage = request.param
        else:
            storage, extras = request.param
    ac = create_arctic_client(storage, **extras)            
    if ac is None:
        pytest.skip("Storage not activated")
    yield ac


@pytest.fixture(scope="function")
def ac_library_factory(request, ac_client, lib_name) -> Library:
    def create_library(library_options=None, name: str = lib_name):
        return ac_client.create_library(name, library_options)

    return create_library


@pytest.fixture(scope="function")
def ac_library(request, ac_client, lib_name) -> Generator[Library, None, None]:
    config = {}
    if hasattr(request, "param") and request.param:
        config = request.param
    ac: Arctic = ac_client
    if ac is None: pytest.skip()
    yield ac.create_library(lib_name, **config)
    ac.delete_library(lib_name)    


