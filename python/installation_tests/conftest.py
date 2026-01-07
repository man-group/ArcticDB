"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from datetime import datetime
import os
import random
import re
import sys
import threading
import traceback
from typing import Generator
import uuid
import pytest

import arcticdb as adb
from arcticdb.version_store.library import Library
from arcticdb.arctic import Arctic

from client_utils import StorageTypes, create_arctic_client
from logger import get_logger

logger = get_logger()


@pytest.fixture()
def lib_name(request: "pytest.FixtureRequest") -> str:
    name = re.sub(r"[^\w]", "_", request.node.name)[:30]
    pid = os.getpid()
    thread_id = threading.get_ident()
    # There is limit to the name length, and note that without
    # the dot (.) in the name mongo will not work!
    name = f"{name}.{pid}_{thread_id}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S')}_{uuid.uuid4()}"
    return str(hash(name))


@pytest.fixture(scope="function", params=StorageTypes)
def ac_client(request) -> Generator[Arctic, None, None]:
    extras = {}
    if hasattr(request, "param") and request.param:
        if isinstance(request.param, StorageTypes):
            storage = request.param
        else:
            storage, extras = request.param
    logger.info(f"Create arctic type: {storage}")
    ac: Arctic = create_arctic_client(storage, **extras)
    arctic_uri = ac.get_uri() if ac else "Arctic is None (not created)"
    logger.info(f"Arctic uri : {arctic_uri}")
    if ac is None:
        pytest.skip("Storage not activated")
    yield ac
    libs = ac.list_libraries()
    for lname in libs:
        logger.error(
            f"Library '{lname}' not deleted after test."
            + "You have to delete it in test with try: ... finally: delete_library(ac, lib_name)"
        )
        if not (os.getenv("GITHUB_ACTIONS") == "true"):
            raise Exception(
                "(Development only exception): "
                + "You receive this error because there is undeleted data in storage."
                + "Check the error message above and and fix the tests."
                + "This error will not be raised in Github"
            )


@pytest.fixture(scope="function")
def ac_library_factory(request, ac_client, lib_name) -> Library:
    def create_library(library_options=None, name: str = lib_name):
        logger.info(f"Create library : {lib_name}")
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
    if ac is None:
        pytest.skip()
    logger.info(f"Create library : {lib_name}")
    ac.create_library(lib_name, **config)
    lib = ac.get_library(lib_name)
    yield lib
    ac.delete_library(lib_name)


# region Pytest special xfail handling


def pytest_runtest_makereport(item, call):
    import pytest_xfail

    return pytest_xfail.pytest_runtest_makereport(item, call)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    import pytest_xfail

    pytest_xfail.pytest_terminal_summary(terminalreporter, exitstatus)


# endregion
