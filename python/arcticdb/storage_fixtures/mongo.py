"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import tempfile
import time
import logging
from typing import TYPE_CHECKING, Optional

from .api import *
from .utils import get_ephemeral_port, GracefulProcessUtils, wait_for_server_to_come_up, safer_rmtree
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
from arcticdb.version_store.helper import add_mongo_library_to_env
from arcticdb.adapters.prefixing_library_adapter_decorator import PrefixingLibraryAdapterDecorator

# All storage client libraries to be imported on-demand to speed up start-up of ad-hoc test runs
if TYPE_CHECKING:
    from pymongo import MongoClient

# Configure the root logger to INFO, since all ArcticDB loggers default to INFO
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Mongo Storage Fixture")
log_level = os.getenv("ARCTICDB_mongo_test_fixture_loglevel")
if log_level:
    log_level = log_level.upper()
    assert log_level in {"DEBUG", "INFO", "WARN", "ERROR"}, \
        "Log level must be one of DEBUG, INFO, WARN, ERROR"
    logger.setLevel(getattr(logging, log_level))


class MongoDatabase(StorageFixture):
    """Each fixture is backed by its own Mongo database, to make clean up easier."""

    def __init__(self, mongo_uri: str, name: Optional[str] = None, client: Optional["MongoClient"] = None):
        """
        Parameters
        ----------
        mongo_uri
            URI to the MongoDB server, must start with "mongodb://".
        name
            The database name to use in the MongoDB server. Must not contain ``?`` symbol.
            If not supplied, a random name is generated.
            Regardless of whether the database exists before, it will be removed on ``__exit__`` of this Fixture.
        client
            Optionally reusing a client already connected to ``mongo_uri``.
        """
        super().__init__()
        from pymongo import MongoClient

        assert mongo_uri.startswith("mongodb://")
        self.mongo_uri = mongo_uri
        self.client = client or MongoClient(mongo_uri)
        if not name:
            while True:
                logger.debug("Searching for new name")
                name = f"MongoFixture{int(time.time() * 1e6)}"
                if name not in self.client.list_database_names():
                    break
                time.sleep(0.01)
        self.prefix = name + "."

        PrefixingLibraryAdapterDecorator.ensure_registered()
        self.arctic_uri = PrefixingLibraryAdapterDecorator.prefix_uri(self.prefix, mongo_uri)

    def __exit__(self, exc_type, exc_value, traceback):
        self.libs_from_factory.clear()

        with handle_cleanup_exception(self, "prefix_mongo_database"):
            self.client.drop_database("arcticc_" + self.prefix[:-1])
        with handle_cleanup_exception(self, "pymongo client", consequence="The test process may never exit"):
            self.client.close()
            self.client = None

        # With Mongo, the LibraryManager configuration database is global/reused across fixtures, so must delete the
        # library definitions
        self.slow_cleanup()

    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        add_mongo_library_to_env(cfg, lib_name=lib_name, env_name=Defaults.ENV, uri=self.mongo_uri)
        return cfg

    def set_permission(self, *, read: bool, write: bool):
        raise NotImplementedError("Will support setting permissions on Mongo soon")  # TODO


class ExternalMongoDBServer(StorageFixtureFactory):
    """A MongoDB server whose life-cycle is managed externally to this test fixture system."""

    def __init__(self, mongo_uri: str):
        self.mongo_uri = mongo_uri

    def __str__(self):
        return f"{type(self).__name__}[{self.mongo_uri}]"

    def create_fixture(self) -> StorageFixture:
        return MongoDatabase(self.mongo_uri)


class ManagedMongoDBServer(StorageFixtureFactory):
    """Represents a MongoDB server started by this class"""

    _count = -1

    def __init__(self, data_dir: Optional[str] = None, port=0, executable="mongod"):
        self._data_dir = data_dir or tempfile.mkdtemp("ManagedMongoDBServer")
        self._port = port or get_ephemeral_port(5)
        self._executable = executable
        self._client = None

    def _safe_enter(self):
        from pymongo import MongoClient

        cmd = [self._executable, "--port", str(self._port), "--dbpath", self._data_dir]
        self._p = GracefulProcessUtils.start(cmd)
        self.mongo_uri = f"mongodb://localhost:{self._port}"
        wait_for_server_to_come_up(f"http://localhost:{self._port}", "mongod", self._p)
        self._client = MongoClient(self.mongo_uri)

    def __exit__(self, exc_type, exc_value, traceback):
        if self._client:
            with handle_cleanup_exception(self):
                self._client["admin"].command({"shutdown": 1, "force": True, "timeoutSecs": 1})

        with handle_cleanup_exception(self, "process", consequence="On-disk data might not be delete-able. "):
            GracefulProcessUtils.terminate(self._p)

        safer_rmtree(self, self._data_dir)

    def create_fixture(self) -> StorageFixture:
        self._count += 1
        return MongoDatabase(self.mongo_uri, f"Managed{self._count}")

    def __str__(self):
        return f"{type(self).__name__}[{self.mongo_uri}]"


def is_mongo_host_running(host):
    import requests
    try:
        res = requests.get(f"http://{host}")
    except requests.exceptions.ConnectionError:
        return False
    return res.status_code == 200 and "mongodb" in res.text.lower()


def auto_detect_server():
    """Use the Server specified by the CI_MONGO_HOST env var. If not set, try localhost before falling back to starting
    a dedicated instance on a random port."""
    CI_MONGO_HOST = "CI_MONGO_HOST"
    mongo_host = os.getenv(CI_MONGO_HOST)
    if mongo_host:
        host = f"{mongo_host}:27017"
        if is_mongo_host_running(host):
            return ExternalMongoDBServer(f"mongodb://{host}")
        else:
            logger.debug(f"Could not connect to {CI_MONGO_HOST}={mongo_host}, so will try localhost.")
    else:
        logger.debug(f"Env var {CI_MONGO_HOST} not set, so will try localhost.")

    host = "localhost:27017"
    if is_mongo_host_running(host):
        return ExternalMongoDBServer(f"mongodb://{host}")
    else:
        logger.debug("Could not connect to localhost, so falling back to managed instance.")

    return ManagedMongoDBServer()
