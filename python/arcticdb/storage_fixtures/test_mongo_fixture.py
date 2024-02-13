import subprocess
import warnings
from dataclasses import dataclass, field
import os
import tempfile
import time
from typing import TYPE_CHECKING, Optional, Any
from abc import abstractmethod

from utils import get_ephemeral_port, GracefulProcessUtils, wait_for_server_to_come_up, \
    safer_rmtree, _DEBUG

# from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
# from arcticdb.version_store.helper import add_mongo_library_to_env
# from arcticdb.adapters.prefixing_library_adapter_decorator import PrefixingLibraryAdapterDecorator

# All storage client libraries to be imported on-demand to speed up start-up of ad-hoc test runs
if TYPE_CHECKING:
    from pymongo import MongoClient

from contextlib import AbstractContextManager, contextmanager
class _SaferContextManager(AbstractContextManager):
    def __enter__(self):
        try:
            self._safe_enter()
            return self
        except Exception as e:
            self.__exit__(type(e), e, None)
            raise

    def _safe_enter(self):
        """Setup that doesn't start external resources can be in __init__.
        Resources that need `__exit__()` cleanup during exceptions should be here."""

class StorageFixtureFactory(_SaferContextManager):
    """For ``StorageFixture``s backed by shared/expensive resources, implement this class to manage those."""

    @abstractmethod
    def __str__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        """Properly clean up the fixture. This should be safe to be called multiple times."""

    @property
    def enforcing_permissions(self):
        """Indicates whether this Factory will create Fixtures that enforces permissions.

        If implemented, the set value should affect subsequent calls to ``create_fixture()`` and, if explicitly
        documented, existing fixtures.

        The base implementation is read-only and always return ``False``."""
        return False

    @contextmanager
    def enforcing_permissions_context(self, set_to=True):
        saved = self.enforcing_permissions
        try:
            self.enforcing_permissions = set_to
            yield
        finally:
            self.enforcing_permissions = saved

    @abstractmethod
    def create_fixture(self):
        ...

class ExceptionInCleanUpWarning(Warning):
    pass


@dataclass
class handle_cleanup_exception(AbstractContextManager):
    """Provides uniform warning containing the given arguments for exceptions in cleanup/__exit__ calls."""

    fixture: Any
    item: Any = ""
    consequence: str = ""
    had_exception: bool = field(default=False, repr=False)

    def __exit__(self, exc_type, e, _):
        if exc_type:
            self.had_exception = True
            warning = ExceptionInCleanUpWarning(f"Error while cleaning up {self}: {exc_type.__qualname__}: {e}")
            warning.__cause__ = e
            warnings.warn(warning)
            return not _DEBUG



class MongoDatabase():
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
        # Note: Not inheriting StorageFixture as requires an arcticdb build!
        from pymongo import MongoClient

        assert mongo_uri.startswith("mongodb://")
        self.mongo_uri = mongo_uri
        self.client = client or MongoClient(mongo_uri)
        if not name:
            while True:
                name = f"MongoFixture{int(time.time() * 1e6)}"
                if name not in self.client.list_database_names():
                    break
                time.sleep(0.01)
        self.prefix = name + "."

        # PrefixingLibraryAdapterDecorator.ensure_registered()
        # self.arctic_uri = PrefixingLibraryAdapterDecorator.prefix_uri(self.prefix, mongo_uri)

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

    def create_test_cfg(self, lib_name: str): #-> EnvironmentConfigsMap:
        #cfg = EnvironmentConfigsMap()
        # add_mongo_library_to_env(cfg, lib_name=lib_name, env_name=Defaults.ENV, uri=self.mongo_uri)
        #return cfg
        return

    def set_permission(self, *, read: bool, write: bool):
        raise NotImplementedError("Will support setting permissions on Mongo soon")  # TODO



class ExternalMongoDBServer(StorageFixtureFactory):
    """A MongoDB server whose life-cycle is managed externally to this test fixture system."""

    def __init__(self, mongo_uri: str):
        self.mongo_uri = mongo_uri

    def __str__(self):
        return f"{type(self).__name__}[{self.mongo_uri}]"

    def create_fixture(self):
        return MongoDatabase(self.mongo_uri)


class ManagedMongoDBServer(StorageFixtureFactory):
    """Represents a MongoDB server started by this class"""

    _count = -1

    def __init__(self, data_dir: Optional[str] = None, port=0, executable="mongod"):
        self._data_dir = data_dir or tempfile.mkdtemp("ManagedMongoDBServer")
        self._port = port or get_ephemeral_port(5)
        self._executable = executable

    def _safe_enter(self):
        from pymongo import MongoClient

        cmd = [self._executable, "--port", str(self._port), "--dbpath", self._data_dir]
        self._p = GracefulProcessUtils.start(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.mongo_uri = f"mongodb://localhost:{self._port}"
        wait_for_server_to_come_up(f"http://localhost:{self._port}", "mongod", self._p)
        self._client = MongoClient(self.mongo_uri)

    def __exit__(self, exc_type, exc_value, traceback):
        safer_rmtree(self, self._data_dir)

    def create_fixture(self):
        self._count += 1
        return MongoDatabase(self.mongo_uri, f"Managed{self._count}")

    def __str__(self):
        return f"{type(self).__name__}[{self.mongo_uri}]"

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def auto_detect_server():
    """Use the Server specified by the CI_MONGO_HOST env var. If not set, try localhost before falling back to starting
    a dedicated instance on a random port."""
    import requests

    def check_mongo_running(host):
        try:
            res = requests.get(f"http://{host}")
        except requests.exceptions.ConnectionError:
            return False
        return res.status_code == 200 and "mongodb" in res.text.lower()

    mongo_host = os.getenv("CI_MONGO_HOST")
    if mongo_host:
        host = f"{mongo_host}:27017"
        assert check_mongo_running(host)
        return ExternalMongoDBServer(f"mongodb://{host}")
    else:
        logger.log(logging.INFO, "NO CI env var set so trying localhost, then will fallback")

    host = "localhost:27017"
    if check_mongo_running(host):
        return ExternalMongoDBServer(f"mongodb://{host}")
    else:
        logger.log(logging.INFO, "Localhost did not work, so falling back")

    return ManagedMongoDBServer()


if __name__ == "__main__":

    logger.log(logging.INFO, "Starting mongo fixture")
    with auto_detect_server() as server:
        fixture = server.create_fixture()  # for testing got rid of the context manager
        logger.log(logging.INFO, "Got a storage fixture, with URI" + str(fixture.mongo_uri))

    logger.log(logging.INFO, "Successful load of the mongo fixture in the CI")
