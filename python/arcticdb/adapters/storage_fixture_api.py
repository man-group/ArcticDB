"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import multiprocessing
import subprocess
import os
import platform
import signal
import functools
import socketserver
from typing import Dict, Union, Pattern
from contextlib import AbstractContextManager, contextmanager
from abc import abstractmethod
from enum import Enum

from arcticdb.config import Defaults
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store.helper import ArcticMemoryConfig
from arcticdb.util.test import apply_lib_cfg
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap

_WINDOWS = platform.system() == "Windows"


def get_ephemeral_port():  # https://stackoverflow.com/a/61685162/
    with socketserver.TCPServer(("localhost", 0), None) as s:
        return s.server_address[1]


def start_process_that_can_be_gracefully_killed(cmd, **kwargs):
    if isinstance(cmd, str) and not kwargs.get("shell"):
        cmd = cmd.split()
    print("Running", cmd)
    creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if _WINDOWS else 0
    return subprocess.Popen(cmd, creationflags=creation_flags, **kwargs)


def gracefully_kill_process(p: Union[multiprocessing.Process, subprocess.Popen]):
    if _WINDOWS:
        # On windows p.terminate() == p.kill(), so close the console first to give the process a chance to clean up
        # https://learn.microsoft.com/en-us/windows/console/generateconsolectrlevent
        os.kill(p.pid, signal.CTRL_BREAK_EVENT)
    p.terminate()
    if not _WINDOWS:
        try:
            if isinstance(p, multiprocessing.Process):
                p.join(timeout=2)
                exitcode = p.exitcode
            else:
                exitcode = p.wait(timeout=2)
        except:
            exitcode = None
        if exitcode is None:
            os.kill(p.pid, signal.SIGKILL)  # TODO (python37): use Process.kill()


def _version_store_factory_impl(
    used, make_cfg, default_name, *, name: str = None, reuse_name=False, **kwargs
) -> NativeVersionStore:
    """Common logic behind all the factory fixtures"""
    name = name or default_name
    if name == "_unique_":
        name = name + str(len(used))
    assert (name not in used) or reuse_name, f"{name} is already in use"
    cfg = make_cfg(name)
    lib = cfg.env_by_id[Defaults.ENV].lib_by_path[name]
    # Use symbol list by default (can still be overridden by kwargs)
    lib.version.symbol_list = True
    apply_lib_cfg(lib, kwargs)
    out = ArcticMemoryConfig(cfg, Defaults.ENV)[name]
    used[name] = out
    return out


class ArcticUriFields(Enum):
    """For use with ``replace_uri_field()`` below"""
    HOST = "HOST"
    USER = "USER"
    PASSWORD = "PASSWORD"
    BUCKET = "BUCKET"

    def __str__(self):
        return self.value


class StorageFixture(AbstractContextManager):
    """Manages the life-cycle of a piece of storage and provides test facing methods:"""

    _FIELD_REGEX: Dict[ArcticUriFields, Pattern]
    """Used by ``replace_uri_field()``."""

    arctic_uri: str
    """The URI of this Storage for use with the ``Arctic`` constructor"""

    generated_libs: Dict[str, NativeVersionStore] = {}

    def __enter__(self):
        """Fixtures are typically set up in __init__, so this just returns self"""
        return self

    @abstractmethod
    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        """Creates a new storage config instance.
        If ``lib_name`` is the same as a previous call on this instance, then that storage should be reused."""

    def create_version_store_factory(self, default_lib_name: str):
        """Returns a function that takes optional library options and produces ``NativeVersionStore``s"""
        return functools.partial(
            _version_store_factory_impl, self.generated_libs, self.create_test_cfg, default_lib_name
        )

    @abstractmethod
    def set_permission(self, *, read: bool, write: bool):
        """Makes the connection to the storage have the given permissions. If unsupported, call ``pytest.skip()``.
        See ``set_enforcing_permissions`` below."""

    @classmethod
    def replace_uri_field(cls, uri: str, field: ArcticUriFields, replacement: str):
        regex = cls._FIELD_REGEX[field]
        match = regex.search(uri)
        assert match, f"{uri} does not have {field}"
        return f"{uri[:match.start(2)]}{replacement}{uri[match.end(2):]}"


class StorageFixtureFactory(AbstractContextManager):
    """For ``StorageFixture``s backed by shared/expensive resources, implement this class to manage those."""

    def __exit__(self, exc_type, exc_value, traceback):
        """Properly clean up the fixture. This should be safe to be called multiple times."""

    @property
    def enforcing_permissions(self):
        """Indicates whether the Factory will create Fixtures that enforces permissions.

        If supported (settable), the new value should affect subsequent calls to ``create_fixture()``
        and possibly existing fixtures."""
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
    def create_fixture(self) -> StorageFixture:
        ...
