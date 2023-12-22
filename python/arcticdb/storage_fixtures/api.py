"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import functools
from typing import Dict, Pattern, Callable, List
from contextlib import AbstractContextManager, contextmanager
from abc import abstractmethod
from enum import Enum

from .utils import handle_cleanup_exception
from arcticdb import Arctic
from arcticdb.config import Defaults
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store.helper import ArcticMemoryConfig
from arcticdb.util.test import apply_lib_cfg
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap


class ArcticUriFields(Enum):
    """For use with ``replace_uri_field()`` below"""

    HOST = "HOST"
    USER = "USER"
    PASSWORD = "PASSWORD"
    BUCKET = "BUCKET"
    CA_PATH = "CA_PATH"

    def __str__(self):
        return self.value


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


class StorageFixture(_SaferContextManager):
    """Manages the life-cycle of a piece of storage and provides test facing methods:"""

    _FIELD_REGEX: Dict[ArcticUriFields, Pattern]
    """Used by ``replace_uri_field()``."""

    arctic_uri: str
    """The URI of this Storage for use with the ``Arctic`` constructor"""

    def __init__(self):
        super().__init__()
        self.libs_from_factory: Dict[str, NativeVersionStore] = {}
        self.libs_names_from_arctic: List[str] = []

    def __str__(self):
        return f"{type(self).__name__}[{self.arctic_uri}]"

    def create_arctic(self, **extras):
        """Similar to `Arctic(self.arctic_uri)` but also keeps track of the libraries created"""
        out = Arctic(self.arctic_uri, **extras)
        out._created_lib_names = self.libs_names_from_arctic
        return out

    @abstractmethod
    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        """Creates a new storage config instance.
        If ``lib_name`` is the same as a previous call on this instance, then that storage should be reused."""

    @staticmethod
    def _factory_impl(
        libs_from_factory, create_cfg, default_name, *, name: str = None, reuse_name=False, lmdb_config=None, **kwargs
    ) -> NativeVersionStore:
        # LmdbStorageFixture overrides the caller to intercept lmdb_config. It's unused in other storages.
        name = name or default_name
        if name == "_unique_":
            name = name + str(len(libs_from_factory))
        assert (name not in libs_from_factory) or reuse_name, f"{name} is already in use"
        cfg = create_cfg(name)
        lib = cfg.env_by_id[Defaults.ENV].lib_by_path[name]
        # Use symbol list by default (can still be overridden by kwargs)
        lib.version.symbol_list = True
        apply_lib_cfg(lib, kwargs)
        out = ArcticMemoryConfig(cfg, Defaults.ENV)[name]
        suffix = 0
        while f"{name}{suffix or ''}" in libs_from_factory:
            suffix += 1
        libs_from_factory[f"{name}{suffix or ''}"] = out
        return out

    def create_version_store_factory(self, default_lib_name: str, **defaults) -> Callable[..., NativeVersionStore]:
        """Returns a factory function that produces ``NativeVersionStore``s.

        The factory can take the following arguments (all optional):
        * ``name``: override the name of the library.
            This can be a magical value "_unique_" which will create libs with random unique names.
        * ``reuse_name``: allow an existing library to be recreated, potentially with different config.
            The storage of all libraries of the same name are shared.
        * Any attribute in ``WriteOptions`` or ``VersionStoreConfig``: these must be passed as keyword arguments
        * ``lmdb_config``: populates the `LmdbConfig` Protobuf that creates the `Library` in C++.
            On Windows, it can be used to override the `map_size`.

        This function itself accepts:
        * ``default_lib_name``: (Required) The library name the factory will use if ``name`` (above) is not passed
        * Any other keyword arguments: becomes default config options for the factory.
            Can be overridden by passing different value(s) to the returned factory!

        The base implementation should work for most Storage types and a typical subtype just needs to implement
        ``create_test_cfg``"""
        return functools.partial(
            self._factory_impl, self.libs_from_factory, self.create_test_cfg, default_lib_name, **defaults
        )

    def slow_cleanup(self, failure_consequence=""):
        for lib in self.libs_from_factory.values():
            with handle_cleanup_exception(self, lib, consequence=failure_consequence):
                lib.version_store.clear()
        self.libs_from_factory.clear()

        arctic = self.create_arctic()
        for lib in self.libs_names_from_arctic[:]:
            with handle_cleanup_exception(self, lib, consequence=failure_consequence):
                arctic.delete_library(lib)

    def set_permission(self, *, read: bool, write: bool):
        """Makes the connection to the storage have the given permissions. If unsupported, call ``pytest.skip()``.
        See ``set_enforcing_permissions`` below."""
        raise NotImplementedError(type(self).__name__ + " does not implement set_permission()")

    def iter_underlying_object_names(self):
        raise NotImplementedError(type(self).__name__ + " does not implement iter_underlying_object_names()")

    def copy_underlying_objects_to(self, destination: "StorageFixture"):
        """Where implemented, copies all objects stored in the backing Storage to the destination.
        Unless otherwise documented, the ``destination`` argument should be the same type as ``self``."""
        raise NotImplementedError(type(self).__name__ + " does not implement copy_underlying_objects_to()")

    @classmethod
    def replace_uri_field(cls, uri: str, field: ArcticUriFields, replacement: str, start=2, end=2):
        """start & end are these regex groups: 1=field name, 2=field value, 3=optional field separator"""
        regex = cls._FIELD_REGEX[field]
        match = regex.search(uri)
        assert match, f"{uri} does not have {field}"
        return f"{uri[:match.start(start)]}{replacement}{uri[match.end(end):]}"


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
    def create_fixture(self) -> StorageFixture:
        ...
