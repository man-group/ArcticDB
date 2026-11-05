"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import functools
import os
import shutil
from tempfile import mkdtemp
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional
from itertools import chain

from .utils import safer_rmtree
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
from arcticdb import Arctic
from arcticdb.config import Defaults
from arcticdb.storage_fixtures.api import StorageFixture
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store.helper import add_file_library_to_env

_DEFAULT_CONFIG = MappingProxyType({})


class FileBackedStorageFixture(StorageFixture):
    def __init__(self, db_dir: Optional[str] = None):
        super().__init__()
        self.db_dir = str(db_dir) if db_dir else mkdtemp(suffix="FileBackedStorageFixtureFactory")
        self.arctic_uri = "lmdb://" + self.db_dir
        self.libs_instances_from_arctic: List[NativeVersionStore] = []
        self.arctic_instances: List[Arctic] = []

    def create_arctic(self, **extras):
        out = super().create_arctic(**extras)
        # This is a hack to allow the tests to access the libs_instances_from_arctic list
        out._accessed_libs = self.libs_instances_from_arctic
        self.arctic_instances.append(out)
        return out

    def __exit__(self, exc_type, exc_value, traceback):
        for result in chain(self.libs_from_factory.values(), self.libs_instances_from_arctic):
            #  pytest holds a member variable `cached_result` equal to `result` above which keeps the storage alive and
            #  locked. See https://github.com/pytest-dev/pytest/issues/5642 . So we need to decref the C++ objects
            #  keeping the LMDB env open before they will release the lock and allow Windows to delete the LMDB files.
            result.version_store = None
            result._library = None
        for a in self.arctic_instances:
            a._library_manager = None
            pass

        safer_rmtree(self, self.db_dir)

    def create_test_cfg(
        self, lib_name: str, *, file_config: Mapping[str, Any] = _DEFAULT_CONFIG
    ) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        composed_config = {"dir_path": self.db_dir, **file_config}
        add_file_library_to_env(cfg, lib_name=lib_name, env_name=Defaults.ENV, file_config=composed_config)
        return cfg

    def create_version_store_factory(self, default_lib_name: str, **defaults):
        default_file_config = MappingProxyType[str, Any](defaults.pop("file_config", _DEFAULT_CONFIG))

        def create_version_store(
            col_per_group: Optional[int] = None,  # Legacy option names
            row_per_segment: Optional[int] = None,
            file_config: Mapping[str, Any] = default_file_config,
            **kwargs,
        ) -> NativeVersionStore:
            if col_per_group is not None and "column_group_size" not in kwargs:
                kwargs["column_group_size"] = col_per_group
            if row_per_segment is not None and "segment_row_size" not in kwargs:
                kwargs["segment_row_size"] = row_per_segment
            cfg_factory = self.create_test_cfg
            if file_config:
                cfg_factory = functools.partial(cfg_factory, file_config=file_config)
            combined_args = {**defaults, **kwargs} if defaults else kwargs
            return self._factory_impl(self.libs_from_factory, cfg_factory, default_lib_name, **combined_args)

        return create_version_store

    def copy_underlying_objects_to(self, destination: "FileBackedStorageFixture"):
        os.rmdir(destination.db_dir)  # Must be empty
        shutil.copytree(self.db_dir, destination.db_dir)
