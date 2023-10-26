"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap

from .api import *
from arcticdb.version_store.helper import add_memory_library_to_env
from arcticdb.adapters.in_memory_library_adapter import InMemoryLibraryAdapter


class InMemoryStorageFixture(StorageFixture):
    arctic_uri = InMemoryLibraryAdapter.REGEX

    def __exit__(self, exc_type, exc_value, traceback):
        self.libs_from_factory.clear()

    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        add_memory_library_to_env(cfg, lib_name=lib_name, env_name=Defaults.ENV)
        return cfg
