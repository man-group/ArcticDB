"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import re
import os

from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticdb.version_store.helper import add_lmdb_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb_ext.storage import Library


class LMDBLibraryAdapter(ArcticLibraryAdapter):
    """
    Use local LMDB library for storage.

    Supports any URI that begins with `lmdb://` - for example, `lmdb:///tmp/lmdb_db`.
    """

    REGEX = r"lmdb://(?P<path>.*)"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("lmdb://")

    def __init__(self, uri: str, *args, **kwargs):
        match = re.match(self.REGEX, uri)
        match_groups = match.groupdict()

        self._path = os.path.abspath(match_groups["path"])
        os.makedirs(self._path, exist_ok=True)

        super().__init__(uri)

    def __repr__(self):
        return "LMDB(path=%s)" % self._path

    @property
    def config_library(self) -> Library:
        env_cfg = EnvironmentConfigsMap()

        add_lmdb_library_to_env(env_cfg, lib_name=self.CONFIG_LIBRARY_NAME, env_name=_DEFAULT_ENV, db_dir=self._path)

        lib = NativeVersionStore.create_store_from_config(env_cfg, _DEFAULT_ENV, self.CONFIG_LIBRARY_NAME)._library

        return lib

    def create_library_config(self, name, library_options: LibraryOptions) -> LibraryConfig:
        env_cfg = EnvironmentConfigsMap()

        add_lmdb_library_to_env(env_cfg, lib_name=name, env_name=_DEFAULT_ENV, db_dir=self._path)

        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options)

        lib = NativeVersionStore.create_store_from_config(env_cfg, _DEFAULT_ENV, name)

        return lib._lib_cfg

    def initialize_library(self, name: str, config: LibraryConfig):
        pass
