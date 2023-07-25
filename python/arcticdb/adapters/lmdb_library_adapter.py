"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import re
import os
import shutil
from arcticdb.log import storage as log

from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticc.pb2.lmdb_storage_pb2 import Config as LmdbConfig
from arcticdb.version_store.helper import add_lmdb_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb_ext.storage import StorageOverride
from arcticdb.encoding_version import EncodingVersion


def _rmtree_errorhandler(func, path, exc_info):
    log.warn("Error removing LMDB tree at path=[{}]", path, exc_info=exc_info)


class LMDBLibraryAdapter(ArcticLibraryAdapter):
    """
    Use local LMDB library for storage.

    Supports any URI that begins with `lmdb://` - for example, `lmdb:///tmp/lmdb_db`.
    """

    REGEX = r"lmdb://(?P<path>.*)"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("lmdb://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        match = re.match(self.REGEX, uri)
        match_groups = match.groupdict()

        self._path = os.path.abspath(match_groups["path"])
        self._encoding_version = encoding_version
        os.makedirs(self._path, exist_ok=True)

        super().__init__(uri, self._encoding_version)

    def __repr__(self):
        return "LMDB(path=%s)" % self._path

    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()

        add_lmdb_library_to_env(env_cfg, lib_name=self.CONFIG_LIBRARY_NAME, env_name=_DEFAULT_ENV, db_dir=self._path)

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, self.CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )

        return lib._library

    def create_library(self, name, library_options: LibraryOptions):
        env_cfg = EnvironmentConfigsMap()

        add_lmdb_library_to_env(env_cfg, lib_name=name, env_name=_DEFAULT_ENV, db_dir=self._path)

        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options)

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, name, encoding_version=self._encoding_version
        )

        return lib

    def cleanup_library(self, library_name: str, library_config: LibraryConfig):
        for k, v in library_config.storage_by_id.items():
            lmdb_config = LmdbConfig()
            v.config.Unpack(lmdb_config)
            shutil.rmtree(os.path.join(lmdb_config.path, library_name), onerror=_rmtree_errorhandler)

    def get_storage_override(self) -> StorageOverride:
        return StorageOverride()
