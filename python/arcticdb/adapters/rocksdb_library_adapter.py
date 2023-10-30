"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import re
import os

# from dataclasses import dataclass


from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticdb.version_store.helper import add_rocksdb_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb.encoding_version import EncodingVersion
from arcticdb_ext.storage import CONFIG_LIBRARY_NAME


class RocksDBLibraryAdapter(ArcticLibraryAdapter):
    """
    Connect to a RocksDB backend.

    Only supports the URI ``"rocksdb://"``. TODO: Complete this
    """

    REGEX = r"rocksdb://(?P<path>[^?]*)$"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("rocksdb://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        match = re.match(self.REGEX, uri)
        match_groups = match.groupdict()

        self._path = os.path.abspath(match_groups["path"])
        self._encoding_version = encoding_version

        os.makedirs(self._path, exist_ok=True)

        super().__init__(uri, self._encoding_version)

    def __repr__(self):
        return "ROCKSDB()"

    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()

        add_rocksdb_library_to_env(env_cfg, lib_name=CONFIG_LIBRARY_NAME, env_name=_DEFAULT_ENV, db_dir=self._path)

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )

        return lib._library

    def get_library_config(self, name, library_options: LibraryOptions):
        env_cfg = EnvironmentConfigsMap()

        add_rocksdb_library_to_env(env_cfg, lib_name=name, env_name=_DEFAULT_ENV, db_dir=self._path)

        library_options.encoding_version = (
            library_options.encoding_version if library_options.encoding_version is not None else self._encoding_version
        )
        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options)

        return NativeVersionStore.create_library_config(
            env_cfg, _DEFAULT_ENV, name, encoding_version=library_options.encoding_version
        )

    # TODO: def cleanup_library should do something similar to LMDB
    # See PR: 918
