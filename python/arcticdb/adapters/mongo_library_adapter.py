"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticdb.version_store.helper import add_mongo_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb_ext.storage import CONFIG_LIBRARY_NAME
from arcticdb.encoding_version import EncodingVersion
from arcticdb.exceptions import UserInputException
import re

try:
    from pymongo.uri_parser import parse_uri

    _HAVE_PYMONGO = True
except ImportError:
    _HAVE_PYMONGO = False


class MongoLibraryAdapter(ArcticLibraryAdapter):
    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("mongodb://")  # mongo+srv:// support?

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        try:
            if _HAVE_PYMONGO:
                parameters = parse_uri(
                    uri
                )  # also checks pymongo uri syntax, throw exception as early as possible if syntax is incorrect
                self._endpoint = f"{parameters['nodelist'][0][0]}:{parameters['nodelist'][0][1]}"
            else:
                match = re.search(r"\/\/(?P<endpoint>[^\/]*)", uri)
                self._endpoint = match["endpoint"]
        except Exception as e:
            raise UserInputException(
                f"Invalid connection string format. {e} Correct format: mongodb://[HOST]/[DATABASE][?OPTIONS]"
            )
        self._uri = uri
        self._encoding_version = encoding_version

        super().__init__(uri, self._encoding_version)

    def __repr__(self):
        return f"mongodb(endpoint={self._endpoint})"

    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()

        add_mongo_library_to_env(cfg=env_cfg, lib_name=CONFIG_LIBRARY_NAME, env_name=_DEFAULT_ENV, uri=self._uri)

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )

        return lib._library

    def get_library_config(self, name, library_options: LibraryOptions):
        env_cfg = EnvironmentConfigsMap()

        add_mongo_library_to_env(cfg=env_cfg, lib_name=name, env_name=_DEFAULT_ENV, uri=self._uri)
        library_options.encoding_version = (
            library_options.encoding_version if library_options.encoding_version is not None else self._encoding_version
        )
        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options)

        return NativeVersionStore.create_library_config(
            env_cfg, _DEFAULT_ENV, name, encoding_version=library_options.encoding_version
        )
