"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import re
from dataclasses import dataclass


from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticdb.version_store.helper import add_memory_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb.encoding_version import EncodingVersion
from arcticdb.exceptions import ArcticDbNotYetImplemented


@dataclass
class ParsedQuery:
    pass


def parse_query(query: str) -> ParsedQuery:
    if query:
        raise ArcticDbNotYetImplemented("In-memory URIs do not support any query parameters yet")
    return ParsedQuery()


class InMemoryLibraryAdapter(ArcticLibraryAdapter):
    """
    Use in-memory storage.

    Supports any URI that begins with `mem://` - for example, `mem://in_memory_db`.
    """

    # Only query parameter is cache_size at the moment, but this is not even used.
    # Keep the query in the regex just in case we want it.
    REGEX = r"mem://(?P<name>[^?]*)?(?P<query>\?.*)?"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("mem://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        match = re.match(self.REGEX, uri)
        match_groups = match.groupdict()

        self._name = match_groups["name"]
        self._encoding_version = encoding_version

        self._query_params: ParsedQuery = parse_query(match["query"])

        super().__init__(uri, self._encoding_version)  # no point in doing this?? I guess good practise... ?

    def __repr__(self):
        return "MEM(name=%s)" % self._name

    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()

        add_memory_library_to_env(env_cfg, lib_name=self.CONFIG_LIBRARY_NAME, env_name=_DEFAULT_ENV, db_name=self._name)

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, self.CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )

        return lib._library

    def create_library(self, name, library_options: LibraryOptions):
        env_cfg = EnvironmentConfigsMap()

        add_memory_library_to_env(env_cfg, lib_name=name, env_name=_DEFAULT_ENV, db_name=self._name)

        library_options.encoding_version = (
            library_options.encoding_version if library_options.encoding_version is not None else self._encoding_version
        )
        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options)

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, name, encoding_version=library_options.encoding_version
        )

        return lib

    def cleanup_library(self, library_name: str, library_config: LibraryConfig):
        # The in-memory Storage held by the LibraryManager and the Arctic instance will both be deleted for us.
        pass
