import re

from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter
from arcticdb.config import _DEFAULT_ENV
from arcticdb.encoding_version import EncodingVersion
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.version_store.helper import add_file_library_to_env
from arcticdb_ext.storage import Library, CONFIG_LIBRARY_NAME


class FileLibraryAdapter(ArcticLibraryAdapter):
    REGEX = r"file://(?P<dir_path>[^?]*)(?P<query>\?.*)?"

    def __init__(self, uri: str, encoding_version: EncodingVersion):
        match = re.match(self.REGEX, uri)
        if match is None:
            raise ValueError(f"uri {uri} does not match FileLibraryAdapter regex {self.REGEX}")

        self._dir_path = match.group("dir_path")
        self._query = match.group("query")
        self._encoding_version = encoding_version

    def __repr__(self):
        return f"File(dir_path={self._dir_path}, query={self._query})"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("file://")

    @property
    def config_library(self) -> Library:
        env_cfg = EnvironmentConfigsMap()

        file_config = {"dir_path": self._dir_path}
        add_file_library_to_env(
            env_cfg,
            lib_name=CONFIG_LIBRARY_NAME,
            env_name=_DEFAULT_ENV,
            file_config=file_config,
        )

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )

        return lib._library

    def add_library_to_env(self, env_cfg: EnvironmentConfigsMap, name: str) -> None:
        file_config = {"dir_path": self._dir_path}
        add_file_library_to_env(env_cfg, lib_name=name, env_name=_DEFAULT_ENV, file_config=file_config)
