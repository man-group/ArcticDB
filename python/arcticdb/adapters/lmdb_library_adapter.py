"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import re
import os
import shutil
from typing import Optional
from dataclasses import dataclass, fields

from arcticdb.log import storage as log

from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticdb.version_store.helper import add_lmdb_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb_ext.storage import StorageOverride, LmdbOverride, CONFIG_LIBRARY_NAME
from arcticdb.encoding_version import EncodingVersion
from arcticdb.exceptions import ArcticDbNotYetImplemented, LmdbOptionsError


def _rm_errorhandler(func, path, exc_info):
    log.warn("Error removing LMDB file at path=[{}] error=[{}]", path, exc_info)


@dataclass
class ParsedQuery:
    map_size: Optional[int] = None


SUFFIX_TO_SCALE = {"KB": int(1e3), "MB": int(1e6), "GB": int(1e9), "TB": int(1e12)}


MAP_SIZE_OPTION_ERROR_TEMPLATE = (
    "Incorrect format for map_size option in LMDB connection string. Correct format is "
    "a positive integer followed by one of (KB, MB, GB, TB), eg '200MB' or '10GB'. But option "
    "map_size was [{}]."
)


def convert_size_str_to_bytes(size: str) -> int:
    """
    1MB -> int(1e6)
    2GB -> int(2e9)
    3TB -> int(3e12)
    1mb -> raise
    1Gb -> raise
    """
    suffix = size[-2:]
    if suffix not in SUFFIX_TO_SCALE:
        raise LmdbOptionsError(MAP_SIZE_OPTION_ERROR_TEMPLATE.format(size))
    scale = SUFFIX_TO_SCALE[suffix]
    mantissa_str = size[:-2]
    try:
        mantissa = int(mantissa_str)
        if mantissa < 1:
            raise ValueError()
    except ValueError:
        raise LmdbOptionsError(MAP_SIZE_OPTION_ERROR_TEMPLATE.format(size))
    return mantissa * scale


def parse_query(query: str) -> ParsedQuery:
    if query and query.startswith("?"):
        query = query.strip("?")
    elif not query:
        return ParsedQuery()

    parsed_query = re.split("[;&]", query)
    parsed_query = {t.split("=", 1)[0]: t.split("=", 1)[1] for t in parsed_query}

    result = dict()
    field_dict = {field.name: field for field in fields(ParsedQuery)}
    for key in parsed_query.keys():
        if key not in field_dict.keys():
            raise LmdbOptionsError(
                "Invalid LMDB URI. "
                f"Invalid query parameter '{key}' passed in. "
                "Value query parameters: "
                f"{list(field_dict.keys())}"
            )

        if key == "map_size":
            value = parsed_query[key]
            map_size_bytes = convert_size_str_to_bytes(value)
            result["map_size"] = map_size_bytes
        else:
            raise ArcticDbNotYetImplemented(
                "Support for option {key} not implemented correctly. This is a bug in "
                "ArcticDB. Please report on ArcticDB Github."
            )

    return ParsedQuery(**result)


class LMDBLibraryAdapter(ArcticLibraryAdapter):
    """
    Use local LMDB library for storage.

    Supports any URI that begins with `lmdb://` - for example, `lmdb:///tmp/lmdb_db`.
    """

    REGEX = r"lmdb://(?P<path>[^?]*)(?P<query>\?.*)?"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("lmdb://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        match = re.match(self.REGEX, uri)
        match_groups = match.groupdict()

        self._path = os.path.abspath(match_groups["path"])
        self._encoding_version = encoding_version

        self._query_params: ParsedQuery = parse_query(match["query"])

        os.makedirs(self._path, exist_ok=True)

        super().__init__(uri, self._encoding_version)

    def __repr__(self):
        return "LMDB(path=%s)" % self._path

    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()

        # 128 MiB - needs to be reasonably small not to waste disk on Windows
        config_library_config = {"map_size": 128 * (1 << 20)}
        add_lmdb_library_to_env(
            env_cfg,
            lib_name=CONFIG_LIBRARY_NAME,
            env_name=_DEFAULT_ENV,
            db_dir=self._path,
            lmdb_config=config_library_config,
        )

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )

        return lib._library

    def get_library_config(self, name, library_options: LibraryOptions):
        env_cfg = EnvironmentConfigsMap()

        lmdb_config = {}
        if self._query_params.map_size:
            lmdb_config["map_size"] = self._query_params.map_size

        add_lmdb_library_to_env(
            env_cfg, lib_name=name, env_name=_DEFAULT_ENV, db_dir=self._path, lmdb_config=lmdb_config
        )

        library_options.encoding_version = (
            library_options.encoding_version if library_options.encoding_version is not None else self._encoding_version
        )
        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options)

        return NativeVersionStore.create_library_config(
            env_cfg, _DEFAULT_ENV, name, encoding_version=library_options.encoding_version
        )

    def cleanup_library(self, library_name: str):
        lmdb_files_removed = True
        for file in ("lock.mdb", "data.mdb"):
            path = os.path.join(self._path, library_name, file)
            try:
                os.remove(path)
            except Exception as e:
                lmdb_files_removed = False
                _rm_errorhandler(None, path, e)
        dir_path = os.path.join(self._path, library_name)
        if os.path.exists(dir_path):
            if os.listdir(dir_path):
                log.warn(
                    "Skipping deletion of directory holding LMDB library during library deletion as it contains "
                    f"files unrelated to LMDB. LMDB files {'have' if lmdb_files_removed else 'have not'} been "
                    f"removed. directory=[{dir_path}]"
                )
            else:
                shutil.rmtree(os.path.join(self._path, library_name), onerror=_rm_errorhandler)

    def get_storage_override(self) -> StorageOverride:
        lmdb_override = LmdbOverride()
        lmdb_override.path = self._path
        if self._query_params.map_size:
            lmdb_override.map_size = self._query_params.map_size

        storage_override = StorageOverride()
        storage_override.set_lmdb_override(lmdb_override)
        return storage_override

    def get_masking_override(self) -> StorageOverride:
        lmdb_override = LmdbOverride()
        storage_override = StorageOverride()
        storage_override.set_lmdb_override(lmdb_override)
        return storage_override
