"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from abc import ABC, abstractmethod
from typing import Iterable, List

from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig, LibraryDescriptor
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.options import DEFAULT_ENCODING_VERSION, LibraryOptions, EnterpriseLibraryOptions
from arcticc.pb2.storage_pb2 import LibraryConfig
from arcticdb_ext.storage import Library, StorageOverride, CONFIG_LIBRARY_NAME
from arcticdb.encoding_version import EncodingVersion


def set_library_options(lib_desc: "LibraryConfig", options: LibraryOptions,
                        enterprise_library_options: EnterpriseLibraryOptions):
    write_options = lib_desc.version.write_options

    write_options.dynamic_strings = True
    write_options.recursive_normalizers = True
    write_options.use_tombstones = True
    write_options.fast_tombstone_all = True
    lib_desc.version.symbol_list = True

    write_options.prune_previous_version = False
    write_options.pickle_on_failure = False
    write_options.snapshot_dedup = False
    write_options.delayed_deletes = False

    write_options.dynamic_schema = options.dynamic_schema
    write_options.de_duplication = options.dedup
    write_options.segment_row_size = options.rows_per_segment
    write_options.column_group_size = options.columns_per_segment

    lib_desc.version.encoding_version = (
        options.encoding_version if options.encoding_version is not None else DEFAULT_ENCODING_VERSION
    )

    write_options.sync_passive.enabled = enterprise_library_options.replication
    write_options.delayed_deletes = enterprise_library_options.background_deletion


class ArcticLibraryAdapter(ABC):
    @abstractmethod
    def __init__(self, uri: str, encoding_version: EncodingVersion):
        ...

    @abstractmethod
    def __repr__(self):
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def supports_uri(uri: str) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def config_library(self) -> Library:
        raise NotImplementedError

    def native_config(self):
        return None

    def get_library_config(self, name: str, library_options: LibraryOptions,
                           enterprise_library_options: EnterpriseLibraryOptions):
        env_cfg = EnvironmentConfigsMap()

        self.add_library_to_env(env_cfg, name)

        library_options.encoding_version = (
            library_options.encoding_version if library_options.encoding_version is not None else self._encoding_version
        )
        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options,
                            enterprise_library_options)

        return NativeVersionStore.create_library_config(
            env_cfg, _DEFAULT_ENV, name, encoding_version=library_options.encoding_version
        )
    
    @abstractmethod
    def add_library_to_env(self, env_cfg: EnvironmentConfigsMap, name: str):
        raise NotImplementedError

    def get_storage_override(self) -> StorageOverride:
        return StorageOverride()

    def get_masking_override(self) -> StorageOverride:
        """Override that clears any storage config that should not be persisted."""
        return StorageOverride()

    def get_name_for_library_manager(self, user_facing_name: str) -> str:
        """Can override to translate user-supplied library names to a format more stuiable to the Storage."""
        return user_facing_name

    def library_manager_names_to_user_facing(self, names: Iterable[str]) -> List[str]:
        """The inverse of `get_name_for_library_manager`."""
        return names
