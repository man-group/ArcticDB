"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import LibraryConfig
from arcticdb_ext.storage import Library
from abc import ABC, abstractmethod


def set_library_options(lib_desc: "LibraryConfig", options: LibraryOptions):
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


class ArcticLibraryAdapter(ABC):
    CONFIG_LIBRARY_NAME = "_arctic_cfg"  # TODO: Should come from native module

    @abstractmethod
    def __init__(self, uri: str):
        pass

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

    @abstractmethod
    def create_library_config(self, name: str, library_options: LibraryOptions) -> LibraryConfig:
        raise NotImplementedError

    @abstractmethod
    def initialize_library(self, name: str, config: LibraryConfig):
        raise NotImplementedError

    def delete_library(self, library: Library, library_config: LibraryConfig):
        return library._nvs.version_store.clear()
