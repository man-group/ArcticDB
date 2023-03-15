"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import LibraryConfig
from arcticdb_ext.storage import Library
from abc import ABC, abstractmethod


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

    @abstractmethod
    def delete_library(self, library: Library, library_config: LibraryConfig):
        raise NotImplementedError
