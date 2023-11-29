"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import re
import time
from typing import Optional
import platform

from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticdb.version_store.helper import add_azure_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb_ext.storage import StorageOverride, AzureOverride, CONFIG_LIBRARY_NAME
from arcticdb.encoding_version import EncodingVersion
from collections import namedtuple
from dataclasses import dataclass, fields

PARSED_QUERY = namedtuple("PARSED_QUERY", ["region"])


@dataclass
class ParsedQuery:
    Path_prefix: Optional[str] = None
    CA_cert_path: str = ""
    Container: Optional[str] = None


class AzureLibraryAdapter(ArcticLibraryAdapter):
    REGEX = r"azure://(?P<query>.*)"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("azure://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        self._uri = uri
        match = re.match(self.REGEX, uri)

        additional_options = self._parse_query(match["query"])
        self._query_params: ParsedQuery = ParsedQuery(**additional_options)

        option_regex = r"(?P<option>[^=]+)=(?P<value>.*)"
        self._endpoint = ";".join(  # azure c++ sdk doesn't accept any non-standard string
            [
                f"{match[0]}={match[1]}"
                for option_str in uri[len("azure://") :].split(";")
                for match in re.findall(option_regex, option_str)
                if match[0] not in additional_options
            ]
        )
        self._container = self._query_params.Container
        if platform.system() == "Windows" and self._query_params.CA_cert_path:
            raise ValueError(f"CA_cert_path cannot be set on Windows platform")
        self._ca_cert_path = self._query_params.CA_cert_path
        self._encoding_version = encoding_version

        super().__init__(uri, self._encoding_version)

    def __repr__(self):
        return "azure(endpoint=%s, container=%s)" % (self._endpoint, self._container)

    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()
        with_prefix = (
            f"{self._query_params.Path_prefix}/{CONFIG_LIBRARY_NAME}" if self._query_params.Path_prefix else False
        )

        add_azure_library_to_env(
            cfg=env_cfg,
            lib_name=CONFIG_LIBRARY_NAME,
            env_name=_DEFAULT_ENV,
            container_name=self._container,
            endpoint=self._endpoint,
            with_prefix=with_prefix,
            ca_cert_path=self._ca_cert_path,
        )

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )

        return lib._library

    def _parse_query(self, query: str) -> ParsedQuery:
        if not query:
            raise ValueError(f"Invalid Azure URI. Missing query parameter")

        parsed_query = re.split("[;]", query)
        parsed_query = {t.split("=", 1)[0]: t.split("=", 1)[1] for t in parsed_query}

        field_dict = {field.name: field for field in fields(ParsedQuery)}

        if parsed_query.get("Path_prefix"):
            parsed_query["Path_prefix"] = parsed_query["Path_prefix"].strip("/")

        _kwargs = {k: v for k, v in parsed_query.items() if k in field_dict.keys()}
        return _kwargs

    def get_storage_override(self) -> AzureOverride:
        azure_override = AzureOverride()
        if self._container:
            azure_override.container_name = self._container
        if self._endpoint:
            azure_override.endpoint = self._endpoint
        if self._ca_cert_path:
            azure_override.ca_cert_path = self._ca_cert_path

        storage_override = StorageOverride()
        storage_override.set_azure_override(azure_override)

        return storage_override

    def get_masking_override(self) -> StorageOverride:
        storage_override = StorageOverride()
        azure_override = AzureOverride()
        storage_override.set_azure_override(azure_override)
        return storage_override

    def get_library_config(self, name, library_options: LibraryOptions):
        env_cfg = EnvironmentConfigsMap()

        if self._query_params.Path_prefix:
            # add time to prefix - so that the azure root folder is unique and we can delete and recreate fast
            with_prefix = f"{self._query_params.Path_prefix}/{name}{time.time() * 1e9:.0f}"
        else:
            with_prefix = True

        add_azure_library_to_env(
            cfg=env_cfg,
            lib_name=name,
            env_name=_DEFAULT_ENV,
            container_name=self._container,
            endpoint=self._endpoint,
            with_prefix=with_prefix,
            ca_cert_path=self._ca_cert_path,
        )

        library_options.encoding_version = (
            library_options.encoding_version if library_options.encoding_version is not None else self._encoding_version
        )
        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options)

        return NativeVersionStore.create_library_config(
            env_cfg, _DEFAULT_ENV, name, encoding_version=library_options.encoding_version
        )

    @property
    def path_prefix(self):
        return self._query_params.Path_prefix
