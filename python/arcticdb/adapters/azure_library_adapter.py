"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import platform
import re
import ssl
import time
from collections import namedtuple
from dataclasses import dataclass, fields
from typing import Optional, Tuple

from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticdb.config import _DEFAULT_ENV
from arcticdb.encoding_version import EncodingVersion
from arcticdb.exceptions import ArcticException
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.version_store.helper import add_azure_library_to_env
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb_ext.storage import StorageOverride, AzureOverride, CONFIG_LIBRARY_NAME

PARSED_QUERY = namedtuple("PARSED_QUERY", ["region"])


@dataclass
class ParsedQuery:
    Path_prefix: Optional[str] = None
    # winhttp is used as Azure backend support on Windows by default; winhttp itself maintains ca cert.
    # The options should be left empty else libcurl will be used on Windows
    CA_cert_path: Optional[str] = ""  # CURLOPT_CAINFO in curl
    CA_cert_dir: Optional[str] = ""  # CURLOPT_CAPATH in curl
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

        # Extract CA certificate settings directly from the URI to be robust to any quirks
        # in how the query string is built (e.g. duplicated keys).
        # We parse the full URI to get the last occurrence of each parameter (in case of duplicates).
        def _get_param_from_uri(key: str) -> Tuple[str, bool]:
            """Returns (value, found) tuple where found indicates if the key was present in URI."""
            query = uri[len("azure://") :]
            value = ""
            found = False
            # Process segments in order, keeping the last occurrence (handles duplicates correctly)
            for segment in query.split(";"):
                segment = segment.strip()
                if not segment:
                    continue
                # Use partition to handle cases where value might contain '='
                k, sep, v = segment.partition("=")
                if k == key and sep and v:
                    # Keep the last occurrence (overwrites previous values if duplicates exist)
                    # Only consider it found if the value is non-empty
                    value = v
                    found = True
            return value, found

        ca_cert_path_from_uri, ca_cert_path_found = _get_param_from_uri("CA_cert_path")
        ca_cert_dir_from_uri, ca_cert_dir_found = _get_param_from_uri("CA_cert_dir")

        # On Linux, prefer the values explicitly provided in the URI (if found),
        # otherwise fall back to what was parsed into _query_params.
        # Ensure values are always strings (not None)
        if ca_cert_path_found:
            self._ca_cert_path = str(ca_cert_path_from_uri) if ca_cert_path_from_uri else ""
        else:
            self._ca_cert_path = str(self._query_params.CA_cert_path) if self._query_params.CA_cert_path else ""

        if ca_cert_dir_found:
            self._ca_cert_dir = str(ca_cert_dir_from_uri) if ca_cert_dir_from_uri else ""
        else:
            self._ca_cert_dir = str(self._query_params.CA_cert_dir) if self._query_params.CA_cert_dir else ""

        if platform.system() != "Linux" and (self._ca_cert_path or self._ca_cert_dir):
            raise ValueError(
                "You have provided `ca_cert_path` or `ca_cert_dir` in the URI which is only supported on Linux. "
                "Remove the setting in the connection URI and use your operating system defaults."
            )

        if not self._ca_cert_path and not self._ca_cert_dir and platform.system() == "Linux":
            if ssl.get_default_verify_paths().cafile is not None:
                self._ca_cert_path = ssl.get_default_verify_paths().cafile
            if ssl.get_default_verify_paths().capath is not None:
                self._ca_cert_dir = ssl.get_default_verify_paths().capath
        self._encoding_version = encoding_version

        super().__init__(uri, self._encoding_version)

    def __repr__(self):
        censored_endpoint = re.sub(r"AccountKey=.+?;", "AccountKey=...;", self._endpoint)
        return "azure(endpoint=%s, container=%s)" % (censored_endpoint, self._container)

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
            ca_cert_dir=self._ca_cert_dir,
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
        if self._ca_cert_dir:
            azure_override.ca_cert_dir = self._ca_cert_dir

        storage_override = StorageOverride()
        storage_override.set_azure_override(azure_override)

        return storage_override

    def get_masking_override(self) -> StorageOverride:
        storage_override = StorageOverride()
        azure_override = AzureOverride()
        storage_override.set_azure_override(azure_override)
        return storage_override

    def add_library_to_env(self, env_cfg: EnvironmentConfigsMap, name: str):
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
            ca_cert_dir=self._ca_cert_dir,
        )

    @property
    def path_prefix(self):
        return self._query_params.Path_prefix
