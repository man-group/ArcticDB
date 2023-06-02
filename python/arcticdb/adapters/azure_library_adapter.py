"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import re
import time
from typing import Optional

from arcticdb.options import LibraryOptions
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig
from arcticdb.version_store.helper import add_azure_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb_ext.storage import Library
from collections import namedtuple
from dataclasses import dataclass, fields
from distutils.util import strtobool

PARSED_QUERY = namedtuple("PARSED_QUERY", ["region"])
USE_AWS_CRED_PROVIDERS_TOKEN = "_RBAC_"


@dataclass
class ParsedQuery:
    access: Optional[str] = None
    secret: Optional[str] = None

    path_prefix: Optional[str] = None
    https: bool = False
    connect_to_azurite: bool = False


class AzureLibraryAdapter(ArcticLibraryAdapter):
    REGEX = r"azure://(?P<endpoint>.*):(?P<container>[-_a-zA-Z0-9.]+)(?P<query>\?.*)?"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("azure://")

    def __init__(self, uri: str, *args, **kwargs):
        match = re.match(self.REGEX, uri)
        match_groups = match.groupdict()

        self._endpoint = match_groups["endpoint"]
        self._bucket = match_groups["container"]

        self._query_params: ParsedQuery = self._parse_query(match["query"])

        self._https = self._query_params.https
        self._connect_to_azurite = self._query_params.connect_to_azurite

        super().__init__(uri)

    def __repr__(self):
        return "azure(endpoint=%s, bucket=%s)" % (self._endpoint, self._bucket)

    @property
    def config_library(self) -> Library:
        env_cfg = EnvironmentConfigsMap()
        _name = self._query_params.access
        _key = self._query_params.secret
        with_prefix = (
            f"{self._query_params.path_prefix}/{self.CONFIG_LIBRARY_NAME}" if self._query_params.path_prefix else False
        )

        add_azure_library_to_env(
            cfg=env_cfg,
            lib_name=self.CONFIG_LIBRARY_NAME,
            env_name=_DEFAULT_ENV,
            credential_name=_name, 
            credential_key=_key,
            container_name=self._bucket,
            endpoint=self._endpoint,
            with_prefix=with_prefix,
            is_https=self._https,
            connect_to_azurite=self._connect_to_azurite
        )

        lib = NativeVersionStore.create_store_from_config(env_cfg, _DEFAULT_ENV, self.CONFIG_LIBRARY_NAME)._library

        return lib

    def _parse_query(self, query: str) -> ParsedQuery:
        if query and query.startswith("?"):
            query = query.strip("?")
        elif not query:
            return ParsedQuery(aws_auth=True)

        parsed_query = re.split("[;&]", query)
        parsed_query = {t.split("=", 1)[0]: t.split("=", 1)[1] for t in parsed_query}

        for key in parsed_query.keys():
            field_dict = {field.name: field for field in fields(ParsedQuery)}
            if key not in field_dict.keys():
                raise ValueError(
                    "Invalid Azure URI. "
                    f"Invalid query parameter '{key}' passed in. "
                    f"Value query parameters: "
                    f"{list(field_dict.keys())}"
                )

            if field_dict[key].type == bool:
                parsed_query[key] = bool(strtobool(parsed_query[key][0]))

        if parsed_query.get("path_prefix"):
            parsed_query["path_prefix"] = parsed_query["path_prefix"].strip("/")

        _kwargs = {k: v for k, v in parsed_query.items()}
        return ParsedQuery(**_kwargs)

    def create_library_config(self, name, library_options: LibraryOptions) -> LibraryConfig:
        env_cfg = EnvironmentConfigsMap()

        _name = self._query_params.access
        _key = self._query_params.secret

        if self._query_params.path_prefix:
            # add time to prefix - so that the s3 root folder is unique and we can delete and recreate fast
            with_prefix = f"{self._query_params.path_prefix}/{name}{time.time() * 1e9:.0f}"
        else:
            with_prefix = True

        add_azure_library_to_env(
            cfg=env_cfg,
            lib_name=name,
            env_name=_DEFAULT_ENV,
            credential_name=_name, 
            credential_key=_key,
            container_name=self._bucket,
            endpoint=self._endpoint,
            with_prefix=with_prefix,
            is_https=self._https,
            connect_to_azurite=self._connect_to_azurite
        )

        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options)

        lib = NativeVersionStore.create_store_from_config(env_cfg, _DEFAULT_ENV, name)

        return lib._lib_cfg

    def initialize_library(self, name: str, config: LibraryConfig):
        pass

    @property
    def path_prefix(self):
        return self._query_params.path_prefix
