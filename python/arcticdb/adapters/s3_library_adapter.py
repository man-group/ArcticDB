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
from arcticdb.version_store.helper import add_s3_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter, set_library_options
from arcticdb_ext.storage import Library, StorageOverride, S3CredentialsOverride
from arcticdb.encoding_version import EncodingVersion
from collections import namedtuple
from dataclasses import dataclass, fields
from distutils.util import strtobool

PARSED_QUERY = namedtuple("PARSED_QUERY", ["region"])
USE_AWS_CRED_PROVIDERS_TOKEN = "_RBAC_"


@dataclass
class ParsedQuery:
    port: Optional[int] = None

    region: Optional[str] = None
    use_virtual_addressing: bool = False

    access: Optional[str] = None
    secret: Optional[str] = None
    aws_auth: Optional[bool] = False

    path_prefix: Optional[str] = None

    force_uri_lib_config: Optional[bool] = False


class S3LibraryAdapter(ArcticLibraryAdapter):
    REGEX = r"s3(s)?://(?P<endpoint>.*):(?P<bucket>[-_a-zA-Z0-9.]+)(?P<query>\?.*)?"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("s3://") or uri.startswith("s3s://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        match = re.match(self.REGEX, uri)
        match_groups = match.groupdict()

        self._endpoint = match_groups["endpoint"]
        self._bucket = match_groups["bucket"]

        self._query_params: ParsedQuery = self._parse_query(match["query"])

        if self._query_params.port:
            self._endpoint += f":{self._query_params.port}"

        self._https = uri.startswith("s3s")
        self._encoding_version = encoding_version

        if "amazonaws" in self._endpoint:
            self._configure_aws()

        super().__init__(uri, self._encoding_version)

    def __repr__(self):
        return "S3(endpoint=%s, bucket=%s)" % (self._endpoint, self._bucket)

    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()
        _name = self._query_params.access if not self._query_params.aws_auth else USE_AWS_CRED_PROVIDERS_TOKEN
        _key = self._query_params.secret if not self._query_params.aws_auth else USE_AWS_CRED_PROVIDERS_TOKEN
        with_prefix = (
            f"{self._query_params.path_prefix}/{self.CONFIG_LIBRARY_NAME}" if self._query_params.path_prefix else False
        )

        add_s3_library_to_env(
            env_cfg,
            lib_name=self.CONFIG_LIBRARY_NAME,
            credential_name=_name,
            credential_key=_key,
            bucket_name=self._bucket,
            endpoint=self._endpoint,
            env_name=_DEFAULT_ENV,
            is_https=self._https,
            with_prefix=with_prefix,
            region=self._query_params.region,
            use_virtual_addressing=self._query_params.use_virtual_addressing,
        )

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, self.CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )

        return lib._library

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
                    "Invalid S3 URI. "
                    f"Invalid query parameter '{key}' passed in. "
                    "Value query parameters: "
                    f"{list(field_dict.keys())}"
                )

            if field_dict[key].type == bool:
                parsed_query[key] = bool(strtobool(parsed_query[key][0]))

            if field_dict[key].type == Optional[bool] and field_dict[key] is not None:
                parsed_query[key] = bool(strtobool(parsed_query[key][0]))

        if parsed_query.get("path_prefix"):
            parsed_query["path_prefix"] = parsed_query["path_prefix"].strip("/")

        _kwargs = {k: v for k, v in parsed_query.items()}
        return ParsedQuery(**_kwargs)

    def get_storage_override(self) -> StorageOverride:
        storage_override = StorageOverride()
        if self._query_params.force_uri_lib_config:
            s3_override = S3CredentialsOverride()
            if self._query_params.access:
                s3_override.credential_name = self._query_params.access
            if self._query_params.secret:
                s3_override.credential_key = self._query_params.secret
            if self._query_params.region:
                s3_override.region = self._query_params.region
            if self._endpoint:
                s3_override.endpoint = self._endpoint
            if self._bucket:
                s3_override.bucket_name = self._bucket
            storage_override = StorageOverride()
            storage_override.set_override(s3_override)

        return storage_override

    def create_library(self, name, library_options: LibraryOptions):
        env_cfg = EnvironmentConfigsMap()

        _name = self._query_params.access if not self._query_params.aws_auth else USE_AWS_CRED_PROVIDERS_TOKEN
        _key = self._query_params.secret if not self._query_params.aws_auth else USE_AWS_CRED_PROVIDERS_TOKEN

        if self._query_params.path_prefix:
            # add time to prefix - so that the s3 root folder is unique and we can delete and recreate fast
            with_prefix = f"{self._query_params.path_prefix}/{name}{time.time() * 1e9:.0f}"
        else:
            with_prefix = True

        add_s3_library_to_env(
            env_cfg,
            lib_name=name,
            credential_name=_name,
            credential_key=_key,
            bucket_name=self._bucket,
            endpoint=self._endpoint,
            is_https=self._https,
            with_prefix=with_prefix,
            env_name=_DEFAULT_ENV,
            region=self._query_params.region,
            use_virtual_addressing=self._query_params.use_virtual_addressing,
        )

        set_library_options(env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options)

        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, name, encoding_version=self._encoding_version
        )

        return lib

    def _configure_aws(self):
        if not self._query_params.region:
            match = re.match(r"s3\.(?P<region>[a-z0-9-]+)\.amazonaws.*", self._endpoint)
            if match:
                match_groups = match.groupdict()
                self._query_params.region = match_groups["region"]
                self._query_params.use_virtual_addressing = True

    @property
    def path_prefix(self):
        return self._query_params.path_prefix
