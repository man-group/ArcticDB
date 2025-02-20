"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import re
import ssl
from dataclasses import dataclass, fields
import time
import platform
from typing import Optional

from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryDescriptor
from arcticdb.adapters.s3_library_adapter import strtobool, USE_AWS_CRED_PROVIDERS_TOKEN
from arcticdb.encoding_version import EncodingVersion
from arcticdb.version_store import NativeVersionStore
from arcticdb_ext.exceptions import UserInputException
from arcticdb_ext.storage import (
    StorageOverride,
    GCPXMLOverride,
    AWSAuthMethod,
    NativeVariantStorage,
    GCPXMLSettings as NativeGCPXMLSettings,
    CONFIG_LIBRARY_NAME
)

from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter

from arcticdb.version_store.helper import add_gcp_library_to_env


@dataclass
class ParsedQuery:
    port: Optional[int] = None
    access: Optional[str] = None
    secret: Optional[str] = None
    aws_auth: Optional[AWSAuthMethod] = AWSAuthMethod.DISABLED
    path_prefix: Optional[str] = None
    CA_cert_path: Optional[str] = ""  # CURLOPT_CAINFO in curl
    CA_cert_dir: Optional[str] = ""  # CURLOPT_CAPATH in curl
    ssl: Optional[bool] = False


def _parse_query(query: str) -> ParsedQuery:
    if query and query.startswith("?"):
        query = query.strip("?")
    elif not query:
        return ParsedQuery()

    parsed_query = re.split("[;&]", query)
    parsed_query = {t.split("=", 1)[0]: t.split("=", 1)[1] for t in parsed_query}

    field_dict = {field.name: field for field in fields(ParsedQuery)}
    for key in parsed_query.keys():
        if key not in field_dict.keys():
            raise ValueError(
                "Invalid GCPXML URI. "
                f"Invalid query parameter '{key}' passed in. "
                "Value query parameters: "
                f"{list(field_dict.keys())}"
            )

        if field_dict[key].type == bool:
            parsed_query[key] = bool(strtobool(parsed_query[key][0]))

        if field_dict[key].type == Optional[bool] and field_dict[key] is not None:
            parsed_query[key] = bool(strtobool(parsed_query[key][0]))

        if field_dict[key].type == Optional[AWSAuthMethod]:
            value = parsed_query[key]
            if strtobool(value) or value.lower() == "default":
                parsed_query[key] = AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN
            else:
                raise ValueError(f"Invalid setting for awsauth {value} should be absent or have value 'true'")

    if parsed_query.get("path_prefix"):
        parsed_query["path_prefix"] = parsed_query["path_prefix"].strip("/")

    _kwargs = {k: v for k, v in parsed_query.items()}
    return ParsedQuery(**_kwargs)


class GCPXMLLibraryAdapter(ArcticLibraryAdapter):
    REGEX = r"gcpxml(s)?://(?P<endpoint>.*):(?P<bucket>[-_a-zA-Z0-9.]+)(?P<query>\?.*)?"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("gcpxml://") or uri.startswith("gcpxmls://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        match = re.match(self.REGEX, uri)
        match_groups = match.groupdict()

        self._endpoint = match_groups["endpoint"]
        self._bucket = match_groups["bucket"]

        query_params: ParsedQuery = _parse_query(match["query"])

        if query_params.port:
            self._endpoint += f":{query_params.port}"

        if query_params.aws_auth is None:
            self._aws_auth = AWSAuthMethod.DISABLED
        else:
            self._aws_auth = query_params.aws_auth

        if query_params.access:
            if self._aws_auth == AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN:
                raise UserInputException(f"Specified both access and awsauth=true in the GCPXML Arctic URI - only one can be set endpoint={self._endpoint} bucket={self._bucket}")
            self._access = query_params.access
        elif self._aws_auth == AWSAuthMethod.DISABLED:
            raise UserInputException(f"Access token or awsauth=true must be specified in GCPXML Arctic URI endpoint={self._endpoint} bucket={self._bucket}")
        else:
            self._access = USE_AWS_CRED_PROVIDERS_TOKEN

        if query_params.secret:
            if self._aws_auth == AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN:
                raise UserInputException(f"Specified both secret and awsauth=true in the GCPXML Arctic URI - only one can be set endpoint={self._endpoint} bucket={self._bucket}")
            self._secret = query_params.secret
        elif self._aws_auth == AWSAuthMethod.DISABLED:
            raise UserInputException(f"Secret or awsauth=true must be specified in GCPXML Arctic URI endpoint={self._endpoint} bucket={self._bucket}")
        else:
            self._secret = USE_AWS_CRED_PROVIDERS_TOKEN

        self._path_prefix = query_params.path_prefix
        self._https = uri.startswith("gcpxmls")

        if platform.system() != "Linux" and (query_params.CA_cert_path or query_params.CA_cert_dir):
            raise ValueError(
                "You have provided `ca_cert_path` or `ca_cert_dir` in the URI which is only supported on Linux. "
                "Remove the setting in the connection URI and use your operating system defaults."
            )
        self._ca_cert_path = query_params.CA_cert_path
        self._ca_cert_dir = query_params.CA_cert_dir
        if not self._ca_cert_path and not self._ca_cert_dir and platform.system() == "Linux":
            if ssl.get_default_verify_paths().cafile is not None:
                self._ca_cert_path = ssl.get_default_verify_paths().cafile
            if ssl.get_default_verify_paths().capath is not None:
                self._ca_cert_dir = ssl.get_default_verify_paths().capath

        self._ssl = query_params.ssl
        self._encoding_version = encoding_version

        super().__init__(uri, self._encoding_version)

    def native_config(self):
        native_settings = NativeGCPXMLSettings()
        native_settings.bucket = self._bucket
        native_settings.endpoint = self._endpoint
        native_settings.access = self._access
        native_settings.secret = self._secret
        native_settings.aws_auth = self._aws_auth
        native_settings.prefix = ""  # Set on the returned value later when needed
        native_settings.https = self._https
        native_settings.ssl = self._ssl
        native_settings.ca_cert_path = self._ca_cert_path
        native_settings.ca_cert_dir = self._ca_cert_dir
        return NativeVariantStorage(native_settings)

    def __repr__(self):
        return "GCPXML(endpoint=%s, bucket=%s)" % (self._endpoint, self._bucket)

    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()
        _name = (
            self._access
            if self._aws_auth == AWSAuthMethod.DISABLED
            else USE_AWS_CRED_PROVIDERS_TOKEN
        )
        _key = (
            self._secret
            if self._aws_auth == AWSAuthMethod.DISABLED
            else USE_AWS_CRED_PROVIDERS_TOKEN
        )
        with_prefix = (
            f"{self._path_prefix}/{CONFIG_LIBRARY_NAME}" if self._path_prefix else False
        )

        add_gcp_library_to_env(
            cfg=env_cfg,
            lib_name=CONFIG_LIBRARY_NAME,
            env_name="local",
            with_prefix=with_prefix,
        )

        lib = NativeVersionStore.create_store_from_config(
            (env_cfg, self.native_config()), "local", CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )

        return lib._library

    def get_storage_override(self) -> StorageOverride:
        storage_override = StorageOverride()
        gcpxml_override = GCPXMLOverride()
        storage_override.set_gcpxml_override(gcpxml_override)
        return storage_override

    def get_masking_override(self) -> StorageOverride:
        storage_override = StorageOverride()
        gcpxml_override = GCPXMLOverride()
        storage_override.set_gcpxml_override(gcpxml_override)
        return storage_override

    def add_library_to_env(self, env_cfg: EnvironmentConfigsMap, name: str):
        if self._path_prefix:
            # add time to prefix - so that the s3 root folder is unique and we can delete and recreate fast
            with_prefix = f"{self._path_prefix}/{name}{time.time() * 1e9:.0f}"
        else:
            with_prefix = True

        add_gcp_library_to_env(
            cfg=env_cfg,
            lib_name=name,
            env_name="local",
            with_prefix=with_prefix,
        )
