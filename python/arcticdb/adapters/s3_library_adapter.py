"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import re
import time
from typing import Optional
from urllib.parse import urlsplit
import ssl
import platform

from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryDescriptor
from arcticdb.version_store.helper import add_s3_library_to_env
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter
from arcticdb.log import version as log
from arcticdb_ext.storage import (
    StorageOverride,
    S3Override,
    CONFIG_LIBRARY_NAME,
    AWSAuthMethod,
    NativeVariantStorage,
    S3Settings as NativeS3Settings,
)
from arcticdb.encoding_version import EncodingVersion
from arcticdb.util.utils import strtobool
from collections import namedtuple
from dataclasses import dataclass, fields

PARSED_QUERY = namedtuple("PARSED_QUERY", ["region"])
USE_AWS_CRED_PROVIDERS_TOKEN = "_RBAC_"

# Standard AWS S3 locations, as used by the AWS CLI, boto3, s3fs, pandas, etc.
#   s3://bucket[/prefix]
#   https://bucket.s3.region.amazonaws.com[/prefix]  (virtual-hosted style)
#   https://s3.region.amazonaws.com/bucket[/prefix]  (path style)
_STANDARD_S3_URI_REGEX = re.compile(
    r"s3s?://(?P<bucket>[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9])(?P<path>/[^?]*)?(?P<query>\?.*)?"
)
_AWS_REGION_PATTERN = r"[a-z]{2}(?:-[a-z]+)+-\d+"
_AWS_S3_HOST_REGEX = re.compile(
    r"(?:(?P<bucket>[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9])\.)?s3(?:[.\-](?P<region>"
    + _AWS_REGION_PATTERN
    + r"))?\.amazonaws\.com"
)


@dataclass
class _StandardS3Location:
    bucket: str
    path_prefix: Optional[str]
    region: Optional[str]
    query: Optional[str]


def _is_legacy_s3_uri(uri: str) -> bool:
    # ArcticDB's own format is s3(s)://<endpoint>:<bucket>, so the part before the query contains a ':'
    authority_and_path = uri.split("://", 1)[1].split("?", 1)[0]
    return ":" in authority_and_path


def _parse_standard_s3_uri(uri: str) -> Optional[_StandardS3Location]:
    """Parse a standard AWS S3 location. Returns None if the URI is not in one of the standard forms."""
    if uri.startswith(("s3://", "s3s://")):
        if _is_legacy_s3_uri(uri):
            return None
        match = _STANDARD_S3_URI_REGEX.fullmatch(uri)
        if not match:
            return None
        return _StandardS3Location(
            bucket=match["bucket"],
            path_prefix=(match["path"] or "").strip("/") or None,
            region=None,
            query=match["query"],
        )
    if uri.startswith("https://"):
        parts = urlsplit(uri)
        host_match = _AWS_S3_HOST_REGEX.fullmatch(parts.hostname or "")
        if not host_match or parts.port or parts.fragment:
            return None
        path = parts.path.strip("/")
        bucket = host_match["bucket"]
        if not bucket:
            # path style: the bucket is the first path segment
            bucket, _, path = path.partition("/")
            if not bucket:
                return None
        return _StandardS3Location(
            bucket=bucket,
            path_prefix=path or None,
            region=host_match["region"],
            query=f"?{parts.query}" if parts.query else None,
        )
    return None


@dataclass
class ParsedQuery:
    port: Optional[int] = None

    region: Optional[str] = None
    use_virtual_addressing: bool = False

    access: Optional[str] = None
    secret: Optional[str] = None
    aws_auth: Optional[AWSAuthMethod] = AWSAuthMethod.DISABLED
    aws_profile: str = ""

    path_prefix: Optional[str] = None

    # DEPRECATED - see https://github.com/man-group/ArcticDB/pull/833
    force_uri_lib_config: Optional[bool] = True

    # winhttp is used as s3 backend support on Windows by default; winhttp itself maintains ca cert.
    # The options has no effect on Windows
    CA_cert_path: Optional[str] = ""  # CURLOPT_CAINFO in curl
    CA_cert_dir: Optional[str] = ""  # CURLOPT_CAPATH in curl

    ssl: Optional[bool] = False

    _test_only_is_nfs_layout: bool = False


class S3LibraryAdapter(ArcticLibraryAdapter):
    REGEX = r"s3(s)?://(?P<endpoint>[^:]*):(?P<bucket>[-_a-zA-Z0-9.]+)(?P<query>\?.*)?"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        if uri.startswith("https://"):
            return _parse_standard_s3_uri(uri) is not None
        return uri.startswith("s3://") or uri.startswith("s3s://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        standard_location = _parse_standard_s3_uri(uri)
        if standard_location:
            self._init_from_standard_location(standard_location)
        else:
            match = re.match(self.REGEX, uri)
            if not match:
                raise ValueError(
                    f"Invalid S3 URI '{uri}'. Expected s3://<bucket>[/<prefix>][?<options>] for AWS S3, "
                    "an https://<bucket>.s3.<region>.amazonaws.com[/<prefix>] URL, "
                    "or s3(s)://<endpoint>:<bucket>[?<options>] for other S3 compatible storages."
                )
            match_groups = match.groupdict()

            self._endpoint = match_groups["endpoint"]
            self._bucket = match_groups["bucket"]

            self._query_params: ParsedQuery = self._parse_query(match["query"])
            self._https = uri.startswith("s3s")

        if self._query_params.force_uri_lib_config is False:
            raise ValueError(
                "The support of 'force_uri_lib_config=false' has been dropped due to security concerns. Please refer to"
                " https://github.com/man-group/ArcticDB/pull/803 for more information."
            )

        if self._query_params.port:
            self._endpoint += f":{self._query_params.port}"

        self._encoding_version = encoding_version
        if platform.system() != "Linux" and (self._query_params.CA_cert_path or self._query_params.CA_cert_dir):
            raise ValueError(
                "You have provided `ca_cert_path` or `ca_cert_dir` in the URI which is only supported on Linux. "
                "Remove the setting in the connection URI and use your operating system defaults."
            )
        self._ca_cert_path = self._query_params.CA_cert_path
        self._ca_cert_dir = self._query_params.CA_cert_dir
        if not self._ca_cert_path and not self._ca_cert_dir and platform.system() == "Linux":
            if ssl.get_default_verify_paths().cafile is not None:
                self._ca_cert_path = ssl.get_default_verify_paths().cafile
            if ssl.get_default_verify_paths().capath is not None:
                self._ca_cert_dir = ssl.get_default_verify_paths().capath

        self._ssl = self._query_params.ssl

        self._test_only_is_nfs_layout = self._query_params._test_only_is_nfs_layout

        if "amazonaws" in self._endpoint:
            self._configure_aws()

        super().__init__(uri, self._encoding_version)

    def _init_from_standard_location(self, location: _StandardS3Location):
        """
        Standard AWS S3 URIs always point at AWS: the endpoint is derived from the region, HTTPS is used, and,
        as with the AWS CLI and boto3, the default AWS credentials provider chain is used unless keys are given.
        """
        query_params = self._parse_query(location.query)
        if location.path_prefix:
            if query_params.path_prefix and query_params.path_prefix != location.path_prefix:
                raise ValueError(
                    f"Invalid S3 URI. The path prefix '{location.path_prefix}' in the URI conflicts with "
                    f"path_prefix='{query_params.path_prefix}' in the query parameters."
                )
            query_params.path_prefix = location.path_prefix

        if query_params.region and location.region and query_params.region != location.region:
            raise ValueError(
                f"Invalid S3 URI. The region '{location.region}' in the URL conflicts with "
                f"region='{query_params.region}' in the query parameters."
            )
        region = (
            query_params.region
            or location.region
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
        )
        if not region:
            raise ValueError(
                f"Can't determine the AWS region for bucket '{location.bucket}'. Add it to the URI "
                f"(e.g. s3://{location.bucket}?region=us-east-1) or set the AWS_REGION environment variable."
            )
        query_params.region = region

        if query_params.aws_auth == AWSAuthMethod.DISABLED and not (query_params.access or query_params.secret):
            query_params.aws_auth = AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN

        # Virtual-hosted style is what AWS recommends, but TLS certificates don't cover bucket names with dots
        query_params.use_virtual_addressing = "." not in location.bucket

        self._query_params = query_params
        self._bucket = location.bucket
        self._endpoint = f"s3.{region}.amazonaws.com"
        self._https = True

    def native_config(self):
        if self._test_only_is_nfs_layout:
            return None
        else:
            return NativeVariantStorage(
                NativeS3Settings(self._query_params.aws_auth, self._query_params.aws_profile, False)
            )

    def __repr__(self):
        return "S3(endpoint=%s, bucket=%s)" % (self._endpoint, self._bucket)

    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()
        if self._query_params.aws_auth == AWSAuthMethod.DISABLED:
            _name = self._query_params.access
            _key = self._query_params.secret
        else:
            if self._query_params.access or self._query_params.secret:
                log.warning("aws_auth is enabled. access and secret set will be ignored")
            _name = USE_AWS_CRED_PROVIDERS_TOKEN
            _key = USE_AWS_CRED_PROVIDERS_TOKEN

        with_prefix = (
            f"{self._query_params.path_prefix}/{CONFIG_LIBRARY_NAME}" if self._query_params.path_prefix else False
        )

        add_s3_library_to_env(
            env_cfg,
            lib_name=CONFIG_LIBRARY_NAME,
            credential_name=_name,
            credential_key=_key,
            bucket_name=self._bucket,
            endpoint=self._endpoint,
            env_name=_DEFAULT_ENV,
            is_https=self._https,
            with_prefix=with_prefix,
            region=self._query_params.region,
            use_virtual_addressing=self._query_params.use_virtual_addressing,
            ca_cert_path=self._ca_cert_path,
            ca_cert_dir=self._ca_cert_dir,
            ssl=self._ssl,
            is_nfs_layout=self._test_only_is_nfs_layout,
            aws_auth=self._query_params.aws_auth,
            aws_profile=self._query_params.aws_profile,
            native_cfg=self.native_config(),
        )
        if self._test_only_is_nfs_layout:
            lib = NativeVersionStore.create_store_from_config(
                (env_cfg), _DEFAULT_ENV, CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
            )
        else:
            lib = NativeVersionStore.create_store_from_config(
                (env_cfg, self.native_config()),
                _DEFAULT_ENV,
                CONFIG_LIBRARY_NAME,
                encoding_version=self._encoding_version,
            )

        return lib._library

    def _parse_query(self, query: str) -> ParsedQuery:
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
                    "Invalid S3 URI. "
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
                elif value.lower() == "sts":
                    parsed_query[key] = AWSAuthMethod.STS_PROFILE_CREDENTIALS_PROVIDER
                else:
                    parsed_query[key] = AWSAuthMethod.DISABLED

        if parsed_query.get("path_prefix"):
            parsed_query["path_prefix"] = parsed_query["path_prefix"].strip("/")

        _kwargs = {k: v for k, v in parsed_query.items()}
        return ParsedQuery(**_kwargs)

    def _get_s3_override(self) -> S3Override:
        s3_override = S3Override()
        # storage_override will overwrite access and key while reading config from storage
        # access and secret whether equals to _RBAC_ are used for determining aws_auth is true on C++ layer
        if self._query_params.aws_auth == AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN:
            s3_override.credential_name = USE_AWS_CRED_PROVIDERS_TOKEN
            s3_override.credential_key = USE_AWS_CRED_PROVIDERS_TOKEN
        else:
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
        if self._https:
            s3_override.https = self._https
        if self._ca_cert_path:
            s3_override.ca_cert_path = self._ca_cert_path
        if self._ca_cert_dir:
            s3_override.ca_cert_dir = self._ca_cert_dir
        if self._ssl:
            s3_override.ssl = self._ssl

        s3_override.use_virtual_addressing = self._query_params.use_virtual_addressing
        return s3_override

    def get_storage_override(self) -> StorageOverride:
        storage_override = StorageOverride()
        if not self._test_only_is_nfs_layout:
            storage_override.set_s3_override(self._get_s3_override())
        return storage_override

    def get_masking_override(self) -> StorageOverride:
        storage_override = StorageOverride()
        s3_override = S3Override()
        storage_override.set_s3_override(s3_override)
        return storage_override

    def add_library_to_env(self, env_cfg: EnvironmentConfigsMap, name: str):
        _name = (
            self._query_params.access
            if self._query_params.aws_auth == AWSAuthMethod.DISABLED
            else USE_AWS_CRED_PROVIDERS_TOKEN
        )
        _key = (
            self._query_params.secret
            if self._query_params.aws_auth == AWSAuthMethod.DISABLED
            else USE_AWS_CRED_PROVIDERS_TOKEN
        )

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
            ca_cert_path=self._ca_cert_path,
            ca_cert_dir=self._ca_cert_dir,
            ssl=self._ssl,
            is_nfs_layout=self._test_only_is_nfs_layout,
            aws_auth=self._query_params.aws_auth,
            aws_profile=self._query_params.aws_profile,
            native_cfg=self.native_config(),
        )

    def _configure_aws(self):
        if not self._query_params.region:
            match = re.match(r"s3[-\.](?P<region>[a-z0-9-]+)\.amazonaws.*", self._endpoint)
            if match:
                match_groups = match.groupdict()
                self._query_params.region = match_groups["region"]
                self._query_params.use_virtual_addressing = True

    @property
    def path_prefix(self):
        return self._query_params.path_prefix
