"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import re
import shutil
import ssl
import stat
import tempfile
import uuid
from typing import TYPE_CHECKING, Optional, Union
from tempfile import mkdtemp

from arcticdb.util.logger import get_logger
from arcticdb_ext.storage import NativeVariantStorage
from azure.core.exceptions import ResourceNotFoundError

from .api import *
from .utils import (
    _LINUX,
    get_ephemeral_port,
    GracefulProcessUtils,
    wait_for_server_to_come_up,
    safer_rmtree,
    get_ca_cert_for_testing,
)
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
from arcticdb.version_store.helper import add_azure_library_to_env

# All storage client libraries to be imported on-demand to speed up start-up of ad-hoc test runs
if TYPE_CHECKING:
    from azure.storage.blob import ContainerClient


class AzureContainer(StorageFixture):
    _FIELD_REGEX = {
        ArcticUriFields.HOST: re.compile("[/;](BlobEndpoint=https?://)([^;:/]+)"),
        ArcticUriFields.USER: re.compile("[/;](AccountName=)([^;]+)(;)"),
        ArcticUriFields.PASSWORD: re.compile("[/;](AccountKey=)([^;]+)(;)"),
        ArcticUriFields.BUCKET: re.compile("[/;](Container=)([^;]+)(;)"),
        ArcticUriFields.CA_PATH: re.compile("[/;](CA_cert_path=)([^;]*)(;?)"),
        ArcticUriFields.PATH_PREFIX: re.compile("[/;](Path_prefix=)([^;]*)(;?)"),
    }

    container: str
    client: Optional["ContainerClient"]
    _admin_client: Optional["ContainerClient"] = None

    def _get_policy(self) -> str:
        from azure.storage.blob import LinearRetry

        # The retry_policy instance will be modified by the pipeline, so cannot be constant
        return {
            "connection_timeout": 1,
            "read_timeout": 2,
            "retry_policy": LinearRetry(retry_total=3, backoff=1),
            "connection_verify": self.factory.client_cert_file,
        }

    def _set_uri_and_client_azurite(self, auth: str):
        from azure.storage.blob import ContainerClient

        f = self.factory
        self.arctic_uri = (
            f"azure://DefaultEndpointsProtocol={f.http_protocol};{auth};BlobEndpoint={f.endpoint_root}/{f.account_name};"
            f"Container={self.container};CA_cert_path={f.client_cert_file}"
        )
        # CA_cert_dir is skipped on purpose; It will be tested manually in other tests

        self.client = ContainerClient.from_connection_string(self.arctic_uri, self.container, **self._get_policy())
        # add connection_verify=False to bypass ssl checking

    def __init__(self, factory: Union["AzuriteStorageFixtureFactory", "AzureStorageFixtureFactory"]) -> None:
        from azure.storage.blob import ContainerClient

        super().__init__()
        self.factory = factory
        if self.is_real_azure():
            self.container = self.factory.default_container
            self.arctic_uri = self.factory.get_arctic_uri()
            self.client = ContainerClient.from_connection_string(self.arctic_uri, self.container, **self._get_policy())
        else:
            self.container = f"container{str(uuid.uuid1())[:8]}"
            self._set_uri_and_client_azurite(f"AccountName={factory.account_name};AccountKey={factory.account_key}")
            # __exit__() assumes this object owns the container, so always create and bail if exists:
            self.client.create_container()

    def is_real_azure(self) -> bool:
        return isinstance(self.factory, AzureStorageFixtureFactory)

    def _safe_enter(self):
        from azure.storage.blob import ContainerClient

        if self.factory.enforcing_permissions:
            from azure.storage.blob import generate_container_sas

            self._admin_client = self.client
            self.set_permission(read=False, write=False)
            sas = generate_container_sas(
                account_name=self.factory.account_name,
                account_key=self.factory.account_key,
                container_name=self.container,
                permission="racwdxyltfmei",  # All permissions, which will be tailored by the policy_id below. See
                # https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas#permissions-for-a-directory-container-or-blob
                policy_id="main",
            )
            if self.is_real_azure():
                # Things to do against real Azure
                uri = f"{self.factory.get_arctic_uri()};SharedAccessSignature={sas}"
                self.client = ContainerClient.from_connection_string(uri, self.container, **self._get_policy())
            else:
                # Container is in a mock env - Azurite
                self._set_uri_and_client_azurite("SharedAccessSignature=" + sas)

    def __exit__(self, exc_type, exc_value, traceback):
        if self.factory.clean_bucket_on_fixture_exit:
            self.factory.cleanup_container(self)
        if self.client:
            self.client.close()
            self.client = None

    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        if self.factory.default_prefix:
            with_prefix = f"{self.factory.default_prefix}/{lib_name}"
        else:
            with_prefix = False
        add_azure_library_to_env(
            cfg=cfg,
            lib_name=lib_name,
            env_name=Defaults.ENV,
            container_name=self.container,
            endpoint=self.arctic_uri,
            ca_cert_path=self.factory.client_cert_file,
            with_prefix=with_prefix,  # to allow azure_store_factory reuse_name to work correctly
        )
        return cfg

    def set_permission(self, *, read: bool, write: bool):
        from azure.storage.blob import ContainerSasPermissions, AccessPolicy

        client = self._admin_client
        assert client, "This instance was not created with a enforcing_permissions Factory"
        # Azurite has a bug where a completely empty (all False) policy becomes undefined when read...
        perm = ContainerSasPermissions(read=read, list=read, write=write, delete=write, set_immutability_policy=True)
        client.set_container_access_policy({"main": AccessPolicy(permission=perm)})

    def iter_underlying_object_names(self):
        return (b.name for b in self.client.list_blobs())  # list_blob_names first appears in v12.14...

    def copy_underlying_objects_to(self, destination: "AzureContainer"):
        src_container = self.client
        dst_container = destination.client
        for name in self.iter_underlying_object_names():
            src_blob = src_container.get_blob_client(name)
            dst_blob = dst_container.get_blob_client(name)
            dst_blob.start_copy_from_url(src_blob.url, requires_sync=True)
            props = dst_blob.get_blob_properties()
            assert props.copy.status == "success"


class AzuriteStorageFixtureFactory(StorageFixtureFactory):
    host = "localhost"

    # Per https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string#configure-a-connection-string-for-azurite
    account_name = "devstoreaccount1"
    account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

    enforcing_permissions = False
    """Set to True to create AzureContainer with SAS authentication"""

    clean_bucket_on_fixture_exit: bool = True

    default_prefix: str = None

    def __init__(
        self, port=None, working_dir: Optional[str] = None, use_ssl: bool = True, ssl_test_support: bool = True
    ):
        self.http_protocol = "https" if use_ssl else "http"
        seed = 1 if use_ssl else 10
        self.port = port or get_ephemeral_port(seed)
        self.endpoint_root = f"{self.http_protocol}://{self.host}:{self.port}"
        self.working_dir = str(working_dir) if working_dir else mkdtemp(suffix="AzuriteStorageFixtureFactory")
        self.ssl_test_support = ssl_test_support

    def __str__(self):
        return f"AzuriteStorageFixtureFactory[port={self.port},dir={self.working_dir}]"

    def _safe_enter(self):
        args = f"{shutil.which('azurite')} --blobPort {self.port} --blobHost {self.host} --queuePort 0 --tablePort 0 --skipApiVersionCheck --silent"
        if self.ssl_test_support:
            self.client_cert_dir = self.working_dir
            self.ca, self.key_file, self.cert_file, self.client_cert_file = get_ca_cert_for_testing(
                self.client_cert_dir
            )
        else:
            self.ca = ""
            self.key_file = ""
            self.cert_file = ""
            self.client_cert_file = ""
            self.client_cert_dir = ""
        if self.http_protocol == "https":
            args += f" --key {self.key_file} --cert {self.cert_file}"
        self._p = GracefulProcessUtils.start_with_retry(
            url=self.endpoint_root,
            service_name="azurite",
            num_retries=2,
            timeout=240,
            process_start_cmd=args,
            cwd=self.working_dir,
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with handle_cleanup_exception(self, "process", consequence="Subsequent file deletion may also fail. "):
            GracefulProcessUtils.terminate(self._p)
        safer_rmtree(self, self.working_dir)

    def create_fixture(self) -> AzureContainer:
        return AzureContainer(self)

    def cleanup_container(self, b: AzureContainer):
        def delete_container_safely(client, timeout):
            try:
                client.delete_container(timeout=timeout)
            except ResourceNotFoundError:
                # Container didn't exist — that's fine, treat as success
                pass

        if b.client:
            if not b.is_real_azure():
                # This code is only for Azurite cleaning, it is faster than this for Azure
                if b._admin_client:
                    delete_container_safely(b._admin_client, timeout=3)
                    b._admin_client.close()
                else:
                    delete_container_safely(b.client, timeout=3)


def find_ca_certs():
    # Common CA certificates locations
    default_paths = ssl.get_default_verify_paths()
    possible_paths = [
        default_paths.cafile,
        default_paths.openssl_cafile_env,
        default_paths.openssl_cafile,
        "/etc/ssl/certs/ca-certificates.crt",
        "/usr/lib/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/ssl/cert.pem",
    ]
    for path in possible_paths:
        if path and os.path.isfile(path):
            return path
    return None


def copy_ca_certs(source_path: str, new_filename: str) -> str:
    """
    Copies a file to the system's temporary directory under a new filename.
    Sets the file permissions to readable and writable by everyone (chmod 666).
    Returns the full path to the new file.
    """
    temp_dir = tempfile.gettempdir()
    destination_path = os.path.join(temp_dir, new_filename)
    shutil.copy2(source_path, destination_path)
    os.chmod(destination_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
    return destination_path


class AzureStorageFixtureFactory(StorageFixtureFactory):
    endpoint: str
    account_name: str
    account_key: str
    connection_string: str = None
    default_container: str = None
    default_prefix: Optional[str] = None
    client_cert_file: str = None
    protocol: str = None
    clean_bucket_on_fixture_exit = True

    def __init__(self, native_config: Optional[dict] = None):
        self.native_config = native_config
        if _LINUX:
            ca_certs_file = find_ca_certs()
            assert ca_certs_file, f"CA file: {ca_certs_file} not found!"
            ca_certs_path = copy_ca_certs(ca_certs_file, "ca-certificates.crt")
            AzureStorageFixtureFactory.client_cert_file = ca_certs_path
            file_stats = os.stat(ca_certs_path)
            get_logger().info(f"File: {ca_certs_file}, copied to: {ca_certs_path}")
            get_logger().info(f"File: {ca_certs_path}, stats: {file_stats}")

    def _safe_enter(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __str__(self):
        return f"[{type(self)}=Container:{self.default_container}], ConnectionString:{self.connection_string}"

    def initialize_from_connection_sting(
        self, constr: str, container: str, prefix: str = None
    ) -> "AzureStorageFixtureFactory":
        def extract_from_regex(re_expr: str, constr: str) -> str:
            match = re.search(re_expr, constr)
            return match.group(1) if match else ""

        if constr is None:
            get_logger().error(f"Azure connection string not available: {constr}")
        if container is None:
            get_logger().error(f"Azure container not available: {container}")
        AzureStorageFixtureFactory.connection_string = constr
        AzureStorageFixtureFactory.account_name = extract_from_regex(r"AccountName=([^;]+)", constr)
        AzureStorageFixtureFactory.account_key = extract_from_regex(r"AccountKey=([^;]+)", constr)
        AzureStorageFixtureFactory.protocol = extract_from_regex(r"DefaultEndpointsProtocol=([^;]+)", constr)
        endpoint_suffix = extract_from_regex(r"EndpointSuffix=([^;]+)", constr)
        AzureStorageFixtureFactory.endpoint = (
            f"{AzureStorageFixtureFactory.protocol}://{AzureStorageFixtureFactory.account_name}.blob.{endpoint_suffix}"
        )
        AzureStorageFixtureFactory.default_container = container
        if prefix:
            AzureStorageFixtureFactory.default_prefix = prefix
        return self

    def get_arctic_uri(self):
        url = f"azure://Container={self.default_container};Path_prefix={self.default_prefix}"
        if self.client_cert_file:
            url += f";CA_cert_path={self.client_cert_file}"
        if self.connection_string:
            url += f";{self.connection_string}"
        else:
            get_logger().error(f"Constructed URL is probably not correct: {url}")
        return url

    def create_fixture(self) -> AzureContainer:
        return AzureContainer(self)

    def cleanup_container(self, b: AzureContainer):
        b.slow_cleanup(failure_consequence="The following delete bucket call will also fail. ")
        if len(b.libs_from_factory) > 0:
            get_logger().warning(f"Libraries not cleared remaining {b.libs_from_factory.keys()}")


def real_azure_from_environment_variables(
    shared_path: bool, native_config: Optional[NativeVariantStorage] = None, additional_suffix: str = ""
) -> AzureStorageFixtureFactory:
    out = AzureStorageFixtureFactory(native_config=native_config)
    if shared_path:
        prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    else:
        prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX", "") + additional_suffix
    out.initialize_from_connection_sting(
        constr=os.getenv("ARCTICDB_REAL_AZURE_CONNECTION_STRING"),
        container=os.getenv("ARCTICDB_REAL_AZURE_CONTAINER"),
        prefix=prefix,
    )
    return out
