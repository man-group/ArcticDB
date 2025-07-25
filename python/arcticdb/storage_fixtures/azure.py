"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import re
import shutil
import uuid
from typing import TYPE_CHECKING, Optional
from tempfile import mkdtemp

from .api import *
from .utils import get_ephemeral_port, GracefulProcessUtils, wait_for_server_to_come_up, safer_rmtree, get_ca_cert_for_testing
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

    def _set_uri_and_client(self, auth: str):
        from azure.storage.blob import ContainerClient, LinearRetry

        f = self.factory
        self.arctic_uri = (
            f"azure://DefaultEndpointsProtocol={f.http_protocol};{auth};BlobEndpoint={f.endpoint_root}/{f.account_name};"
            f"Container={self.container};CA_cert_path={f.client_cert_file}"
        )
        # CA_cert_dir is skipped on purpose; It will be tested manually in other tests

        # The retry_policy instance will be modified by the pipeline, so cannot be constant
        policy = {"connection_timeout": 1, "read_timeout": 2, "retry_policy": LinearRetry(retry_total=3, backoff=1), "connection_verify": f.client_cert_file}
        self.client = ContainerClient.from_connection_string(self.arctic_uri, self.container, **policy)
        # add connection_verify=False to bypass ssl checking

    def __init__(self, factory: "AzuriteStorageFixtureFactory") -> None:
        super().__init__()
        self.factory = factory
        self.container = f"container{str(uuid.uuid1())[:8]}"
        self._set_uri_and_client(f"AccountName={factory.account_name};AccountKey={factory.account_key}")

        # __exit__() assumes this object owns the container, so always create and bail if exists:
        self.client.create_container()

    def _safe_enter(self):
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
            self._set_uri_and_client("SharedAccessSignature=" + sas)

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client:
            if self._admin_client:
                self._admin_client.delete_container(timeout=3)
                self._admin_client.close()
            else:
                self.client.delete_container(timeout=3)
            self.client.close()
            self.client = None

    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        add_azure_library_to_env(
            cfg=cfg,
            lib_name=lib_name,
            env_name=Defaults.ENV,
            container_name=self.container,
            endpoint=self.arctic_uri,
            ca_cert_path=self.factory.client_cert_file,
            with_prefix=False,  # to allow azure_store_factory reuse_name to work correctly
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

    def __init__(self, port=0, working_dir: Optional[str] = None, use_ssl: bool = True, ssl_test_support: bool = True):
        self.http_protocol = "https" if use_ssl else "http"
        self.port = port or get_ephemeral_port(1)
        self.endpoint_root = f"{self.http_protocol}://{self.host}:{self.port}"
        self.working_dir = str(working_dir) if working_dir else mkdtemp(suffix="AzuriteStorageFixtureFactory")
        self.ssl_test_support = ssl_test_support

    def __str__(self):
        return f"AzuriteStorageFixtureFactory[port={self.port},dir={self.working_dir}]"

    def _safe_enter(self):
        args = f"{shutil.which('azurite')} --blobPort {self.port} --blobHost {self.host} --queuePort 0 --tablePort 0 --skipApiVersionCheck --silent"
        if self.ssl_test_support:
            self.client_cert_dir = self.working_dir
            self.ca, self.key_file, self.cert_file, self.client_cert_file = get_ca_cert_for_testing(self.client_cert_dir)
        else:
            self.ca = ""
            self.key_file = ""
            self.cert_file = ""
            self.client_cert_file = ""
            self.client_cert_dir = ""
        if self.http_protocol == "https":
            args += f" --key {self.key_file} --cert {self.cert_file}"
        self._p = GracefulProcessUtils.start(args, cwd=self.working_dir)
        wait_for_server_to_come_up(self.endpoint_root, "azurite", self._p)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with handle_cleanup_exception(self, "process", consequence="Subsequent file deletion may also fail. "):
            GracefulProcessUtils.terminate(self._p)
        safer_rmtree(self, self.working_dir)

    def create_fixture(self) -> AzureContainer:
        return AzureContainer(self)
