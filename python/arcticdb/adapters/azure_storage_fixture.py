"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import re
import shutil
from typing import Optional
from tempfile import mkdtemp

from azure.storage.blob import (
    ContainerClient,
    AccessPolicy,
    ContainerSasPermissions,
    generate_container_sas,
    LinearRetry,
)

from arcticdb.version_store.helper import add_azure_library_to_env
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap

from .storage_fixture_api import *


class AzureContainer(StorageFixture):
    _FIELD_REGEX = {
        ArcticUriFields.HOST: re.compile("[/;](BlobEndpoint=https?://)([^;:/]+)"),
        ArcticUriFields.USER: re.compile("[/;](AccountName=)([^;]+);"),
        ArcticUriFields.PASSWORD: re.compile("[/;](AccountKey=)([^;]+);"),
        ArcticUriFields.BUCKET: re.compile("[/;](Container=)([^;]+);"),
    }
    _POLICY = {"connection_timeout": 1, "read_timeout": 2, "retry_policy": LinearRetry(retry_total=3, backoff=1)}
    _bucket_id = 0

    container: str
    client: Optional[ContainerClient]
    _admin_client: Optional[ContainerClient] = None

    def _set_uri_and_client(self, auth: str):
        f = self.factory
        self.arctic_uri = (
            f"azure://DefaultEndpointsProtocol=http;{auth};BlobEndpoint={f.endpoint_root}/{f.account_name};"
            f"Container={self.container};CA_cert_path={f.ca_cert_path}"
        )

        self.client = ContainerClient.from_connection_string(self.arctic_uri, self.container, **self._POLICY)
        # add connection_verify=False to bypass ssl checking

    def __init__(self, factory: "AzuriteStorageFixtureFactory") -> None:
        self.factory = factory
        self.container = f"container{self._bucket_id}"
        AzureContainer._bucket_id += 1
        self._set_uri_and_client(f"AccountName={factory.account_name};AccountKey={factory.account_key}")

        # __exit__() assumes this object owns the container, so always create and bail if exists:
        self.client.create_container()

        if factory.enforcing_permissions:
            self._admin_client = self.client
            self.set_permission(read=False, write=False)
            sas = generate_container_sas(
                account_name=factory.account_name,
                account_key=factory.account_key,
                container_name=self.container,
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
            ca_cert_path=self.factory.ca_cert_path,
        )
        return cfg

    def set_permission(self, *, read: bool, write: bool):
        client = self._admin_client
        assert client, "This instance was not created with a enforcing_permissions Factory"
        perm = ContainerSasPermissions(read=read, list=read, write=write, delete=write)
        client.set_container_access_policy({"main": AccessPolicy(permission=perm)})


class AzuriteStorageFixtureFactory(StorageFixtureFactory):
    host = "127.0.0.1"

    # Per https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string#configure-a-connection-string-for-azurite
    account_name = "devstoreaccount1"
    account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

    # Default cert path is used; May run into problem on Linux's non RHEL distribution
    # See more on https://github.com/man-group/ArcticDB/issues/514
    ca_cert_path = ""

    enforcing_permissions = False
    """Set to True to create AzureContainer with SAS authentication"""

    def __init__(self, port=0, working_dir: Optional[str] = None):
        self.port = port or get_ephemeral_port()
        self.endpoint_root = f"http://{self.host}:{self.port}"
        self.working_dir = str(working_dir) if working_dir else mkdtemp(suffix="AzuriteStorageFixtureFactory")

    def __enter__(self):
        exe = shutil.which("azurite")
        cmd = f"{exe} --blobPort {self.port} --blobHost {self.host} --queuePort 0 --tablePort 0"
        self._p = start_process_that_can_be_gracefully_killed(cmd, cwd=self.working_dir)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            gracefully_kill_process(self._p)
        finally:
            shutil.rmtree(self.working_dir, ignore_errors=True)

    def create_fixture(self) -> AzureContainer:
        return AzureContainer(self)
