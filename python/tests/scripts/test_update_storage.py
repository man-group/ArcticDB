import os
import sys

import pytest

from arcticdb import Arctic
from arcticdb.scripts.update_storage import run
from arcticdb.options import LibraryOptions, EnterpriseLibraryOptions
from arcticc.pb2.s3_storage_pb2 import Config as S3Config
from arcticc.pb2.azure_storage_pb2 import Config as AzureConfig
from arcticdb.storage_fixtures.api import ArcticUriFields
from arcticdb.storage_fixtures.azure import AzureContainer
from arcticdb.storage_fixtures.s3 import S3Bucket
from arcticdb.adapters.s3_library_adapter import USE_AWS_CRED_PROVIDERS_TOKEN
from ..util.mark import AZURE_TESTS_MARK, SSL_TEST_SUPPORTED, SKIP_CONDA_MARK
from ..util.storage_test import get_s3_storage_config

LIB_NAME = "test_lib"


def create_library_config(ac: Arctic, name: str):
    cfg = ac._library_adapter.get_library_config(name, LibraryOptions(), EnterpriseLibraryOptions())
    ac._library_manager.write_library_config(cfg, name, test_only_validation_toggle=False)


def _get_azure_storage_config(cfg):
    primary_storage_name = cfg.lib_desc.storage_ids[0]
    primary_any = cfg.storage_by_id[primary_storage_name]
    azure_config = AzureConfig()
    primary_any.config.Unpack(azure_config)
    return azure_config


@SKIP_CONDA_MARK  # issue with fixture cleanup
def test_upgrade_script_dryrun_s3(s3_storage: S3Bucket, lib_name):
    ac = s3_storage.create_arctic()
    try:
        create_library_config(ac, lib_name)

        # When
        run(uri=s3_storage.arctic_uri, run=False)

        # Then
        config = ac._library_manager.get_library_config(lib_name)
        storage_config = get_s3_storage_config(config)
        assert storage_config.bucket_name == s3_storage.bucket
        assert storage_config.credential_name == s3_storage.key.id
        assert storage_config.credential_key == s3_storage.key.secret
    finally:
        ac.delete_library(lib_name)


@SKIP_CONDA_MARK  # issue with fixture cleanup
def test_upgrade_script_s3(s3_storage: S3Bucket, lib_name):
    ac = s3_storage.create_arctic()
    try:
        create_library_config(ac, lib_name)

        run(uri=s3_storage.arctic_uri, run=True)

        config = ac._library_manager.get_library_config(lib_name)
        storage_config = get_s3_storage_config(config)
        assert storage_config.bucket_name == ""
        assert storage_config.credential_name == ""
        assert storage_config.credential_key == ""
    finally:
        ac.delete_library(lib_name)


@pytest.mark.parametrize("default_credential_provider", ["1", "true"])  # true for testing backwards compatibility
@SKIP_CONDA_MARK  # issue with fixture cleanup
def test_upgrade_script_s3_rbac_ok(s3_clean_bucket: S3Bucket, monkeypatch, default_credential_provider, lib_name):
    """Just _RBAC_ as creds is a placeholder. Leave config with that alone."""
    if os.name == "nt":
        if sys.version_info < (3, 9):
            pytest.skip("Older Python don't support unsetenv on Windows")

        # We statically linked msvcrt which maintains a seprate copy of the environment
        from arcticdb_ext.tools import putenv_s

        monkeypatch.setattr(os, "putenv", putenv_s)
        monkeypatch.setattr(os, "unsetenv", lambda n: putenv_s(n, ""))

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", s3_clean_bucket.key.id)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", s3_clean_bucket.key.secret)

    uri = s3_clean_bucket.replace_uri_field(
        s3_clean_bucket.arctic_uri, ArcticUriFields.USER, f"aws_auth={default_credential_provider}", start=1
    )
    uri = s3_clean_bucket.replace_uri_field(uri, ArcticUriFields.PASSWORD, "", start=1, end=3)
    s3_clean_bucket.arctic_uri = uri

    ac = s3_clean_bucket.create_arctic()  # create_arctic() does proper clean up
    try:
        create_library_config(ac, lib_name)  # Note this avoids the override so _RBAC_ is actually written to storage
        ac[lib_name].write_pickle("x", 123)

        run(uri=uri, run=True)

        ac = Arctic(uri)
        config = ac._library_manager.get_library_config(lib_name)
        s3_clean_bucket = get_s3_storage_config(config)
        assert s3_clean_bucket.bucket_name == s3_clean_bucket.bucket_name
        assert s3_clean_bucket.credential_name == USE_AWS_CRED_PROVIDERS_TOKEN
        assert s3_clean_bucket.credential_key == USE_AWS_CRED_PROVIDERS_TOKEN

        lib = ac[lib_name]
        assert lib.read("x").data == 123
    finally:
        ac.delete_library(lib_name)


@AZURE_TESTS_MARK
def test_upgrade_script_dryrun_azure(azurite_storage: AzureContainer, lib_name):
    # Given
    if SSL_TEST_SUPPORTED:
        # azurite factory doesn't set client_cert_dir by default
        azurite_storage.arctic_uri += f";CA_cert_dir={azurite_storage.factory.client_cert_dir}"
    ac = azurite_storage.create_arctic()
    create_library_config(ac, lib_name)

    # When
    run(uri=azurite_storage.arctic_uri, run=False)

    # Then
    config = ac._library_manager.get_library_config(lib_name)
    azure_storage = _get_azure_storage_config(config)
    assert azure_storage.ca_cert_path == azurite_storage.factory.client_cert_file
    assert azure_storage.ca_cert_dir == azurite_storage.factory.client_cert_dir
    assert azure_storage.container_name == azurite_storage.container
    assert azurite_storage.factory.account_name in azure_storage.endpoint
    assert azurite_storage.factory.account_key in azure_storage.endpoint
    assert azure_storage.max_connections == 0
    assert azure_storage.prefix.startswith(lib_name)


@AZURE_TESTS_MARK
def test_upgrade_script_azure(azurite_storage: AzureContainer, lib_name):
    # Given
    if SSL_TEST_SUPPORTED:
        # azurite factory doesn't set client_cert_dir by default
        azurite_storage.arctic_uri += f";CA_cert_dir={azurite_storage.factory.client_cert_dir}"
    ac = azurite_storage.create_arctic()
    create_library_config(ac, lib_name)

    try:
        run(uri=azurite_storage.arctic_uri, run=True)

        config = ac._library_manager.get_library_config(lib_name)
        azure_storage = _get_azure_storage_config(config)
        assert azure_storage.ca_cert_path == ""
        assert azure_storage.ca_cert_dir == ""
        assert azure_storage.container_name == ""
        assert azure_storage.endpoint == ""
        assert azure_storage.prefix.startswith(lib_name)
    finally:
        ac.delete_library(lib_name)
