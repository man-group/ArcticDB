import os
import sys

import pytest

from arcticdb import Arctic
from arcticdb.scripts.update_storage import run
from arcticdb.options import LibraryOptions
from arcticc.pb2.s3_storage_pb2 import Config as S3Config
from arcticc.pb2.azure_storage_pb2 import Config as AzureConfig
from arcticdb.storage_fixtures.api import ArcticUriFields
from arcticdb.storage_fixtures.azure import AzureContainer
from arcticdb.storage_fixtures.s3 import S3Bucket
from arcticdb.adapters.s3_library_adapter import USE_AWS_CRED_PROVIDERS_TOKEN
from tests.util.mark import AZURE_TESTS_MARK

LIB_NAME = "test_lib"


def create_library_config(ac: Arctic, name: str):
    opts = LibraryOptions()
    cfg = ac._library_adapter.get_library_config(name, opts)
    ac._library_manager.write_library_config(cfg, name, test_only_validation_toggle=False)


def _get_s3_storage_config(cfg):
    primary_storage_name = cfg.lib_desc.storage_ids[0]
    primary_any = cfg.storage_by_id[primary_storage_name]
    s3_config = S3Config()
    primary_any.config.Unpack(s3_config)
    return s3_config


def _get_azure_storage_config(cfg):
    primary_storage_name = cfg.lib_desc.storage_ids[0]
    primary_any = cfg.storage_by_id[primary_storage_name]
    azure_config = AzureConfig()
    primary_any.config.Unpack(azure_config)
    return azure_config


def test_upgrade_script_dryrun_s3(s3_storage: S3Bucket):
    ac = s3_storage.create_arctic()
    create_library_config(ac, LIB_NAME)

    # When
    run(uri=s3_storage.arctic_uri, run=False)

    # Then
    config = ac._library_manager.get_library_config(LIB_NAME)
    storage_config = _get_s3_storage_config(config)
    assert storage_config.bucket_name == s3_storage.bucket
    assert storage_config.credential_name == s3_storage.key.id
    assert storage_config.credential_key == s3_storage.key.secret


def test_upgrade_script_s3(s3_storage: S3Bucket):
    ac = s3_storage.create_arctic()
    create_library_config(ac, LIB_NAME)

    run(uri=s3_storage.arctic_uri, run=True)

    config = ac._library_manager.get_library_config(LIB_NAME)
    storage_config = _get_s3_storage_config(config)
    assert storage_config.bucket_name == ""
    assert storage_config.credential_name == ""
    assert storage_config.credential_key == ""


def test_upgrade_script_s3_rbac_ok(s3_storage: S3Bucket, monkeypatch):
    """Just _RBAC_ as creds is a placeholder. Leave config with that alone."""
    if os.name == "nt":
        if sys.version_info < (3, 9):
            pytest.skip("Older Python don't support unsetenv on Windows")

        # We statically linked msvcrt which maintains a seprate copy of the environment
        from arcticdb_ext.tools import putenv_s

        monkeypatch.setattr(os, "putenv", putenv_s)
        monkeypatch.setattr(os, "unsetenv", lambda n: putenv_s(n, ""))

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", s3_storage.key.id)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", s3_storage.key.secret)

    uri = s3_storage.replace_uri_field(s3_storage.arctic_uri, ArcticUriFields.USER, "aws_auth=true", start=1)
    uri = s3_storage.replace_uri_field(uri, ArcticUriFields.PASSWORD, "", start=1, end=3)
    s3_storage.arctic_uri = uri

    ac = s3_storage.create_arctic()  # create_arctic() does proper clean up
    create_library_config(ac, LIB_NAME)  # Note this avoids the override so _RBAC_ is actually written to storage
    ac[LIB_NAME].write_pickle("x", 123)

    run(uri=uri, run=True)

    ac = Arctic(uri)
    config = ac._library_manager.get_library_config(LIB_NAME)
    s3_storage = _get_s3_storage_config(config)
    assert s3_storage.bucket_name == s3_storage.bucket_name
    assert s3_storage.credential_name == USE_AWS_CRED_PROVIDERS_TOKEN
    assert s3_storage.credential_key == USE_AWS_CRED_PROVIDERS_TOKEN

    lib = ac[LIB_NAME]
    assert lib.read("x").data == 123


@AZURE_TESTS_MARK
def test_upgrade_script_dryrun_azure(azurite_storage: AzureContainer):
    # Given
    ac = azurite_storage.create_arctic()
    create_library_config(ac, LIB_NAME)

    # When
    run(uri=azurite_storage.arctic_uri, run=False)

    # Then
    config = ac._library_manager.get_library_config(LIB_NAME)
    azure_storage = _get_azure_storage_config(config)
    assert azure_storage.ca_cert_path == azurite_storage.factory.ca_cert_path
    assert azure_storage.container_name == azurite_storage.container
    assert azurite_storage.factory.account_name in azure_storage.endpoint
    assert azurite_storage.factory.account_key in azure_storage.endpoint
    assert azure_storage.max_connections == 0
    assert azure_storage.prefix.startswith(LIB_NAME)


@AZURE_TESTS_MARK
def test_upgrade_script_azure(azurite_storage: AzureContainer):
    # Given
    ac = azurite_storage.create_arctic()
    create_library_config(ac, LIB_NAME)

    run(uri=azurite_storage.arctic_uri, run=True)

    config = ac._library_manager.get_library_config(LIB_NAME)
    azure_storage = _get_azure_storage_config(config)
    assert azure_storage.ca_cert_path == ""
    assert azure_storage.container_name == ""
    assert azure_storage.endpoint == ""
    assert azure_storage.prefix.startswith(LIB_NAME)
