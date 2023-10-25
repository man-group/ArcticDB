import sys

import pytest
import pandas as pd

from arcticdb import Arctic
from arcticdb.config import MACOS_CONDA_BUILD, MACOS_CONDA_BUILD_SKIP_REASON
from arcticdb.scripts.update_storage import run
from arcticdb.options import LibraryOptions
from arcticc.pb2.s3_storage_pb2 import Config as S3Config
from arcticc.pb2.azure_storage_pb2 import Config as AzureConfig
from arcticdb.util.test import assert_frame_equal

from arcticdb.adapters.s3_library_adapter import USE_AWS_CRED_PROVIDERS_TOKEN

LIB_NAME = "test_lib"


def create_library_config(ac: Arctic, name: str):
    opts = LibraryOptions()
    library = ac._library_adapter.create_library(name, opts)
    ac._library_manager.write_library_config(library._lib_cfg, name, test_only_validation_toggle=False)


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


def test_upgrade_script_dryrun_s3(moto_s3_endpoint_and_credentials):
    # Given
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials
    uri = (
        endpoint.replace("http://", "s3://").rsplit(":", 1)[0]
        + ":"
        + bucket
        + "?access="
        + aws_access_key
        + "&secret="
        + aws_secret_key
        + "&port="
        + port
    )

    ac = Arctic(uri)
    create_library_config(ac, LIB_NAME)

    # When
    run(uri=uri, run=False)

    # Then
    config = ac._library_manager.get_library_config(LIB_NAME)
    s3_storage = _get_s3_storage_config(config)
    assert s3_storage.bucket_name == bucket
    assert s3_storage.credential_name == aws_access_key
    assert s3_storage.credential_key == aws_secret_key


def test_upgrade_script_s3(moto_s3_endpoint_and_credentials):
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials
    uri = (
        endpoint.replace("http://", "s3://").rsplit(":", 1)[0]
        + ":"
        + bucket
        + "?access="
        + aws_access_key
        + "&secret="
        + aws_secret_key
        + "&port="
        + port
    )

    ac = Arctic(uri)
    create_library_config(ac, LIB_NAME)

    run(uri=uri, run=True)

    config = ac._library_manager.get_library_config(LIB_NAME)
    s3_storage = _get_s3_storage_config(config)
    assert s3_storage.bucket_name == ""
    assert s3_storage.credential_name == ""
    assert s3_storage.credential_key == ""


def get_s3_aws_auth_uri_bucket_and_setup_server(moto_s3_endpoint_and_credentials, monkeypatch):
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", aws_access_key)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", aws_secret_key)
    return (
        endpoint.replace("http://", "s3://").rsplit(":", 1)[0] + ":" + bucket + "?aws_auth=true" + "&port=" + port,
        bucket,
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Test fixture issue with creds")
def test_upgrade_script_s3_rbac_ok(moto_s3_endpoint_and_credentials, monkeypatch):
    """Just _RBAC_ as creds is a placeholder. Leave config with that alone."""
    uri, bucket = get_s3_aws_auth_uri_bucket_and_setup_server(moto_s3_endpoint_and_credentials, monkeypatch)

    ac = Arctic(uri)
    create_library_config(ac, LIB_NAME)

    run(uri=uri, run=True)

    config = ac._library_manager.get_library_config(LIB_NAME)
    s3_storage = _get_s3_storage_config(config)
    assert s3_storage.bucket_name == bucket
    assert s3_storage.credential_name == USE_AWS_CRED_PROVIDERS_TOKEN
    assert s3_storage.credential_key == USE_AWS_CRED_PROVIDERS_TOKEN


@pytest.mark.skipif(sys.platform == "win32", reason="Test fixture issue with creds")
def test_aws_auth_with_storage_overrider(moto_s3_endpoint_and_credentials, monkeypatch):
    uri, _ = get_s3_aws_auth_uri_bucket_and_setup_server(moto_s3_endpoint_and_credentials, monkeypatch)
    expected = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    sym = "test"
    ac = Arctic(uri)
    ac.create_library(LIB_NAME)
    ac[LIB_NAME].write(sym, expected)

    ac = Arctic(uri)  # Force load lib config from storage, to check "storage-overridened" config
    assert_frame_equal(expected, ac[LIB_NAME].read(sym).data)


# Side effect needed from "_create_container" fixture
@pytest.mark.skipif(MACOS_CONDA_BUILD, reason=MACOS_CONDA_BUILD_SKIP_REASON)
def test_upgrade_script_dryrun_azure(
    azurite_azure_test_connection_setting, azurite_azure_uri, azure_client_and_create_container
):
    # Given
    (endpoint, container, credential_name, credential_key, ca_cert_path) = azurite_azure_test_connection_setting
    assert container
    ac = Arctic(azurite_azure_uri)
    create_library_config(ac, LIB_NAME)

    # When
    run(uri=azurite_azure_uri, run=False)

    # Then
    config = ac._library_manager.get_library_config(LIB_NAME)
    azure_storage = _get_azure_storage_config(config)
    assert azure_storage.ca_cert_path == ca_cert_path
    assert azure_storage.container_name == container
    assert credential_key in azure_storage.endpoint
    assert credential_name in azure_storage.endpoint
    assert azure_storage.max_connections == 0
    assert azure_storage.prefix.startswith(LIB_NAME)


# Side effect needed from "_create_container" fixture
@pytest.mark.skipif(MACOS_CONDA_BUILD, reason=MACOS_CONDA_BUILD_SKIP_REASON)
def test_upgrade_script_azure(
    azurite_azure_test_connection_setting, azurite_azure_uri, azure_client_and_create_container
):
    # Given
    (endpoint, container, credential_name, credential_key, ca_cert_path) = azurite_azure_test_connection_setting
    assert container
    ac = Arctic(azurite_azure_uri)

    create_library_config(ac, LIB_NAME)

    run(uri=azurite_azure_uri, run=True)

    config = ac._library_manager.get_library_config(LIB_NAME)
    azure_storage = _get_azure_storage_config(config)
    assert azure_storage.ca_cert_path == ""
    assert azure_storage.container_name == ""
    assert azure_storage.endpoint == ""
    assert azure_storage.prefix.startswith(LIB_NAME)
