# Copyright 2023 Man Group Operations Limited
#
# Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
#
# As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.

import os
import platform
import pytest
import ssl
import uuid
import pandas as pd

from arcticdb.storage_fixtures.azure import AzureContainer
from arcticdb.arctic import Arctic
from arcticc.pb2.azure_storage_pb2 import Config as AzureConfig
from arcticdb.util.test import assert_frame_equal
from arcticdb.scripts.update_storage import run
from tests.util.mark import AZURE_TESTS_MARK, SSL_TESTS_MARK, SSL_TEST_SUPPORTED
from tests.scripts.test_update_storage import create_library_config
from tests.integration.arcticdb.test_arctic import (
    ParameterDisplayStatus,
    parameter_display_status,
    DefaultSetting,
    edit_connection_string,
)


def _get_azure_storage_config(cfg):
    """Helper function to extract Azure storage config from library config."""
    primary_storage_name = cfg.lib_desc.storage_ids[0]
    primary_any = cfg.storage_by_id[primary_storage_name]
    azure_config = AzureConfig()
    primary_any.config.Unpack(azure_config)
    return azure_config


def test_azure_transport_selection(azurite_storage: AzureContainer):
    """Test that the correct transport is selected based on the platform."""
    # Get the current platform
    system = platform.system().lower()

    # Create Arctic instance with CA cert settings
    ca_cert_path = "/path/to/cert.pem"
    ca_cert_dir = "/path/to/certs"

    # Modify the URI to include CA cert settings
    uri = azurite_storage.arctic_uri
    uri += f";CA_cert_path={ca_cert_path};CA_cert_dir={ca_cert_dir}"

    if system == "windows" or system == "darwin":
        # On Windows and macOS, CA cert settings should raise an exception
        with pytest.raises(ValueError) as exc_info:
            # Create a new Arctic instance with the modified URI
            Arctic(uri)
        assert "You have provided `ca_cert_path` or `ca_cert_dir` in the URI which is only supported on Linux." in str(
            exc_info.value
        )
    else:
        # On Linux, CA cert settings should be accepted
        ac = Arctic(uri)
        # Verify that the adapter correctly extracted the CA cert values from the URI
        assert ac._library_adapter._ca_cert_path == ca_cert_path
        assert ac._library_adapter._ca_cert_dir == ca_cert_dir


def test_azure_transport_default_settings(azurite_storage: AzureContainer):
    """Test that the default transport settings work correctly."""
    # Create Arctic instance with default settings
    ac = azurite_storage.create_arctic()

    # Create a library first to get the config
    lib_name = f"test_lib_{uuid.uuid4().hex[:8]}"
    lib = ac.create_library(lib_name)
    config = ac._library_manager.get_library_config(lib_name)
    azure_storage = _get_azure_storage_config(config)

    # Verify that the transport is created successfully
    assert azure_storage is not None

    # Perform basic operations to verify transport works
    lib.write("test_symbol", pd.DataFrame({"a": [1, 2, 3]}))
    assert_frame_equal(lib.read("test_symbol").data, pd.DataFrame({"a": [1, 2, 3]}))


@pytest.mark.skipif(platform.system().lower() != "linux", reason="Test only runs on Linux")
def test_azure_transport_linux_ca_cert_path(azurite_storage: AzureContainer):
    """Test that CA cert path is properly handled on Linux."""
    # Create a temporary CA cert file
    ca_cert_path = "/tmp/test_ca.pem"
    with open(ca_cert_path, "w") as f:
        f.write("-----BEGIN CERTIFICATE-----\nMOCK CERTIFICATE\n-----END CERTIFICATE-----")

    try:
        # Create Arctic instance with CA cert path
        uri = azurite_storage.arctic_uri + f";CA_cert_path={ca_cert_path}"
        ac = Arctic(uri)

        # Verify that the adapter correctly extracted the CA cert path from the URI
        assert ac._library_adapter._ca_cert_path == ca_cert_path

        # Create a library to verify it works
        lib_name = f"test_lib_{uuid.uuid4().hex[:8]}"
        lib = ac.create_library(lib_name)

        # Perform basic operations to verify transport works
        lib.write("test_symbol", pd.DataFrame({"a": [1, 2, 3]}))
        assert_frame_equal(lib.read("test_symbol").data, pd.DataFrame({"a": [1, 2, 3]}))
    finally:
        # Clean up the temporary CA cert file
        if os.path.exists(ca_cert_path):
            os.remove(ca_cert_path)


@pytest.mark.skipif(platform.system().lower() != "linux", reason="Test only runs on Linux")
def test_azure_transport_linux_ca_cert_dir(azurite_storage: AzureContainer):
    """Test that CA cert directory is properly handled on Linux."""
    # Create a temporary CA cert directory
    ca_cert_dir = "/tmp/test_certs"
    os.makedirs(ca_cert_dir, exist_ok=True)

    try:
        # Create Arctic instance with CA cert directory
        uri = azurite_storage.arctic_uri + f";CA_cert_dir={ca_cert_dir}"
        ac = Arctic(uri)

        # Verify that the adapter correctly extracted the CA cert directory from the URI
        assert ac._library_adapter._ca_cert_dir == ca_cert_dir

        # Create a library to verify it works
        lib_name = f"test_lib_{uuid.uuid4().hex[:8]}"
        lib = ac.create_library(lib_name)

        # Perform basic operations to verify transport works
        lib.write("test_symbol", pd.DataFrame({"a": [1, 2, 3]}))
        assert_frame_equal(lib.read("test_symbol").data, pd.DataFrame({"a": [1, 2, 3]}))
    finally:
        # Clean up the temporary CA cert directory
        if os.path.exists(ca_cert_dir):
            os.rmdir(ca_cert_dir)


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
@SSL_TESTS_MARK
@pytest.mark.parametrize("client_cert_file", parameter_display_status)
@pytest.mark.parametrize("client_cert_dir", parameter_display_status)
def test_azurite_ssl_verification(azurite_ssl_storage, monkeypatch, client_cert_file, client_cert_dir, lib_name):
    storage = azurite_ssl_storage
    # Leaving ca file and ca dir unset will fallback to using os default setting,
    # which is different from the test environment
    default_setting = DefaultSetting(storage.factory)
    monkeypatch.setattr("ssl.get_default_verify_paths", lambda: default_setting)
    uri = edit_connection_string(storage.arctic_uri, ";", storage, None, client_cert_file, client_cert_dir)
    ac = Arctic(uri)
    try:
        lib = ac.create_library(lib_name)
        lib.write("sym", pd.DataFrame())
    finally:
        ac.delete_library(lib_name)
