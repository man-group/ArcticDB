# Copyright 2023 Man Group Operations Limited
#
# Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
#
# As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.

import os
import platform
import pytest
from arcticdb.storage_fixtures.azure import AzureContainer
from arcticdb.exceptions import ArcticException

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
    
    if system == "windows":
        # On Windows, CA cert settings should raise an exception
        with pytest.raises(ArcticException) as exc_info:
            azurite_storage.create_arctic(uri=uri)
        assert "CA certificate settings are not supported on Windows" in str(exc_info.value)
        
    elif system == "darwin":
        # On macOS, CA cert settings should raise an exception
        with pytest.raises(ArcticException) as exc_info:
            azurite_storage.create_arctic(uri=uri)
        assert "CA certificate settings are not supported on macOS" in str(exc_info.value)
        
    else:
        # On Linux, CA cert settings should be accepted
        ac = azurite_storage.create_arctic(uri=uri)
        config = ac._library_manager.get_library_config("test_lib")
        azure_storage = config.storage.azure
        assert azure_storage.ca_cert_path == ca_cert_path
        assert azure_storage.ca_cert_dir == ca_cert_dir

def test_azure_transport_default_settings(azurite_storage: AzureContainer):
    """Test that the default transport settings work correctly."""
    # Create Arctic instance with default settings
    ac = azurite_storage.create_arctic()
    config = ac._library_manager.get_library_config("test_lib")
    azure_storage = config.storage.azure
    
    # Verify that the transport is created successfully
    assert azure_storage is not None
    
    # Create a library and perform basic operations to verify transport works
    lib = ac.create_library("test_lib")
    lib.write("test_symbol", "test_data")
    assert lib.read("test_symbol") == "test_data"

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
        ac = azurite_storage.create_arctic(uri=uri)
        config = ac._library_manager.get_library_config("test_lib")
        azure_storage = config.storage.azure
        
        # Verify that the CA cert path is set correctly
        assert azure_storage.ca_cert_path == ca_cert_path
        
        # Create a library and perform basic operations to verify transport works
        lib = ac.create_library("test_lib")
        lib.write("test_symbol", "test_data")
        assert lib.read("test_symbol") == "test_data"
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
        ac = azurite_storage.create_arctic(uri=uri)
        config = ac._library_manager.get_library_config("test_lib")
        azure_storage = config.storage.azure
        
        # Verify that the CA cert directory is set correctly
        assert azure_storage.ca_cert_dir == ca_cert_dir
        
        # Create a library and perform basic operations to verify transport works
        lib = ac.create_library("test_lib")
        lib.write("test_symbol", "test_data")
        assert lib.read("test_symbol") == "test_data"
    finally:
        # Clean up the temporary CA cert directory
        if os.path.exists(ca_cert_dir):
            os.rmdir(ca_cert_dir) 