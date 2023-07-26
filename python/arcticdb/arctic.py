"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from typing import List, Optional

from arcticdb.options import LibraryOptions
from arcticdb_ext.storage import LibraryManager, StorageOverride
from arcticdb.exceptions import LibraryNotFound
from arcticdb.version_store.library import Library
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.s3_library_adapter import S3LibraryAdapter
from arcticdb.adapters.lmdb_library_adapter import LMDBLibraryAdapter
from arcticdb.encoding_version import EncodingVersion
from arcticdb.adapters.azure_library_adapter import AzureLibraryAdapter


class Arctic:
    """
    Top-level library management class. Arctic instances can be configured against an S3 environment and enable the
    creation, deletion and retrieval of Arctic libraries.
    """

    _LIBRARY_ADAPTERS = [S3LibraryAdapter, LMDBLibraryAdapter, AzureLibraryAdapter]

    def __init__(self, uri: str, encoding_version: EncodingVersion = EncodingVersion.V1):
        """
        Initializes a top-level Arctic library management instance.

        For more information on how to use Arctic Library instances please see the documentation on Library.

        Parameters
        ----------

        uri: str
            URI specifying the backing store used to access, configure, and create Arctic libraries.

            S3
            --

                The S3 URI connection scheme has the form ``s3(s)://<s3 end point>:<s3 bucket>[?options]``.

                Use s3s as the protocol if communicating with a secure endpoint.

                Options is a query string that specifies connection specific options as ``<name>=<value>`` pairs joined with
                ``&``.

                Available options for S3:

                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | Option                    | Description                                                                                                                                                   |
                +===========================+===============================================================================================================================================================+
                | port                      | port to use for S3 connection                                                                                                                                 |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | region                    | S3 region                                                                                                                                                     |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | use_virtual_addressing    | Whether to use virtual addressing to access the S3 bucket                                                                                                     |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | access                    | S3 access key                                                                                                                                                 |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | secret                    | S3 secret access key                                                                                                                                          |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | path_prefix               | Path within S3 bucket to use for data storage                                                                                                                 |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | aws_auth                  | If true, authentication to endpoint will be computed via AWS environment vars/config files. If no options are provided `aws_auth` will be assumed to be true. |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | force_uri_lib_config      | Override the credentials and endpoint of an S3 storage with the URI of the Arctic object. Use if accessing a replicated (to different region/bucket) library. |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+

                Note: When connecting to AWS, `region` can be automatically deduced from the endpoint if the given endpoint
                specifies the region and `region` is not set.

            Azure
            -----

                The Azure URI connection scheme has the form ``azure://[options]``.
                It is based on the Azure Connection String, with additional options for configuring ArcticDB.
                Please refer to https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string for more details.

                ``options`` is a string that specifies connection specific options as ``<name>=<value>`` pairs joined with ``;`` (the final key value pair should not include a trailing ``;``).

                Additional options specific for ArcticDB:

                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | Option                    | Description                                                                                                                                                   |
                +===========================+===============================================================================================================================================================+
                | Container                 | Azure container for blobs                                                                                                                                     |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | Path_prefix               | Path within Azure container to use for data storage                                                                                                           |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | CA_cert_path              | Azure CA certificate path. If not set, default path will be used.                                                                                             |
                |                           | Note: For Linux distribution, default path is set to `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem`.                                                     |
                |                           | If the certificate cannot be found in the provided path, an Azure exception with no meaningful error code will be thrown.                                     |
                |                           | For more details, please see https://github.com/Azure/azure-sdk-for-cpp/issues/4738.                                                                          |
                |                           | For example, ``Failed to iterate azure blobs 'C' 0:``.                                                                                                        |
                |                           |                                                                                                                                                               |
                |                           | Default certificate path in various Linux distributions:                                                                                                      |
                |                           | "/etc/ssl/certs/ca-certificates.crt"                  Debian/Ubuntu/Gentoo etc.                                                                               |
                |                           | "/etc/pki/tls/certs/ca-bundle.crt"                    Fedora/RHEL 6                                                                                           |
                |                           | "/etc/ssl/ca-bundle.pem"                              OpenSUSE                                                                                                |
                |                           | "/etc/pki/tls/cacert.pem"                             OpenELEC                                                                                                |
                |                           | "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"   CentOS/RHEL 7                                                                                           |
                |                           | "/etc/ssl/cert.pem"                                   Alpine Linux                                                                                            |
                +---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+

                Note: Support for Azure Blob Storage is currently only available in *non-Conda* binaries distribution.

                Exception: Azure exceptions message always ends with `{AZURE_SDK_HTTP_STATUS_CODE}:{AZURE_SDK_REASON_PHRASE}`.

                Please refer to https://github.com/Azure/azure-sdk-for-cpp/blob/24ed290815d8f9dbcd758a60fdc5b6b9205f74e0/sdk/core/azure-core/inc/azure/core/http/http_status_code.hpp for
                more details of provided status codes.

                Note that due to a bug in Azure C++ SDK (https://github.com/Azure/azure-sdk-for-cpp/issues/4738), Azure may not give meaningful status codes and
                reason phrases in the exception. To debug these instances, please set the environment variable ``export AZURE_LOG_LEVEL`` to ``1`` to turn on the SDK debug logging.


            LMDB
            ----

                The LMDB URI connection scheme has the form ``lmdb:///<path to store LMDB files>``. There are no options
                available for the LMDB URI connection scheme.

        Examples
        --------

        >>> ac = Arctic('s3://MY_ENDPOINT:MY_BUCKET')  # Leave AWS to derive credential information
        >>> ac = Arctic('s3://MY_ENDPOINT:MY_BUCKET?region=YOUR_REGION&access=ABCD&secret=DCBA') # Manually specify creds
        >>> ac = Arctic('azure://CA_cert_path=/etc/ssl/certs/ca-certificates.crt;BlobEndpoint=https://arctic.blob.core.windows.net;Container=acblob;SharedAccessSignature=sp=sig')
        >>> ac.create_library('travel_data')
        >>> ac.list_libraries()
        ['travel_data']
        >>> travel_library = ac['travel_data']
        >>> ac.delete_library('travel_data')
        """
        _cls = None
        for adapter_cls in self._LIBRARY_ADAPTERS:
            if adapter_cls.supports_uri(uri):
                _cls = adapter_cls
                break
        else:
            raise ValueError(
                f"Invalid URI specified. Please see URI format specification for available formats. uri={uri}"
            )

        self._encoding_version = encoding_version
        self._library_adapter = _cls(uri, self._encoding_version)
        self._library_manager = LibraryManager(self._library_adapter.config_library)
        self._uri = uri
        self._open_libraries = dict()

    def __getitem__(self, name: str) -> Library:
        already_open = self._open_libraries.get(name)
        if already_open:
            return already_open

        if not self._library_manager.has_library(name):
            raise LibraryNotFound(name)

        storage_override = self._library_adapter.get_storage_override()
        lib = NativeVersionStore(
            self._library_manager.get_library(name, storage_override),
            repr(self._library_adapter),
            lib_cfg=self._library_manager.get_library_config(name, storage_override),
        )
        return Library(repr(self), lib)

    def __repr__(self):
        return "Arctic(config=%r)" % self._library_adapter

    def get_library(self, library: str) -> Library:
        """
        Returns the library named `library`.

        This method can also be invoked through subscripting. ``Arctic('bucket').get_library("test")`` is equivalent to
        ``Arctic('bucket')["test"]``.

        Examples
        --------
        >>> arctic = Arctic('s3://MY_ENDPOINT:MY_BUCKET')
        >>> arctic.create_library('test.library')
        >>> my_library = arctic.get_library('test.library')
        >>> my_library = arctic['test.library']

        Returns
        -------
        Library
        """
        return self[library]

    def create_library(self, name: str, library_options: Optional[LibraryOptions] = None) -> None:
        """
        Creates the library named ``name``.

        Arctic libraries contain named symbols which are the atomic unit of data storage within Arctic. Symbols
        contain data that in most cases strongly resembles a DataFrame and are versioned such that all modifying
        operations can be tracked and reverted.

        Arctic libraries support concurrent writes and reads to multiple symbols as well as concurrent reads to a single
        symbol. However, concurrent writers to a single symbol are not supported other than for primitives that
        explicitly state support for single-symbol concurrent writes.

        Parameters
        ----------
        name: str
            The name of the library that you wish to create.

        library_options: Optional[LibraryOptions]
            Options to use in configuring the library. Defaults if not provided are the same as are documented in LibraryOptions.

        Examples
        --------
        >>> arctic = Arctic('s3://MY_ENDPOINT:MY_BUCKET')
        >>> arctic.create_library('test.library')
        >>> my_library = arctic['test.library']
        """
        if name in self._open_libraries or self._library_manager.has_library(name):
            raise ValueError(f"Library [{name}] already exists.")

        if library_options is None:
            library_options = LibraryOptions()

        library = self._library_adapter.create_library(name, library_options)
        library.env = repr(self._library_adapter)
        lib = Library(repr(self), library)
        self._open_libraries[name] = lib
        self._library_manager.write_library_config(library._lib_cfg, name)

    def delete_library(self, name: str) -> None:
        """
        Removes the library called ``name``. This will remove the underlying data contained within the library and as
        such will take as much time as the underlying delete operations take.

        If no library with ``name`` exists then this is a no-op. In particular this method does not raise in this case.

        Parameters
        ----------
        name: `str`
            Name of the library to delete.
        """
        already_open = self._open_libraries.pop(name, None)
        if not already_open and not self._library_manager.has_library(name):
            return
        config = self._library_manager.get_library_config(name, StorageOverride())
        (already_open or self[name])._nvs.version_store.clear()
        del already_open  # essential to free resources held by the library
        try:
            self._library_adapter.cleanup_library(name, config)
        finally:
            self._library_manager.remove_library_config(name)

    def list_libraries(self) -> List[str]:
        """
        Lists all libraries available.

        Examples
        --------
        >>> arctic = Arctic('s3://MY_ENDPOINT:MY_BUCKET')
        >>> arctic.list_libraries()
        ['test.library']

        Returns
        -------
        A list of all library names that exist in this Arctic instance.
        """
        return self._library_manager.list_libraries()

    def get_uri(self) -> str:
        """
        Returns the URI that was used to create the Arctic instance.

        Examples
        --------
        >>> arctic = Arctic('s3://MY_ENDPOINT:MY_BUCKET')
        >>> arctic.get_uri()

        Returns
        -------
        s3://MY_ENDPOINT:MY_BUCKET
        """
        return self._uri
