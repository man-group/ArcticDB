"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import logging
from typing import List, Optional, Any, Union

from arcticdb.options import DEFAULT_ENCODING_VERSION, LibraryOptions, EnterpriseLibraryOptions
from arcticdb_ext.storage import LibraryManager
from arcticdb.exceptions import LibraryNotFound, MismatchingLibraryOptions
from arcticdb.version_store.library import ArcticInvalidApiUsageException, Library
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter
from arcticdb.adapters.s3_library_adapter import S3LibraryAdapter
from arcticdb.adapters.lmdb_library_adapter import LMDBLibraryAdapter
from arcticdb.adapters.azure_library_adapter import AzureLibraryAdapter
from arcticdb.adapters.mongo_library_adapter import MongoLibraryAdapter
from arcticdb.adapters.in_memory_library_adapter import InMemoryLibraryAdapter
from arcticdb.adapters.gcpxml_library_adapter import GCPXMLLibraryAdapter
from arcticdb.encoding_version import EncodingVersion
from arcticdb.options import ModifiableEnterpriseLibraryOption, ModifiableLibraryOption


logger = logging.getLogger(__name__)


class Arctic:
    """
    Top-level library management class. Arctic instances can be configured against an S3 environment and enable the
    creation, deletion and retrieval of Arctic libraries.
    """

    _LIBRARY_ADAPTERS = [
        S3LibraryAdapter,
        GCPXMLLibraryAdapter,
        LMDBLibraryAdapter,
        AzureLibraryAdapter,
        MongoLibraryAdapter,
        InMemoryLibraryAdapter,
    ]

    # For test fixture clean up
    _created_lib_names: Optional[List[str]] = None
    _accessed_libs: Optional[List[NativeVersionStore]] = None

    def __init__(self, uri: str, encoding_version: EncodingVersion = DEFAULT_ENCODING_VERSION):
        """
        Initializes a top-level Arctic library management instance.

        For more information on how to use Arctic Library instances please see the documentation on Library.

        Parameters
        ----------

        uri: str
            URI specifying the backing store used to access, configure, and create Arctic libraries.
            For more details about the parameters, please refer to the [Arctic URI Documentation](./arctic_uri.md).

        encoding_version: EncodingVersion, default DEFAULT_ENCODING_VERSION
            When creating new libraries with this Arctic instance, the default encoding version to use.
            Can be overridden by specifying the encoding version in the LibraryOptions argument to create_library.

        Examples
        --------

        >>> ac = adb.Arctic('s3://MY_ENDPOINT:MY_BUCKET')  # Leave AWS to derive credential information
        >>> ac = adb.Arctic('s3://MY_ENDPOINT:MY_BUCKET?region=YOUR_REGION&access=ABCD&secret=DCBA') # Manually specify creds
        >>> ac = adb.Arctic('azure://CA_cert_path=/etc/ssl/certs/ca-certificates.crt;BlobEndpoint=https://arctic.blob.core.windows.net;Container=acblob;SharedAccessSignature=sp=sig')
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
        self._library_adapter: ArcticLibraryAdapter = _cls(uri, self._encoding_version)
        self._library_manager = LibraryManager(self._library_adapter.config_library)
        self._uri = uri

    def __getitem__(self, name: str) -> Library:
        lib_mgr_name = self._library_adapter.get_name_for_library_manager(name)
        if not self._library_manager.has_library(lib_mgr_name):
            raise LibraryNotFound(name)

        storage_override = self._library_adapter.get_storage_override()
        lib = NativeVersionStore(
            self._library_manager.get_library(lib_mgr_name, storage_override, native_storage_config=self._library_adapter.native_config()),
            repr(self._library_adapter),
            lib_cfg=self._library_manager.get_library_config(lib_mgr_name, storage_override),
            native_cfg=self._library_adapter.native_config()
        )
        if self._accessed_libs is not None:
            self._accessed_libs.append(lib)
        return Library(repr(self), lib)

    def __repr__(self):
        return "Arctic(config=%r)" % self._library_adapter

    def __contains__(self, name: str):
        return self.has_library(name)

    def get_library(
        self, name: str, create_if_missing: Optional[bool] = False, library_options: Optional[LibraryOptions] = None
    ) -> Library:
        """
        Returns the library named ``name``.

        This method can also be invoked through subscripting. ``adb.Arctic('bucket').get_library("test")`` is equivalent to
        ``adb.Arctic('bucket')["test"]``.

        Parameters
        ----------
        name: str
            The name of the library that you wish to retrieve.

        create_if_missing: Optional[bool], default = False
            If True, and the library does not exist, then create it.

        library_options: Optional[LibraryOptions], default = None
            If create_if_missing is True, and the library does not already exist, then it will be created with these
            options, or the defaults if not provided.
            If create_if_missing is True, and the library already exists, ensures that the existing library options
            match these.
            Unused if create_if_missing is False.

        Examples
        --------
        >>> arctic = adb.Arctic('s3://MY_ENDPOINT:MY_BUCKET')
        >>> arctic.create_library('test.library')
        >>> my_library = arctic.get_library('test.library')
        >>> my_library = arctic['test.library']

        Returns
        -------
        Library
        """
        if library_options and not create_if_missing:
            raise ArcticInvalidApiUsageException(
                "In get_library, library_options must be falsey if create_if_missing is falsey"
            )
        try:
            lib = self[name]
            if create_if_missing and library_options:
                if library_options.encoding_version is None:
                    library_options.encoding_version = self._encoding_version
                if lib.options() != library_options:
                    raise MismatchingLibraryOptions(f"{name}: {lib.options()} != {library_options}")
            return lib
        except LibraryNotFound as e:
            if create_if_missing:
                return self.create_library(name, library_options)
            else:
                raise e

    def create_library(self,
                       name: str,
                       library_options: Optional[LibraryOptions] = None,
                       enterprise_library_options: Optional[EnterpriseLibraryOptions] = None) -> Library:
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
            Options to use in configuring the library. Defaults if not provided are the same as documented in LibraryOptions.

        enterprise_library_options: Optional[EnterpriseLibraryOptions]
            Enterprise options to use in configuring the library. Defaults if not provided are the same as documented in
            EnterpriseLibraryOptions. These options are only relevant to ArcticDB enterprise users.

        Examples
        --------
        >>> arctic = adb.Arctic('s3://MY_ENDPOINT:MY_BUCKET')
        >>> arctic.create_library('test.library')
        >>> my_library = arctic['test.library']

        Returns
        -------
        Library that was just created
        """
        if self.has_library(name):
            raise ValueError(f"Library [{name}] already exists.")

        if library_options is None:
            library_options = LibraryOptions()

        if enterprise_library_options is None:
            enterprise_library_options = EnterpriseLibraryOptions()

        cfg = self._library_adapter.get_library_config(name, library_options, enterprise_library_options)
        lib_mgr_name = self._library_adapter.get_name_for_library_manager(name)
        self._library_manager.write_library_config(cfg, lib_mgr_name, self._library_adapter.get_masking_override())
        if self._created_lib_names is not None:
            self._created_lib_names.append(name)
        return self.get_library(name)

    def delete_library(self, name: str) -> None:
        """
        Removes the library called ``name``. This will remove the underlying data contained within the library and as
        such will take as much time as the underlying delete operations take.

        If no library with ``name`` exists then this is a no-op. In particular this method does not raise in this case.

        Parameters
        ----------
        name: str
            Name of the library to delete.
        """
        try:
            lib = self[name]
        except LibraryNotFound:
            return
        lib._nvs.version_store.clear()
        lib_mgr_name = self._library_adapter.get_name_for_library_manager(name)
        self._library_manager.cleanup_library_if_open(lib_mgr_name)
        self._library_manager.remove_library_config(lib_mgr_name)

        if self._created_lib_names and name in self._created_lib_names:
            self._created_lib_names.remove(name)

    def has_library(self, name: str) -> bool:
        """
        Query if the given library exists

        Parameters
        ----------
        name: str
            Name of the library to check the existence of.

        Returns
        -------
        True if the library exists, False otherwise.
        """
        return self._library_manager.has_library(self._library_adapter.get_name_for_library_manager(name))

    def list_libraries(self) -> List[str]:
        """
        Lists all libraries available.

        Examples
        --------
        >>> arctic = adb.Arctic('s3://MY_ENDPOINT:MY_BUCKET')
        >>> arctic.list_libraries()
        ['test.library']

        Returns
        -------
        A list of all library names that exist in this Arctic instance.
        """
        raw_libs = self._library_manager.list_libraries()
        return self._library_adapter.library_manager_names_to_user_facing(raw_libs)

    def get_uri(self) -> str:
        """
        Returns the URI that was used to create the Arctic instance.

        Examples
        --------
        >>> arctic = adb.Arctic('s3://MY_ENDPOINT:MY_BUCKET')
        >>> arctic.get_uri()

        Returns
        -------
        s3://MY_ENDPOINT:MY_BUCKET
        """
        return self._uri

    def modify_library_option(self,
                              library: Library,
                              option: Union[ModifiableLibraryOption, ModifiableEnterpriseLibraryOption],
                              option_value: Any):
        """
        Modify an option for a library.

        See `LibraryOptions` and `EnterpriseLibraryOptions` for descriptions of the meanings of the various options.

        After the modification, this process and other processes that open the library will use the new value. Processes
        that already have the library open will not see the configuration change until they restart.

        Parameters
        ----------
        library
            The library to modify.

        option
            The library option to change.

        option_value
            The new setting for the library option.
        """

        self._library_manager.modify_library_option(library.name, option, option_value)

        lib_mgr_name = self._library_adapter.get_name_for_library_manager(library.name)
        storage_override = self._library_adapter.get_storage_override()
        new_cfg = self._library_manager.get_library_config(lib_mgr_name, storage_override)
        library._nvs._initialize(
            self._library_manager.get_library(lib_mgr_name, storage_override, ignore_cache=True, native_storage_config=self._library_adapter.native_config()),
            library._nvs.env,
            new_cfg,
            library._nvs._custom_normalizer,
            library._nvs._open_mode
        )

        logger.info(f"Set option=[{option}] to value=[{option_value}] for Arctic=[{self}] Library=[{library}]")
