"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from typing import List, Optional

from arcticdb.options import LibraryOptions
from arcticdb_ext.storage import LibraryManager
from arcticdb.version_store.library import Library
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.adapters.s3_library_adapter import S3LibraryAdapter
from arcticdb.adapters.lmdb_library_adapter import LMDBLibraryAdapter


class Arctic:
    """
    Top-level library management class. Arctic instances can be configured against an S3 environment and enable the
    creation, deletion and retrieval of Arctic libraries.
    """

    _LIBRARY_ADAPTERS = [S3LibraryAdapter, LMDBLibraryAdapter]

    def __init__(self, uri: str):
        """
        Initializes a top-level Arctic library management instance.

        For more information on how to use Arctic Library instances please see the documentation on Library.

        Parameters
        ----------

        uri: str
            URI specifying the backing store used to access, configure, and create Arctic libraries.

            The S3 URI connection scheme has the form ``s3(s)://<s3 end point>:<s3 bucket>[?options]``.

            Use s3s as the protocol if communicating with a secure endpoint.

            Options is a query string that specifies connection specific options as ``<name>=<value>`` pairs joined with
            ``&``.

            Available options:

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

            Note: When connecting to AWS, `region` can be automatically deduced from the endpoint if the given endpoint
            specifies the region and `region` is not set.

            The LMDB URI connection scheme has the form ``lmdb:///<path to store LMDB files>``.

        Examples
        --------

        >>> ac = Arctic('s3://MY_ENDPOINT:MY_BUCKET')  # Leave AWS to derive credential information
        >>> ac = Arctic('s3://MY_ENDPOINT:MY_BUCKET?region=YOUR_REGION&access=ABCD&secret=DCBA') # Manually specify creds
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

        self._library_adapter = _cls(uri)
        self._library_manager = LibraryManager(self._library_adapter.config_library)

    def __getitem__(self, name: str) -> Library:
        lib = NativeVersionStore(
            self._library_manager.get_library(name),
            repr(self._library_adapter),
            lib_cfg=self._library_manager.get_library_config(name),
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
        if self._library_manager.has_library(name):
            raise ValueError(f"{name} already exists as a library. Please delete prior to re-creating.")

        if library_options is None:
            library_options = LibraryOptions()

        library_config = self._library_adapter.create_library_config(name, library_options)
        self._library_adapter.initialize_library(name, library_config)
        self._library_manager.write_library_config(library_config, name)

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
        if not self._library_manager.has_library(name):
            return
        self._library_adapter.delete_library(self[name], self._library_manager.get_library_config(name))
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
