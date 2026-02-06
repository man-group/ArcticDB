"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import logging
from re import L
from typing import TYPE_CHECKING, List, Optional, Any, Union

if TYPE_CHECKING:
    from arcticdb.version_store.duckdb import ArcticDuckDBContext

from arcticdb.options import (
    DEFAULT_ENCODING_VERSION,
    LibraryOptions,
    EnterpriseLibraryOptions,
    RuntimeOptions,
    OutputFormat,
    ArrowOutputStringFormat,
)
from arcticdb.dependencies import pyarrow as pa
from arcticdb_ext.storage import LibraryManager
from arcticdb.exceptions import LibraryNotFound, MismatchingLibraryOptions, KeyNotFoundException
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

    # This is a hack to allow the tests to access the libs_instances_from_arctic list
    # It is set by the LmdbStorageFixture
    _accessed_libs: Optional[List[NativeVersionStore]] = None

    def __init__(
        self,
        uri: str,
        encoding_version: EncodingVersion = DEFAULT_ENCODING_VERSION,
        output_format: Union[OutputFormat, str] = OutputFormat.PANDAS,
        arrow_string_format_default: Union[
            ArrowOutputStringFormat, "pa.DataType"
        ] = ArrowOutputStringFormat.LARGE_STRING,
    ):
        """
        Initializes a top-level Arctic library management instance.

        For more information on how to use Arctic Library instances please see the documentation on Library.

        Parameters
        ----------

        uri: str
            URI specifying the backing store used to access, configure, and create Arctic libraries.
            For more details about the parameters, please refer to the [Arctic URI Documentation](./arctic_uri.md).

        encoding_version: EncodingVersion, default = EncodingVersion.V1
            When creating new libraries with this Arctic instance, the default encoding version to use.
            Can be overridden by specifying the encoding version in the LibraryOptions argument to create_library.

        output_format: Union[OutputFormat, str], default = OutputFormat.PANDAS
            Default output format for all read operations on libraries created from this `Arctic` instance.
            Can be overridden per library or per read operation.
            See `OutputFormat` documentation for details on available formats.

        arrow_string_format_default: Union[ArrowOutputStringFormat, "pa.DataType"], default = ArrowOutputStringFormat.LARGE_STRING
            Default string column format when using `PYARROW` or `POLARS` output formats.
            Can be overridden per library or per read operation.
            See `ArrowOutputStringFormat` documentation for details on available string formats.

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
        self._runtime_options = RuntimeOptions(
            output_format=output_format, arrow_string_format_default=arrow_string_format_default
        )

    def _get_library(
        self,
        name: str,
        output_format: Optional[Union[OutputFormat, str]] = None,
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None,
    ) -> Library:
        lib_mgr_name = self._library_adapter.get_name_for_library_manager(name)

        storage_override = self._library_adapter.get_storage_override()

        runtime_options = self._runtime_options
        if output_format is not None:
            runtime_options.set_output_format(output_format=output_format)
        if arrow_string_format_default is not None:
            runtime_options.set_arrow_string_format_default(arrow_string_format_default=arrow_string_format_default)

        try:
            lib_with_config = self._library_manager.get_library(
                lib_mgr_name, storage_override, native_storage_config=self._library_adapter.native_config()
            )
            lib = NativeVersionStore(
                lib_with_config.library,
                repr(self._library_adapter),
                lib_cfg=lib_with_config.config,
                native_cfg=self._library_adapter.native_config(),
                runtime_options=runtime_options,
            )
        except KeyNotFoundException as ex:
            raise LibraryNotFound(name) from ex

        if self._accessed_libs is not None:
            self._accessed_libs.append(lib)
        return Library(repr(self), lib)

    def __getitem__(self, name: str):
        return self._get_library(name)

    def __repr__(self):
        return "Arctic(config=%r)" % self._library_adapter

    def __contains__(self, name: str):
        return self.has_library(name)

    def get_library(
        self,
        name: str,
        create_if_missing: Optional[bool] = False,
        library_options: Optional[LibraryOptions] = None,
        output_format: Optional[Union[OutputFormat, str]] = None,
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None,
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

        output_format: Optional[Union[OutputFormat, str]], default = None
            Default output format for all read operations on this library.
            If `None`, uses the output format from the `Arctic` instance.
            Can be overridden per read operation.
            See `OutputFormat` documentation for details on available formats.

        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]], default=None
            Default string column format when using `PYARROW` or `POLARS` output formats on this library.
            If `None`, uses the `arrow_string_format_default` from the `Arctic` instance.
            Can be overridden per read operation.
            See `ArrowOutputStringFormat` documentation for details on available string formats.

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
            lib = self._get_library(
                name, output_format=output_format, arrow_string_format_default=arrow_string_format_default
            )
            if create_if_missing and library_options:
                if library_options.encoding_version is None:
                    library_options.encoding_version = self._encoding_version
                if lib.options() != library_options:
                    raise MismatchingLibraryOptions(f"{name}: {lib.options()} != {library_options}")
            return lib
        except LibraryNotFound as e:
            if create_if_missing:
                return self.create_library(name, library_options, output_format, arrow_string_format_default)
            else:
                raise e

    def create_library(
        self,
        name: str,
        library_options: Optional[LibraryOptions] = None,
        enterprise_library_options: Optional[EnterpriseLibraryOptions] = None,
        output_format: Optional[Union[OutputFormat, str]] = None,
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None,
    ) -> Library:
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

        output_format: Optional[Union[OutputFormat, str]], default = None
            Default output format for all read operations on this library.
            If `None`, uses the output format from the `Arctic` instance.
            Can be overridden per read operation.
            See `OutputFormat` documentation for details on available formats.

        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]], default=None
            Default string column format when using `PYARROW` or `POLARS` output formats on this library.
            If `None`, uses the `arrow_string_format_default` from the `Arctic` instance.
            Can be overridden per read operation.
            See `ArrowOutputStringFormat` documentation for details on available string formats.
            Note that this setting is only applied to the runtime `Library` instance and is not stored as part of the library configuration.

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
        return self.get_library(
            name, output_format=output_format, arrow_string_format_default=arrow_string_format_default
        )

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

    def modify_library_option(
        self,
        library: Library,
        option: Union[ModifiableLibraryOption, ModifiableEnterpriseLibraryOption],
        option_value: Any,
    ):
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
        lib_with_config = self._library_manager.get_library(
            lib_mgr_name,
            storage_override,
            ignore_cache=True,
            native_storage_config=self._library_adapter.native_config(),
        )
        library._nvs._initialize(
            lib_with_config.library,
            library._nvs.env,
            lib_with_config.config,
            library._nvs._custom_normalizer,
            library._nvs._open_mode,
        )

        logger.info(f"Set option=[{option}] to value=[{option_value}] for Arctic=[{self}] Library=[{library}]")

    def sql(
        self,
        query: str,
        output_format: Optional[Union[OutputFormat, str]] = None,
    ):
        """
        Execute a SQL database discovery query on this Arctic instance.

        ArcticDB uses ``database.library`` naming convention where database is
        the permissioning unit (typically one per user). Top-level libraries
        without a database prefix are grouped under ``__default__``.

        Parameters
        ----------
        query : str
            SQL query to execute. Currently only ``SHOW DATABASES`` is supported,
            which returns databases with library counts.

        output_format : OutputFormat or str, optional
            Output format for the result:
            - ``"pandas"`` (default): Returns a pandas DataFrame
            - ``"pyarrow"``: Returns a PyArrow Table
            - ``"polars"``: Returns a Polars DataFrame

        Returns
        -------
        pandas.DataFrame, pyarrow.Table, or polars.DataFrame
            Query result in the requested format.

        Raises
        ------
        ValueError
            If the query is not a supported database discovery query.

        Examples
        --------
        List all databases with library counts:

        >>> arctic = adb.Arctic('lmdb://mydata')
        >>> arctic.create_library('jblackburn.market_data')
        >>> arctic.create_library('jblackburn.reference_data')
        >>> arctic.create_library('global_config')
        >>> result = arctic.sql("SHOW DATABASES")
        >>> print(result)
           database_name  library_count
        0     jblackburn              2
        1     __default__              1

        See Also
        --------
        duckdb : Context manager for complex cross-library queries.
        Library.sql : SQL queries on individual libraries.
        """
        from arcticdb.version_store.duckdb.duckdb import _check_duckdb_available, _parse_library_name
        from arcticdb.version_store.duckdb.pushdown import is_database_discovery_query

        _check_duckdb_available()

        # Check for SHOW DATABASES
        if not is_database_discovery_query(query):
            raise ValueError(
                "Arctic.sql() only supports SHOW DATABASES. "
                "For data queries, use library.sql() or arctic.duckdb() context manager."
            )

        # Get list of libraries and group by database
        libraries = self.list_libraries()

        from collections import defaultdict

        database_counts = defaultdict(int)
        for lib_name in libraries:
            database, _ = _parse_library_name(lib_name)
            database_counts[database] += 1

        # Build result table
        import pyarrow as pa

        arrow_table = pa.table(
            {
                "database_name": list(database_counts.keys()),
                "library_count": list(database_counts.values()),
            }
        )

        return self._format_query_result(arrow_table, output_format)

    def _format_query_result(
        self,
        arrow_table,
        output_format: Optional[Union[OutputFormat, str]] = None,
    ):
        """Convert Arrow table to requested output format."""
        if output_format is None:
            output_fmt_str = OutputFormat.PANDAS.lower()
        elif isinstance(output_format, OutputFormat):
            output_fmt_str = output_format.lower()
        else:
            output_fmt_str = str(output_format).lower()

        if output_fmt_str == OutputFormat.PYARROW.lower():
            return arrow_table
        elif output_fmt_str == OutputFormat.POLARS.lower():
            import polars as pl

            return pl.from_arrow(arrow_table)
        else:
            # Default to pandas
            return arrow_table.to_pandas()

    def duckdb(self, connection: Any = None) -> "ArcticDuckDBContext":
        """
        Create a DuckDB context for cross-library SQL queries.

        The context manager allows explicit library and symbol registration,
        enabling data discovery queries like SHOW DATABASES and queries
        that span multiple libraries.

        Parameters
        ----------
        connection : duckdb.DuckDBPyConnection, optional
            External DuckDB connection to use. If provided, ArcticDB will register
            symbols into this connection but will NOT close it when the context exits.
            This allows joining ArcticDB data with data from other sources.
            If not provided, a new in-memory connection is created and closed on exit.

        Returns
        -------
        ArcticDuckDBContext
            Context manager for DuckDB queries.

        Examples
        --------
        Basic SHOW DATABASES:

        >>> with arctic.duckdb() as ddb:
        ...     ddb.register_all_libraries()
        ...     databases = ddb.query("SHOW DATABASES")

        Cross-library queries:

        >>> with arctic.duckdb() as ddb:
        ...     ddb.register_symbol("market_data", "prices")
        ...     ddb.register_symbol("reference_data", "securities", alias="ref")
        ...     result = ddb.query('''
        ...         SELECT p.ticker, r.name, p.price
        ...         FROM prices p
        ...         JOIN ref r ON p.ticker = r.ticker
        ...     ''')

        See Also
        --------
        sql : Simple SQL queries for database discovery.
        Library.duckdb : Context manager for single-library queries.
        """
        from arcticdb.version_store.duckdb import ArcticDuckDBContext

        return ArcticDuckDBContext(self, connection=connection)

    def duckdb_register(
        self,
        conn,
        libraries: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Register ArcticDB symbols from one or more libraries into a DuckDB connection.

        Symbols are materialized as Arrow tables and registered with the DuckDB
        connection. Table names are prefixed with the library name to avoid
        collisions: ``library_name__symbol``.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection to register tables into.
        libraries : list of str, optional
            Library names to register. If None, registers all libraries.

        Returns
        -------
        list of str
            Names of registered tables (in ``library__symbol`` format).

        Examples
        --------
        Register all libraries:

        >>> import duckdb
        >>> conn = duckdb.connect()
        >>> arctic.duckdb_register(conn)
        ['market_data__trades', 'market_data__prices', 'reference_data__securities']

        Register specific libraries:

        >>> arctic.duckdb_register(conn, libraries=["market_data"])
        >>> conn.sql("SELECT * FROM market_data__trades LIMIT 10").df()

        Cross-library JOIN:

        >>> arctic.duckdb_register(conn, libraries=["market_data", "reference_data"])
        >>> conn.sql('''
        ...     SELECT t.ticker, s.name, t.price
        ...     FROM market_data__trades t
        ...     JOIN reference_data__securities s ON t.ticker = s.ticker
        ... ''').df()

        See Also
        --------
        Library.duckdb_register : Register symbols from a single library.
        duckdb : Context manager for streaming cross-library queries.
        """
        import pyarrow as pa_mod

        from arcticdb.version_store.duckdb.duckdb import _check_duckdb_available, _BaseDuckDBContext

        _check_duckdb_available()
        _BaseDuckDBContext._validate_external_connection(conn)

        if libraries is None:
            libraries = self.list_libraries()

        registered = []
        for lib_name in libraries:
            lib = self.get_library(lib_name)
            for symbol in lib.list_symbols():
                table_name = f"{lib_name}__{symbol}"
                arrow_table = lib.read(symbol).data
                if not isinstance(arrow_table, pa_mod.Table):
                    arrow_table = pa_mod.Table.from_pandas(arrow_table)
                conn.register(table_name, arrow_table)
                registered.append(table_name)

        return registered
