"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from arcticdb.arctic import Arctic
    from arcticdb.version_store.library import Library
    from arcticdb.version_store.processing import QueryBuilder

# Type aliases
Timestamp = Any  # pandas.Timestamp or datetime
AsOf = Union[int, str, "Timestamp"]


def _check_duckdb_available():
    """Check if duckdb is installed and raise helpful error if not."""
    try:
        import duckdb

        return duckdb
    except ImportError:
        raise ImportError("DuckDB integration requires the 'duckdb' package. " "Install it with: pip install duckdb")


def _parse_library_name(library_name: str) -> Tuple[str, str]:
    """
    Parse library name into (database, library) tuple.

    ArcticDB uses `database.library` naming convention where database is
    the permissioning unit (typically one per user). Split on first dot only
    to support multi-component library names. Libraries without dots are
    grouped under '__default__' database.

    Parameters
    ----------
    library_name : str
        Full library name as stored in ArcticDB

    Returns
    -------
    tuple[str, str]
        (database_name, library_name) tuple

    Examples
    --------
    >>> _parse_library_name("jblackburn.test_lib")
    ('jblackburn', 'test_lib')
    >>> _parse_library_name("jblackburn.test.lib")
    ('jblackburn', 'test.lib')
    >>> _parse_library_name("global_data")
    ('__default__', 'global_data')
    """
    if "." not in library_name:
        return "__default__", library_name
    parts = library_name.split(".", 1)
    return parts[0], parts[1]


def _extract_symbols_from_query(query: str) -> List[str]:
    """
    Extract symbol names from SQL query using DuckDB's AST parser.

    Uses DuckDB's json_serialize_sql() to parse the query and extract table
    names from FROM and JOIN clauses.

    Parameters
    ----------
    query : str
        SQL query string.

    Returns
    -------
    List[str]
        List of unique symbol names found in the query.

    Raises
    ------
    ValueError
        If no symbols could be extracted from the query.
    """
    from arcticdb.version_store.duckdb.pushdown import extract_pushdown_from_sql

    # Use the combined function which parses the SQL only once
    _, symbols = extract_pushdown_from_sql(query)
    return symbols


class _BaseDuckDBContext:
    """
    Base class for DuckDB context managers with shared connection and query logic.

    This base class provides common functionality for both single-library and
    multi-library DuckDB context managers, including connection lifecycle
    management, query execution, and format conversion.
    """

    _context_name = "DuckDBContext"  # Override in subclasses for error messages

    def __init__(self, connection=None):
        self._external_conn = connection
        self._conn = None
        self._owns_connection = False
        self._registered_symbols: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _validate_external_connection(connection):
        """
        Validate that the provided connection is a usable DuckDB connection.

        Parameters
        ----------
        connection : Any
            The connection object to validate.

        Raises
        ------
        TypeError
            If the connection is not a DuckDB connection object.
        ValueError
            If the connection is not usable (e.g., already closed).
        """
        if not hasattr(connection, "execute"):
            raise TypeError(
                f"Expected a DuckDB connection object, got {type(connection).__name__}. "
                "Create one with: duckdb.connect()"
            )
        try:
            connection.execute("SELECT 1")
        except Exception as e:
            raise ValueError(
                f"The provided DuckDB connection is not usable: {e}. " "Ensure the connection is open and valid."
            ) from e

    def __enter__(self):
        if self._external_conn is not None:
            self._validate_external_connection(self._external_conn)
            self._conn = self._external_conn
            self._owns_connection = False
        else:
            duckdb = _check_duckdb_available()
            self._conn = duckdb.connect(":memory:")
            self._owns_connection = True
        return self

    @property
    def connection(self):
        """The underlying DuckDB connection.

        Use this to pass the connection to a nested context manager for
        cross-library or cross-instance JOINs::

            with lib_a.duckdb() as outer:
                outer.register_symbol("trades")
                with lib_b.duckdb(connection=outer.connection) as inner:
                    inner.register_symbol("prices")
                    result = inner.query("SELECT * FROM trades JOIN prices ...")
        """
        self._check_in_context()
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Unregister symbols this context registered
        if self._conn:
            for table_name in self._registered_symbols:
                try:
                    self._conn.unregister(table_name)
                except Exception:
                    pass  # Connection may already be closed or table already removed
            self._registered_symbols.clear()
            if self._owns_connection:
                self._conn.close()
        self._conn = None
        return False

    def _check_in_context(self):
        """Ensure the context manager is being used within a 'with' block."""
        if self._conn is None:
            raise RuntimeError(f"{self._context_name} must be used within a 'with' block")

    def _format_query_result(self, sql: str, output_format: str) -> Any:
        """
        Execute SQL and return result in requested format.

        Parameters
        ----------
        sql : str
            SQL query to execute.
        output_format : str
            Output format: "pandas", "arrow", or "polars".

        Returns
        -------
        pandas.DataFrame, pyarrow.Table, or polars.DataFrame
            Query result in the requested format.

        Raises
        ------
        ValueError
            If the output format is not recognized.
        """
        if output_format == "arrow":
            return self._conn.execute(sql).fetch_arrow_table()
        elif output_format == "pandas":
            return self._conn.execute(sql).df()
        elif output_format == "polars":
            import polars as pl

            return pl.from_arrow(self._conn.execute(sql).fetch_arrow_table())
        else:
            raise ValueError(
                f"Unknown output format: {output_format}. " f"Expected one of: 'pandas', 'arrow', 'polars'"
            )

    def execute(self, sql: str):
        """
        Execute SQL statement without returning results.

        Useful for DDL statements or intermediate operations.

        Parameters
        ----------
        sql : str
            SQL statement to execute.

        Returns
        -------
        Self
            To allow method chaining.
        """
        self._check_in_context()
        self._conn.execute(sql)
        return self

    @property
    def registered_symbols(self) -> Dict[str, Dict[str, Any]]:
        """Return information about registered symbols."""
        return self._registered_symbols.copy()

    @property
    def connection(self):
        """
        Return the underlying DuckDB connection for advanced usage.

        Warning: Direct connection manipulation may interfere with
        the context manager's resource management.
        """
        return self._conn


class DuckDBContext(_BaseDuckDBContext):
    """
    Context manager for executing SQL queries across multiple ArcticDB symbols.

    Provides fine-grained control over symbol registration and query execution,
    enabling complex queries including JOINs across multiple symbols.

    Can optionally use an external DuckDB connection, allowing joins between
    ArcticDB data and other DuckDB data sources (Parquet files, CSV, other databases, etc.).

    Examples
    --------
    Basic usage:

    >>> with lib.duckdb() as ddb:
    ...     ddb.register_symbol("trades", date_range=(start, end))
    ...     ddb.register_symbol("prices", as_of=-1, alias="latest_prices")
    ...     result = ddb.query('''
    ...         SELECT t.ticker, t.quantity * p.price as notional
    ...         FROM trades t
    ...         JOIN latest_prices p ON t.ticker = p.ticker
    ...         WHERE t.quantity > 1000
    ...     ''')

    Join with external data sources using your own DuckDB connection:

    >>> import duckdb
    >>> conn = duckdb.connect()
    >>> conn.execute("CREATE TABLE benchmarks AS SELECT * FROM 'benchmarks.parquet'")
    >>> with lib.duckdb(connection=conn) as ddb:
    ...     ddb.register_symbol("returns")
    ...     result = ddb.query('''
    ...         SELECT r.date, r.return - b.return as alpha
    ...         FROM returns r
    ...         JOIN benchmarks b ON r.date = b.date
    ...     ''')
    >>> # Connection is still open - ArcticDB did not close it
    >>> conn.execute("SELECT * FROM benchmarks")  # Still works

    See Also
    --------
    Library.sql : Simple SQL queries on single symbols.
    """

    _context_name = "DuckDBContext"

    def __init__(self, library: "Library", connection: Any = None):
        """
        Initialize the DuckDB context.

        Parameters
        ----------
        library : Library
            The ArcticDB library to query.
        connection : duckdb.DuckDBPyConnection, optional
            External DuckDB connection to use. If provided, ArcticDB will register
            symbols into this connection but will NOT close it when the context exits.
            This allows joining ArcticDB data with other data already in the connection.
            If not provided, a new in-memory connection is created and closed on exit.
        """
        super().__init__(connection=connection)
        self._library = library

    def __enter__(self) -> "DuckDBContext":
        super().__enter__()
        return self

    def register_symbol(
        self,
        symbol: str,
        alias: Optional[str] = None,
        as_of: Optional[AsOf] = None,
        date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None,
        row_range: Optional[Tuple[int, int]] = None,
        columns: Optional[List[str]] = None,
        query_builder: Optional["QueryBuilder"] = None,
    ) -> "DuckDBContext":
        """
        Register an ArcticDB symbol as a DuckDB table.

        The symbol data is streamed lazily using Arrow record batches,
        so large datasets don't need to be fully loaded into memory.

        Parameters
        ----------
        symbol : str
            ArcticDB symbol to register.
        alias : str, optional
            Table name in DuckDB. Defaults to the symbol name.
            Useful for registering the same symbol multiple times with different filters.
        as_of : AsOf, optional
            Version to read. See Library.read() for details.
        date_range : tuple, optional
            Date range filter applied at the ArcticDB level before SQL processing.
        row_range : tuple, optional
            Row range filter applied at the ArcticDB level.
        columns : list, optional
            Column subset. Only specified columns are read from storage.
        query_builder : QueryBuilder, optional
            ArcticDB query builder for pre-filtering before SQL processing.

        Returns
        -------
        DuckDBContext
            Self, to allow method chaining.

        Examples
        --------
        >>> with lib.duckdb() as ddb:
        ...     ddb.register_symbol("trades")
        ...     ddb.register_symbol("trades", alias="recent_trades",
        ...                         date_range=(datetime(2024, 1, 1), None))
        """
        self._check_in_context()

        table_name = alias or symbol

        reader = self._library._read_as_record_batch_reader(
            symbol=symbol,
            as_of=as_of,
            date_range=date_range,
            row_range=row_range,
            columns=columns,
            query_builder=query_builder,
        )

        # Convert to native PyArrow RecordBatchReader for DuckDB compatibility
        self._conn.register(table_name, reader.to_pyarrow_reader())
        self._registered_symbols[table_name] = {
            "symbol": symbol,
            "as_of": as_of,
            "date_range": date_range,
        }

        return self

    def query(
        self,
        sql: str,
        output_format: str = "pandas",
    ) -> Any:
        """
        Execute SQL query and return results.

        Parameters
        ----------
        sql : str
            SQL query to execute. Can reference any registered symbols as tables.
        output_format : str, default="pandas"
            Output format: "pandas", "arrow", or "polars".

        Returns
        -------
        pandas.DataFrame, pyarrow.Table, or polars.DataFrame
            Query result in the requested format.

        Raises
        ------
        RuntimeError
            If called outside of a 'with' block or if no symbols have been registered.

        Examples
        --------
        >>> result = ddb.query('''
        ...     SELECT ticker, SUM(quantity) as total_qty
        ...     FROM trades
        ...     GROUP BY ticker
        ...     ORDER BY total_qty DESC
        ... ''')
        """
        self._check_in_context()

        if not self._registered_symbols:
            raise RuntimeError(
                "No symbols have been registered. "
                "Use register_symbol() to register ArcticDB symbols as tables before querying."
            )

        return self._format_query_result(sql, output_format)

    def register_all_symbols(self, as_of: Optional[AsOf] = None) -> "DuckDBContext":
        """
        Register all symbols from the library as DuckDB tables.

        This enables data discovery queries like SHOW TABLES and SHOW ALL TABLES
        to list all symbols stored in the ArcticDB library.

        Parameters
        ----------
        as_of : AsOf, optional
            Version to read for all symbols. See Library.read() for details.
            If not specified, reads the latest version of each symbol.

        Returns
        -------
        DuckDBContext
            Self, to allow method chaining.

        Examples
        --------
        >>> with lib.duckdb() as ddb:
        ...     ddb.register_all_symbols()
        ...     tables = ddb.query("SHOW TABLES")
        ...     print(tables)  # Lists all symbols in the library
        """
        self._check_in_context()

        symbols = self._library.list_symbols()
        for symbol in symbols:
            self.register_symbol(symbol, as_of=as_of)

        return self


class ArcticDuckDBContext(_BaseDuckDBContext):
    """
    Context manager for executing SQL queries across multiple ArcticDB libraries.

    Provides access to all libraries in an Arctic instance as "databases",
    enabling data discovery queries like SHOW DATABASES and cross-library queries.

    Examples
    --------
    Basic usage with SHOW DATABASES:

    >>> with arctic.duckdb() as ddb:
    ...     ddb.register_library("market_data")
    ...     ddb.register_library("reference_data")
    ...     databases = ddb.query("SHOW DATABASES")
    ...     print(databases)  # Lists registered libraries

    Register all libraries for discovery:

    >>> with arctic.duckdb() as ddb:
    ...     ddb.register_all_libraries()
    ...     databases = ddb.query("SHOW DATABASES")

    Cross-library queries with table prefixes:

    >>> with arctic.duckdb() as ddb:
    ...     ddb.register_symbol("market_data", "prices")
    ...     ddb.register_symbol("reference_data", "securities", alias="ref_securities")
    ...     result = ddb.query('''
    ...         SELECT p.ticker, r.name, p.price
    ...         FROM prices p
    ...         JOIN ref_securities r ON p.ticker = r.ticker
    ...     ''')

    See Also
    --------
    Arctic.sql : Simple SQL queries for database discovery.
    Library.duckdb : Context manager for single-library queries.
    """

    _context_name = "ArcticDuckDBContext"

    def __init__(self, arctic: "Arctic", connection: Any = None):
        """
        Initialize the Arctic DuckDB context.

        Parameters
        ----------
        arctic : Arctic
            The ArcticDB Arctic instance to query.
        connection : duckdb.DuckDBPyConnection, optional
            External DuckDB connection to use. If provided, ArcticDB will register
            tables into this connection but will NOT close it when the context exits.
            If not provided, a new in-memory connection is created and closed on exit.
        """
        super().__init__(connection=connection)
        self._arctic = arctic
        self._registered_libraries: Dict[str, Dict[str, Any]] = {}

    def __enter__(self) -> "ArcticDuckDBContext":
        super().__enter__()
        return self

    def register_library(self, library_name: str) -> "ArcticDuckDBContext":
        """
        Register a library as a "database" for discovery queries.

        This registers the library name so it appears in SHOW DATABASES results.
        To query symbols from the library, use register_symbol().

        Parameters
        ----------
        library_name : str
            Name of the ArcticDB library to register.

        Returns
        -------
        ArcticDuckDBContext
            Self, to allow method chaining.

        Examples
        --------
        >>> with arctic.duckdb() as ddb:
        ...     ddb.register_library("market_data")
        ...     ddb.register_library("reference_data")
        ...     databases = ddb.query("SHOW DATABASES")
        """
        self._check_in_context()

        if library_name not in self._arctic:
            raise ValueError(f"Library '{library_name}' does not exist")

        self._registered_libraries[library_name] = {"name": library_name}
        return self

    def register_all_libraries(self) -> "ArcticDuckDBContext":
        """
        Register all libraries from the Arctic instance for discovery.

        This enables SHOW DATABASES to list all libraries stored in the Arctic instance.

        Returns
        -------
        ArcticDuckDBContext
            Self, to allow method chaining.

        Examples
        --------
        >>> with arctic.duckdb() as ddb:
        ...     ddb.register_all_libraries()
        ...     databases = ddb.query("SHOW DATABASES")
        ...     print(databases)  # Lists all libraries
        """
        self._check_in_context()

        for lib_name in self._arctic.list_libraries():
            self._registered_libraries[lib_name] = {"name": lib_name}

        return self

    def register_symbol(
        self,
        library_name: str,
        symbol: str,
        alias: Optional[str] = None,
        as_of: Optional[AsOf] = None,
        date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None,
        row_range: Optional[Tuple[int, int]] = None,
        columns: Optional[List[str]] = None,
        query_builder: Optional["QueryBuilder"] = None,
    ) -> "ArcticDuckDBContext":
        """
        Register an ArcticDB symbol from a specific library as a DuckDB table.

        Parameters
        ----------
        library_name : str
            Name of the ArcticDB library containing the symbol.
        symbol : str
            ArcticDB symbol to register.
        alias : str, optional
            Table name in DuckDB. Defaults to the symbol name.
        as_of : AsOf, optional
            Version to read. See Library.read() for details.
        date_range : tuple, optional
            Date range filter applied at the ArcticDB level.
        row_range : tuple, optional
            Row range filter applied at the ArcticDB level.
        columns : list, optional
            Column subset to read from storage.
        query_builder : QueryBuilder, optional
            ArcticDB query builder for pre-filtering.

        Returns
        -------
        ArcticDuckDBContext
            Self, to allow method chaining.

        Examples
        --------
        >>> with arctic.duckdb() as ddb:
        ...     ddb.register_symbol("market_data", "prices")
        ...     ddb.register_symbol("reference_data", "securities", alias="ref")
        ...     result = ddb.query("SELECT * FROM prices JOIN ref ON ...")
        """
        self._check_in_context()

        library = self._arctic.get_library(library_name)
        table_name = alias or symbol

        reader = library._read_as_record_batch_reader(
            symbol=symbol,
            as_of=as_of,
            date_range=date_range,
            row_range=row_range,
            columns=columns,
            query_builder=query_builder,
        )

        self._conn.register(table_name, reader.to_pyarrow_reader())
        self._registered_symbols[table_name] = {
            "library": library_name,
            "symbol": symbol,
            "as_of": as_of,
            "date_range": date_range,
        }

        # Also ensure the library is registered for SHOW DATABASES
        if library_name not in self._registered_libraries:
            self._registered_libraries[library_name] = {"name": library_name}

        return self

    def query(
        self,
        sql: str,
        output_format: str = "pandas",
    ) -> Any:
        """
        Execute SQL query and return results.

        Parameters
        ----------
        sql : str
            SQL query to execute. Supports ``SHOW DATABASES`` for listing
            registered libraries grouped by database.
        output_format : str, default="pandas"
            Output format: "pandas", "arrow", or "polars".

        Returns
        -------
        pandas.DataFrame, pyarrow.Table, or polars.DataFrame
            Query result in the requested format.

        Examples
        --------
        >>> result = ddb.query("SHOW DATABASES")
        >>> result = ddb.query("SELECT * FROM prices WHERE price > 100")
        """
        self._check_in_context()

        from arcticdb.version_store.duckdb.pushdown import is_database_discovery_query

        # Handle SHOW DATABASES - return registered libraries grouped by database
        if is_database_discovery_query(sql):
            return self._execute_show_databases(output_format)

        if not self._registered_symbols:
            raise RuntimeError(
                "No symbols have been registered. "
                "Use register_symbol() to register ArcticDB symbols as tables before querying."
            )

        return self._format_query_result(sql, output_format)

    def _execute_show_databases(self, output_format: str) -> Any:
        """Execute SHOW DATABASES and return registered libraries grouped by database."""
        import pyarrow as pa
        from collections import defaultdict

        # Group registered libraries by database
        database_counts = defaultdict(int)
        for lib_name in self._registered_libraries.keys():
            database, _ = _parse_library_name(lib_name)
            database_counts[database] += 1

        # Create result table with database hierarchy info
        arrow_table = pa.table(
            {
                "database_name": list(database_counts.keys()),
                "library_count": list(database_counts.values()),
            }
        )

        return self._format_output(arrow_table, output_format)

    def _format_output(self, arrow_table, output_format: str) -> Any:
        """Convert Arrow table to requested output format."""
        if output_format == "arrow":
            return arrow_table
        elif output_format == "pandas":
            return arrow_table.to_pandas()
        elif output_format == "polars":
            import polars as pl

            return pl.from_arrow(arrow_table)
        else:
            raise ValueError(
                f"Unknown output format: {output_format}. " f"Expected one of: 'pandas', 'arrow', 'polars'"
            )

    @property
    def registered_libraries(self) -> Dict[str, Dict[str, Any]]:
        """Return information about registered libraries."""
        return self._registered_libraries.copy()
