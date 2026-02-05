"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
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


class DuckDBContext:
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

    def __init__(self, library: "Library", connection: Any=None):
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
        self._library = library
        self._external_conn = connection
        self._conn = None
        self._owns_connection = False
        self._registered_symbols: Dict[str, Dict[str, Any]] = {}

    def __enter__(self) -> "DuckDBContext":
        if self._external_conn is not None:
            # Use externally provided connection - don't close it on exit
            self._conn = self._external_conn
            self._owns_connection = False
        else:
            # Create our own connection - we're responsible for closing it
            duckdb = _check_duckdb_available()
            self._conn = duckdb.connect(":memory:")
            self._owns_connection = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Only close the connection if we created it
        if self._conn and self._owns_connection:
            self._conn.close()
        self._conn = None
        return False

    def register_symbol(
        self,
        symbol: str,
        alias: Optional[str]=None,
        as_of: Optional[AsOf]=None,
        date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]]=None,
        row_range: Optional[Tuple[int, int]]=None,
        columns: Optional[List[str]]=None,
        query_builder: Optional["QueryBuilder"]=None,
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
        if self._conn is None:
            raise RuntimeError("DuckDBContext must be used within a 'with' block")

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
        output_format: str="pandas",
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
        if self._conn is None:
            raise RuntimeError("DuckDBContext must be used within a 'with' block")

        if not self._registered_symbols:
            raise RuntimeError(
                "No symbols have been registered. "
                "Use register_symbol() to register ArcticDB symbols as tables before querying."
            )

        if output_format == "arrow":
            return self._conn.execute(sql).arrow()
        elif output_format == "pandas":
            return self._conn.execute(sql).df()
        elif output_format == "polars":
            import polars as pl

            return pl.from_arrow(self._conn.execute(sql).arrow())
        else:
            raise ValueError(
                f"Unknown output format: {output_format}. " f"Expected one of: 'pandas', 'arrow', 'polars'"
            )

    def execute(self, sql: str) -> "DuckDBContext":
        """
        Execute SQL statement without returning results.

        Useful for DDL statements or intermediate operations.

        Parameters
        ----------
        sql : str
            SQL statement to execute.

        Returns
        -------
        DuckDBContext
            Self, to allow method chaining.
        """
        if self._conn is None:
            raise RuntimeError("DuckDBContext must be used within a 'with' block")

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
