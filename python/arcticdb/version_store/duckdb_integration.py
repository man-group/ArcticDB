"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

import re
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
    Extract symbol names from SQL query by parsing FROM and JOIN clauses.

    This is a simplified parser that handles common cases. For complex queries,
    use the DuckDBContext API which allows explicit symbol registration.

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
    # Pattern to match table names after FROM and JOIN keywords
    # Handles: FROM symbol, JOIN symbol, LEFT JOIN symbol, etc.
    pattern = r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)"
    matches = re.findall(pattern, query, re.IGNORECASE)

    if not matches:
        raise ValueError(
            "Could not extract symbol names from query. "
            "Ensure query contains FROM or JOIN clauses with symbol names, "
            "or use duckdb_context() to register symbols explicitly."
        )

    # Return unique symbols preserving order
    seen = set()
    unique_symbols = []
    for symbol in matches:
        if symbol.lower() not in seen:
            seen.add(symbol.lower())
            unique_symbols.append(symbol)

    return unique_symbols


class DuckDBContext:
    """
    Context manager for executing SQL queries across multiple ArcticDB symbols.

    Provides fine-grained control over symbol registration and query execution,
    enabling complex queries including JOINs across multiple symbols.

    Examples
    --------
    >>> with lib.duckdb_context() as ddb:
    ...     ddb.register_symbol("trades", date_range=(start, end))
    ...     ddb.register_symbol("prices", as_of=-1, alias="latest_prices")
    ...     result = ddb.query('''
    ...         SELECT t.ticker, t.quantity * p.price as notional
    ...         FROM trades t
    ...         JOIN latest_prices p ON t.ticker = p.ticker
    ...         WHERE t.quantity > 1000
    ...     ''')

    See Also
    --------
    Library.sql : Simple SQL queries on single symbols.
    Library.read_as_record_batch_reader : Low-level streaming Arrow reader.
    """

    def __init__(self, library: "Library"):
        """
        Initialize the DuckDB context.

        Parameters
        ----------
        library : Library
            The ArcticDB library to query.
        """
        self._library = library
        self._conn = None
        self._registered_symbols: Dict[str, Dict[str, Any]] = {}

    def __enter__(self) -> "DuckDBContext":
        duckdb = _check_duckdb_available()
        self._conn = duckdb.connect(":memory:")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.close()
            self._conn = None
        return False

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
        >>> with lib.duckdb_context() as ddb:
        ...     ddb.register_symbol("trades")
        ...     ddb.register_symbol("trades", alias="recent_trades",
        ...                         date_range=(datetime(2024, 1, 1), None))
        """
        if self._conn is None:
            raise RuntimeError("DuckDBContext must be used within a 'with' block")

        table_name = alias or symbol

        reader = self._library.read_as_record_batch_reader(
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
