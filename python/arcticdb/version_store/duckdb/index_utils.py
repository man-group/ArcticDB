"""
Index column resolution helpers for DuckDB SQL integration.

Provides utilities to retrieve index column metadata from symbol descriptions,
used for pandas index reconstruction after SQL queries and for date_range pushdown.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    import pandas as pd

    from arcticdb.version_store.library import Library

logger = logging.getLogger(__name__)

# Dtype substrings that indicate a timestamp/datetime index.
# date_range pushdown is only valid for these types — numeric indexes
# (INT64, UINT64, etc.) must NOT be pushed as date_range because
# pd.Timestamp(int) silently produces nonsensical results.
_DATETIME_DTYPE_MARKERS = ("NANOSECONDS_UTC64", "MICROS_UTC64")


def get_index_fields_for_symbol(library: "Library", symbol: str, as_of=None):
    """Return the index field descriptors for a symbol, or None.

    Uses get_description() to retrieve index metadata.  Returns None for
    RangeIndex (no physical index columns) or when the symbol has no named index.
    """
    desc = library.get_description(symbol, as_of=as_of)

    if desc.index_type not in ("index", "multi_index"):
        return None

    fields = list(desc.index)
    if not fields or all(f.name is None for f in fields):
        return None

    return fields


def get_index_columns_for_symbol(library: "Library", symbol: str, as_of=None) -> Optional[List[str]]:
    """Return the list of index column names for a symbol, or None if not applicable."""
    fields = get_index_fields_for_symbol(library, symbol, as_of=as_of)
    if fields is None:
        return None
    return [f.name for f in fields]


def get_datetime_index_columns_for_symbol(library: "Library", symbol: str, as_of=None) -> Optional[List[str]]:
    """Return datetime index column names for a symbol, or None.

    Like ``get_index_columns_for_symbol`` but only returns columns whose
    dtype is a datetime/timestamp type.  Used for date_range pushdown where
    numeric index values must not be converted via ``pd.Timestamp(int)``.
    """
    fields = get_index_fields_for_symbol(library, symbol, as_of=as_of)
    if fields is None:
        return None

    datetime_cols = [
        f.name for f in fields if f.name is not None and any(m in str(f.dtype) for m in _DATETIME_DTYPE_MARKERS)
    ]
    return datetime_cols if datetime_cols else None


def _resolve_symbol_as_of(as_of, real_symbol: str, sql_name: str = None):
    """Resolve per-symbol as_of from dict or scalar.

    Parameters
    ----------
    as_of : AsOf or Dict[str, AsOf] or None
        Scalar as_of (applied to all symbols) or dict mapping symbol names to versions.
    real_symbol : str
        The resolved ArcticDB symbol name.
    sql_name : str, optional
        The SQL table name (may differ from real_symbol due to case-insensitive matching).

    Returns
    -------
    AsOf or None
        The resolved as_of value for this symbol.
    """
    if not isinstance(as_of, dict):
        return as_of
    if real_symbol in as_of:
        return as_of[real_symbol]
    if sql_name and sql_name in as_of:
        return as_of[sql_name]
    return None


def reconstruct_pandas_index(result: "pd.DataFrame", symbol_versions: Dict[str, Any], library: "Library"):
    """Set the best matching index on a pandas DataFrame from SQL query results.

    Checks each symbol's index columns and picks the most specific (most levels)
    index whose columns are all present in the result.

    Parameters
    ----------
    result : pd.DataFrame
        The query result to set the index on.
    symbol_versions : dict
        Mapping of symbol_name -> as_of value (resolved version int or user-provided as_of).
    library : Library
        ArcticDB library for index column lookup.

    Returns
    -------
    pd.DataFrame
        With index set, or unchanged if no suitable index found.
    """
    best_index = None
    for sym, version in symbol_versions.items():
        idx_cols = get_index_columns_for_symbol(library, sym, as_of=version)
        if idx_cols is not None and all(c in result.columns for c in idx_cols):
            if best_index is None or len(idx_cols) > len(best_index):
                best_index = idx_cols
    if best_index is not None:
        result = result.set_index(best_index)
    return result


def resolve_index_columns_for_sql(library: "Library", sql_ast, as_of=None) -> Optional[List[str]]:
    """Look up datetime index column names for symbols referenced in a SQL AST.

    Used by ``Library.sql()`` and ``Library.explain()`` to enable date_range
    pushdown on named datetime index columns (e.g. ``WHERE Date >= '2025-01-01'``
    on a symbol whose DatetimeIndex is named ``Date``).

    Only returns columns whose dtype is a timestamp type.  Numeric index columns
    are excluded because ``pd.Timestamp(int_value)`` silently produces nonsensical
    timestamps instead of raising.
    """
    from arcticdb.version_store.duckdb.duckdb import _resolve_symbol
    from arcticdb.version_store.duckdb.pushdown import _extract_tables_from_ast

    if sql_ast is None:
        return None
    try:
        alias_map = _extract_tables_from_ast(sql_ast)
        all_idx_cols = []
        for sql_name in set(alias_map.values()):
            try:
                real_sym = _resolve_symbol(sql_name, library)
            except Exception:
                real_sym = sql_name
            symbol_as_of = _resolve_symbol_as_of(as_of, real_sym, sql_name)
            idx_cols = get_datetime_index_columns_for_symbol(library, real_sym, as_of=symbol_as_of)
            if idx_cols:
                all_idx_cols.extend(idx_cols)
        return all_idx_cols if all_idx_cols else None
    except (IndexError, TypeError, ValueError):
        # AST parsing or index inspection failed — disable date pushdown
        return None
    except Exception:
        logger.debug("Failed to resolve index columns for SQL date pushdown", exc_info=True)
        return None
