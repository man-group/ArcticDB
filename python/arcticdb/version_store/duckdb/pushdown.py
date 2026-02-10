"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

# NOTE: SQL Parsing Policy
# ========================
# ALWAYS use DuckDB's json_serialize_sql() AST parser for SQL analysis. Never use regular
# expressions or string matching to parse SQL structure (e.g. extracting table names, columns,
# filters). SQL grammar is too complex for regex — edge cases with quoting, comments, subqueries,
# CTEs, etc. will break string-based approaches.
#
# Read-only validation also uses the AST parser: json_serialize_sql() only accepts SELECT-like
# statements, so non-SELECT queries (INSERT, UPDATE, etc.) are rejected by DuckDB itself.

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from arcticdb.version_store.processing import QueryBuilder

logger = logging.getLogger(__name__)

# ISO date pattern: YYYY-MM-DD with optional time component.
# Used to auto-convert VARCHAR literals to timestamps when they look like dates,
# so that `WHERE ts < '2024-01-03'` works the same as `WHERE ts < TIMESTAMP '2024-01-03'`.
# This matches standard SQL behavior where string-to-timestamp casts are implicit.
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


@dataclass
class PushdownInfo:
    """Information about what can be pushed down to ArcticDB for a single table."""

    columns: Optional[List[str]] = None
    query_builder: Optional[QueryBuilder] = None
    limit: Optional[int] = None
    date_range: Optional[Tuple[Any, Any]] = None

    # Tracking what was pushed down
    filter_pushed_down: bool = False
    columns_pushed_down: Optional[List[str]] = None
    limit_pushed_down: Optional[int] = None
    date_range_pushed_down: bool = False

    # Filters that couldn't be pushed (will be applied by DuckDB)
    unpushed_filters: List[str] = field(default_factory=list)

    # True when ArcticDB can handle the entire query natively (single table,
    # no GROUP BY / ORDER BY / DISTINCT / JOINs / CTEs / LIMIT with ordering),
    # so DuckDB is not needed and we can return the read result directly.
    fully_pushed: bool = False


def _extract_limit_from_ast(ast: Dict) -> Optional[int]:
    """Extract LIMIT value from parsed SQL AST."""
    try:
        statements = ast.get("statements", [])
        if not statements:
            return None

        node = statements[0].get("node", {})
        modifiers = node.get("modifiers", [])

        for mod in modifiers:
            if mod.get("type") == "LIMIT_MODIFIER":
                limit_node = mod.get("limit", {})
                if limit_node.get("class") == "CONSTANT":
                    value = limit_node.get("value", {}).get("value")
                    if value is not None:
                        return int(value)

        return None
    except (ValueError, KeyError, TypeError, IndexError) as e:
        logger.debug("Failed to extract LIMIT from AST: %s", e)
        return None


def _has_order_by(ast: Dict) -> bool:
    """Check whether the query has an ORDER BY clause.

    LIMIT cannot safely be pushed down as a storage-level row_range when the
    query also contains ORDER BY, because the sort order in storage may differ
    from the requested sort.  DuckDB needs *all* rows to perform the sort
    before applying the LIMIT.
    """
    try:
        modifiers = ast["statements"][0]["node"].get("modifiers", [])
        return any(mod.get("type") == "ORDER_MODIFIER" for mod in modifiers)
    except (KeyError, IndexError, TypeError):
        return False


def _extract_column_refs_from_node(
    node: Dict,
    table_alias_map: Dict[str, str],
    columns_by_table: Dict[str, Set[str]],
) -> None:
    """
    Recursively extract column references from an AST node.

    Parameters
    ----------
    node : dict
        AST node to traverse.
    table_alias_map : dict
        Mapping of alias -> table_name.
    columns_by_table : dict
        Output dict mapping table_name -> set of column names (mutated in place).
    """
    if not isinstance(node, dict):
        return

    if node.get("class") == "COLUMN_REF":
        col_names = node.get("column_names", [])
        if col_names:
            if len(col_names) >= 2:
                # Qualified: table.column or alias.column
                table_ref = col_names[0]
                col_name = col_names[-1]
                # Resolve alias to table name
                table_name = table_alias_map.get(table_ref.lower(), table_ref)
            else:
                # Unqualified column - we'll add to all tables
                col_name = col_names[0]
                table_name = None

            if table_name:
                if table_name not in columns_by_table:
                    columns_by_table[table_name] = set()
                columns_by_table[table_name].add(col_name)
            else:
                # Add to all known tables (will be refined later)
                for tbl in table_alias_map.values():
                    if tbl not in columns_by_table:
                        columns_by_table[tbl] = set()
                    columns_by_table[tbl].add(col_name)
        return

    # Recurse into child nodes
    for key, value in node.items():
        if isinstance(value, dict):
            _extract_column_refs_from_node(value, table_alias_map, columns_by_table)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _extract_column_refs_from_node(item, table_alias_map, columns_by_table)


def _extract_columns_from_select_list(select_list: List[Dict], table_alias_map: Dict[str, str]) -> Dict[str, Set[str]]:
    """
    Extract column references from SELECT list, grouped by table.

    Parameters
    ----------
    select_list : list
        The select_list from the AST.
    table_alias_map : dict
        Mapping of alias -> table_name.

    Returns
    -------
    dict
        Mapping of table_name -> set of column names.
    """
    columns_by_table: Dict[str, Set[str]] = {}
    for item in select_list:
        _extract_column_refs_from_node(item, table_alias_map, columns_by_table)
    return columns_by_table


def _extract_columns_from_where(where_clause: Dict, table_alias_map: Dict[str, str]) -> Dict[str, Set[str]]:
    """Extract column references from WHERE clause, grouped by table."""
    columns_by_table: Dict[str, Set[str]] = {}
    if where_clause:
        _extract_column_refs_from_node(where_clause, table_alias_map, columns_by_table)
    return columns_by_table


def _extract_cte_names(ast: Dict) -> Set[str]:
    """
    Extract CTE (Common Table Expression) names from the AST.

    CTE names (defined by WITH ... AS) are not real table references — they're
    query-local aliases. They must be excluded from the symbol list so we don't
    try to read them as ArcticDB symbols.
    """
    cte_names: Set[str] = set()

    def collect_ctes(node: Any) -> None:
        if not isinstance(node, dict):
            return
        cte_map = node.get("cte_map", {})
        for entry in cte_map.get("map", []):
            key = entry.get("key", "")
            if key:
                cte_names.add(key.lower())
            # Recurse into nested CTEs
            inner_query = entry.get("value", {}).get("query", {}).get("node", {})
            if inner_query:
                collect_ctes(inner_query)

    try:
        statements = ast.get("statements", [])
        if statements:
            collect_ctes(statements[0].get("node", {}))
    except (ValueError, KeyError, TypeError, IndexError) as e:
        logger.debug("Failed to extract CTE names from AST: %s", e)

    return cte_names


def _extract_tables_from_ast(ast: Dict) -> Dict[str, str]:
    """
    Extract table references from AST, returning mapping of alias -> table_name.

    Handles FROM clause, JOINs, and DDL queries like DESCRIBE/SHOW.
    CTE names are excluded so they aren't mistaken for ArcticDB symbols.
    """
    cte_names = _extract_cte_names(ast)
    alias_map: Dict[str, str] = {}

    def extract_tables_recursive(node: Any) -> None:
        """Recursively search for BASE_TABLE nodes anywhere in the AST."""
        if not isinstance(node, dict):
            return

        node_type = node.get("type", "")

        if node_type == "BASE_TABLE":
            table_name = node.get("table_name", "")
            alias = node.get("alias", "") or table_name
            if table_name and table_name.lower() not in cte_names:
                alias_map[alias.lower()] = table_name
                alias_map[table_name.lower()] = table_name

        elif node_type in ("JOIN", "INNER_JOIN", "LEFT_JOIN", "RIGHT_JOIN", "FULL_JOIN", "CROSS_JOIN"):
            left = node.get("left", {})
            right = node.get("right", {})
            extract_tables_recursive(left)
            extract_tables_recursive(right)

        elif node_type == "SUBQUERY":
            # For subqueries, we don't push down
            pass

        else:
            # Recursively search all dict values for nested table references
            # This handles DESCRIBE/SHOW queries where table is at from_table.query.from_table
            for key, value in node.items():
                if isinstance(value, dict):
                    extract_tables_recursive(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            extract_tables_recursive(item)

    try:
        statements = ast.get("statements", [])
        if not statements:
            return alias_map

        node = statements[0].get("node", {})
        extract_tables_recursive(node)

    except (ValueError, KeyError, TypeError, IndexError) as e:
        logger.debug("Failed to extract tables from AST: %s", e)

    return alias_map


def _ast_to_filters(node: Dict) -> List[Dict]:
    """
    Convert a DuckDB AST node to a list of filter dictionaries.

    Recursively processes AND conjunctions to flatten them into a list.
    OR conjunctions and unsupported expressions return empty list.

    Parameters
    ----------
    node : dict
        AST node from json_serialize_sql()

    Returns
    -------
    list
        List of filter dicts with keys: column, op, value, type
    """
    node_class = node.get("class", "")
    node_type = node.get("type", "")

    if node_class == "CONJUNCTION" and node_type == "CONJUNCTION_AND":
        # Flatten AND conjunctions into list of filters
        filters = []
        for child in node.get("children", []):
            child_filters = _ast_to_filters(child)
            if child_filters:
                filters.extend(child_filters)
        return filters
    elif node_class == "CONJUNCTION" and node_type == "CONJUNCTION_OR":
        # OR conjunctions cannot be pushed to ArcticDB
        return []
    elif node_class == "COMPARISON":
        return _parse_comparison_node(node)
    elif node_class == "OPERATOR" and node_type == "OPERATOR_IS_NULL":
        return _parse_null_check_node(node, is_not=False)
    elif node_class == "OPERATOR" and node_type == "OPERATOR_IS_NOT_NULL":
        return _parse_null_check_node(node, is_not=True)
    elif node_class == "OPERATOR" and node_type == "COMPARE_IN":
        return _parse_in_node(node, is_not=False)
    elif node_class == "OPERATOR" and node_type == "COMPARE_NOT_IN":
        return _parse_in_node(node, is_not=True)
    elif node_class == "BETWEEN" and node_type == "COMPARE_BETWEEN":
        return _parse_between_node(node)
    elif node_class == "FUNCTION":
        # Functions (like LIKE, UPPER, etc.) cannot be pushed down
        return []
    else:
        return []


def _parse_comparison_node(node: Dict) -> List[Dict]:
    """Parse a comparison AST node (=, !=, <, >, <=, >=)."""
    node_type = node.get("type", "")

    # Map DuckDB comparison types to operators
    op_map = {
        "COMPARE_EQUAL": "=",
        "COMPARE_NOTEQUAL": "!=",
        "COMPARE_LESSTHAN": "<",
        "COMPARE_GREATERTHAN": ">",
        "COMPARE_LESSTHANOREQUALTO": "<=",
        "COMPARE_GREATERTHANOREQUALTO": ">=",
    }

    op = op_map.get(node_type)
    if not op:
        return []

    left = node.get("left", {})
    right = node.get("right", {})

    # Left side should be a column reference
    if left.get("class") != "COLUMN_REF":
        return []

    column_names = left.get("column_names", [])
    if not column_names:
        return []
    column = column_names[-1]  # Use last part for qualified names

    # Right side should be a constant
    value = _extract_constant_value(right)
    if value is None:
        return []

    return [
        {
            "column": column,
            "op": op,
            "value": value,
            "type": "comparison",
        }
    ]


def _parse_null_check_node(node: Dict, is_not: bool) -> List[Dict]:
    """Parse IS NULL / IS NOT NULL AST node."""
    children = node.get("children", [])
    if not children:
        return []

    child = children[0]
    if child.get("class") != "COLUMN_REF":
        return []

    column_names = child.get("column_names", [])
    if not column_names:
        return []
    column = column_names[-1]

    return [
        {
            "column": column,
            "op": "IS NOT NULL" if is_not else "IS NULL",
            "value": None,
            "type": "null_check",
        }
    ]


def _parse_in_node(node: Dict, is_not: bool) -> List[Dict]:
    """Parse IN / NOT IN AST node."""
    children = node.get("children", [])
    if len(children) < 2:
        return []

    # First child is the column
    col_node = children[0]
    if col_node.get("class") != "COLUMN_REF":
        return []

    column_names = col_node.get("column_names", [])
    if not column_names:
        return []
    column = column_names[-1]

    # Remaining children are the values
    values = []
    for val_node in children[1:]:
        val = _extract_constant_value(val_node)
        if val is None:
            return []  # Can't push if any value can't be extracted
        values.append(val)

    if not values:
        return []

    return [
        {
            "column": column,
            "op": "NOT IN" if is_not else "IN",
            "value": values,
            "type": "membership",
        }
    ]


def _parse_between_node(node: Dict) -> List[Dict]:
    """Parse BETWEEN AST node."""
    input_node = node.get("input", {})
    lower_node = node.get("lower", {})
    upper_node = node.get("upper", {})

    if input_node.get("class") != "COLUMN_REF":
        return []

    column_names = input_node.get("column_names", [])
    if not column_names:
        return []
    column = column_names[-1]

    lower = _extract_constant_value(lower_node)
    upper = _extract_constant_value(upper_node)

    if lower is None or upper is None:
        return []

    return [
        {
            "column": column,
            "op": "BETWEEN",
            "value": (lower, upper),
            "type": "range",
        }
    ]


def _convert_to_timestamp(value: Any) -> Any:
    """Convert a value to pandas Timestamp, returning None on failure."""
    import pandas as pd

    try:
        return pd.Timestamp(value)
    except (ValueError, TypeError):
        return None


def _extract_constant_value(node: Dict) -> Any:
    """Extract a Python value from a constant AST node.

    Also handles CAST nodes by extracting the value from the inner child
    and using the cast type to determine the Python type.
    """
    node_class = node.get("class", "")
    node_type = node.get("type", "")

    _TIMESTAMP_TYPES = {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S", "DATE"}
    _INTEGER_TYPES = {"INTEGER", "BIGINT", "SMALLINT", "TINYINT", "UINTEGER", "UBIGINT", "USMALLINT", "UTINYINT"}
    _FLOAT_TYPES = {"FLOAT", "DOUBLE", "REAL"}

    # Handle CAST nodes (e.g., '2024-01-01'::TIMESTAMP_NS)
    if node_class == "CAST" and node_type == "OPERATOR_CAST":
        child = node.get("child", {})
        cast_type = node.get("cast_type", {}).get("id", "")

        child_value = _extract_constant_value(child)
        if child_value is None:
            return None

        if cast_type in _TIMESTAMP_TYPES:
            return _convert_to_timestamp(child_value)
        elif cast_type in _INTEGER_TYPES:
            try:
                return int(child_value)
            except (ValueError, TypeError):
                return None
        elif cast_type in _FLOAT_TYPES:
            try:
                return float(child_value)
            except (ValueError, TypeError):
                return None
        else:
            return child_value

    elif node_class == "CONSTANT" and node_type == "VALUE_CONSTANT":
        value_info = node.get("value", {})
        if value_info.get("is_null"):
            return None

        type_id = value_info.get("type", {}).get("id", "")
        raw_value = value_info.get("value")

        if raw_value is None:
            return None

        _ALL_INTEGER_TYPES = _INTEGER_TYPES | {"HUGEINT", "UHUGEINT"}

        if type_id in _ALL_INTEGER_TYPES:
            return int(raw_value)
        elif type_id in _FLOAT_TYPES:
            return float(raw_value)
        elif type_id == "DECIMAL":
            # DECIMAL stores value as integer, need to apply scale
            type_info = value_info.get("type", {}).get("type_info", {})
            scale = type_info.get("scale", 0)
            return float(raw_value) / (10**scale)
        elif type_id == "BOOLEAN":
            return bool(raw_value)
        elif type_id == "VARCHAR":
            # Auto-convert ISO date strings to timestamps so that
            # WHERE ts < '2024-01-03' works without explicit TIMESTAMP keyword.
            if isinstance(raw_value, str) and _ISO_DATE_RE.match(raw_value):
                ts = _convert_to_timestamp(raw_value)
                if ts is not None:
                    return ts
            return raw_value
        elif type_id in _TIMESTAMP_TYPES:
            return _convert_to_timestamp(raw_value)
        else:
            return raw_value

    else:
        return None


def _build_query_builder(parsed_filters: List[Dict]) -> Optional[QueryBuilder]:
    """
    Build ArcticDB QueryBuilder from parsed filter expressions.

    Parameters
    ----------
    parsed_filters : list
        List of parsed filter dicts.

    Returns
    -------
    QueryBuilder or None
        QueryBuilder with filters applied, or None if no filters.
    """
    if not parsed_filters:
        return None

    q = QueryBuilder()
    filter_expr = None

    for f in parsed_filters:
        col = f["column"]
        op = f["op"]
        value = f["value"]
        ftype = f["type"]

        try:
            if ftype == "comparison" and op == "=":
                expr = q[col] == value
            elif ftype == "comparison" and op == "!=":
                expr = q[col] != value
            elif ftype == "comparison" and op == "<":
                expr = q[col] < value
            elif ftype == "comparison" and op == ">":
                expr = q[col] > value
            elif ftype == "comparison" and op == "<=":
                expr = q[col] <= value
            elif ftype == "comparison" and op == ">=":
                expr = q[col] >= value
            elif ftype == "membership" and op == "IN":
                expr = q[col].isin(*value)
            elif ftype == "membership" and op == "NOT IN":
                expr = q[col].isnotin(*value)
            elif ftype == "null_check" and op == "IS NULL":
                expr = q[col].isnull()
            elif ftype == "null_check" and op == "IS NOT NULL":
                expr = q[col].notnull()
            elif ftype == "range":
                low, high = value
                expr = (q[col] >= low) & (q[col] <= high)
            else:
                continue

            if filter_expr is None:
                filter_expr = expr
            else:
                filter_expr = filter_expr & expr

        except (ValueError, KeyError, TypeError) as e:
            # Skip filters that can't be converted to QueryBuilder
            logger.warning("Skipping filter that couldn't be converted to QueryBuilder: %s (error: %s)", f, e)
            continue

    if filter_expr is not None:
        return q[filter_expr]

    return None


def _extract_date_range(
    parsed_filters: List[Dict],
    index_columns: Optional[List[str]] = None,
) -> Tuple[Optional[Tuple[Any, Any]], List[Dict]]:
    """
    Extract date_range from filters on index columns.

    By default, only the literal column name ``"index"`` is recognised.
    When *index_columns* is supplied (e.g. ``["Date"]`` for a symbol whose
    DatetimeIndex is named ``Date``), those column names are also treated as
    index filters eligible for date_range pushdown.

    Parameters
    ----------
    parsed_filters : list of dict
        Filters parsed from the SQL WHERE clause.
    index_columns : list of str, optional
        Additional column names that should be treated as index columns.

    Returns
    -------
    tuple
        (date_range or None, remaining filters)
    """
    import pandas as pd

    # Build the set of column names (lower-cased) we treat as the row index.
    index_names = {"index"}
    if index_columns:
        index_names.update(c.lower() for c in index_columns)

    date_range = [None, None]
    remaining = []
    has_strict_date_op = False

    for f in parsed_filters:
        col = f.get("column", "").lower()
        if col not in index_names:
            remaining.append(f)
            continue

        op = f["op"]
        value = f["value"]
        ftype = f["type"]

        try:
            if ftype == "range":  # BETWEEN
                low, high = value
                date_range[0] = pd.Timestamp(low) if not isinstance(low, pd.Timestamp) else low
                date_range[1] = pd.Timestamp(high) if not isinstance(high, pd.Timestamp) else high
            elif ftype == "comparison":
                ts = pd.Timestamp(value) if not isinstance(value, pd.Timestamp) else value
                if op in (">=", ">"):
                    date_range[0] = ts
                    if op == ">":
                        has_strict_date_op = True
                elif op in ("<=", "<"):
                    date_range[1] = ts
                    if op == "<":
                        has_strict_date_op = True
                else:
                    remaining.append(f)
            else:
                remaining.append(f)
        except (ValueError, TypeError):
            remaining.append(f)

    if date_range[0] is not None or date_range[1] is not None:
        result_range = tuple(date_range)
        # Attach a flag indicating strict operators were used.  ArcticDB's
        # date_range is always inclusive, so strict < / > need DuckDB to
        # apply the final exclusion.
        return result_range, remaining, has_strict_date_op

    return None, parsed_filters, False


_ONLY_SELECT_ERROR = "Only SELECT statements can be serialized to json!"


def _get_sql_ast(query: str) -> Optional[Dict]:
    """
    Parse SQL into AST using DuckDB's json_serialize_sql function.

    Parameters
    ----------
    query : str
        SQL query to parse.

    Returns
    -------
    dict or None
        Parsed AST dictionary, or None if parsing fails.
    """
    import duckdb

    try:
        conn = duckdb.connect(":memory:")
        try:
            result = conn.execute("SELECT json_serialize_sql(?)", [query]).fetchone()
            if result:
                ast = json.loads(result[0])
                if not ast.get("error"):
                    return ast
                else:
                    logger.debug("DuckDB returned error parsing SQL: %s", ast.get("error_message", ""))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("Failed to parse SQL to AST: %s", e)

    return None


def _get_sql_ast_or_raise(query: str) -> Dict:
    """
    Parse SQL into AST, raising ValueError if the query is not a supported SELECT-like statement.

    DuckDB's json_serialize_sql() only accepts SELECT-like statements (SELECT, WITH, SHOW,
    DESCRIBE). Non-SELECT statements (INSERT, UPDATE, DELETE, CREATE, DROP, etc.) produce
    a specific error that we translate into a clear user-facing message.
    """
    import duckdb

    try:
        conn = duckdb.connect(":memory:")
        try:
            result = conn.execute("SELECT json_serialize_sql(?)", [query]).fetchone()
            if result:
                ast = json.loads(result[0])
                if not ast.get("error"):
                    return ast
                error_message = ast.get("error_message", "")
                if _ONLY_SELECT_ERROR in error_message:
                    raise ValueError(
                        "Unsupported SQL statement. "
                        "ArcticDB's SQL interface is read-only. "
                        "Only SELECT, SHOW, DESCRIBE, and WITH (CTE) queries are supported. "
                        "To write data, use lib.write() or lib.update()."
                    )
                else:
                    raise ValueError(f"Could not parse SQL query: {error_message}")
        finally:
            conn.close()
    except ValueError:
        raise
    except Exception as e:
        logger.warning("Failed to parse SQL to AST: %s", e)

    raise ValueError("Could not parse SQL query. Ensure query is valid SQL.")


def is_table_discovery_query(query: str, _ast: Optional[Dict] = None) -> bool:
    """
    Check if a SQL query is a table discovery query (SHOW TABLES, SHOW ALL TABLES).

    Uses DuckDB's AST parser to detect these queries rather than string matching.

    Parameters
    ----------
    query : str
        SQL query to check.
    _ast : dict, optional
        Pre-parsed AST to avoid re-parsing.  Internal use only.

    Returns
    -------
    bool
        True if the query is SHOW TABLES or SHOW ALL TABLES, False otherwise.
    """
    ast = _ast if _ast is not None else _get_sql_ast(query)
    if ast is None:
        return False

    try:
        statements = ast.get("statements", [])
        if not statements:
            return False

        node = statements[0].get("node", {})
        from_table = node.get("from_table", {})

        # SHOW TABLES and SHOW ALL TABLES are parsed as SELECT with SHOW_REF from_table
        if from_table.get("type") == "SHOW_REF":
            table_name = from_table.get("table_name", "")
            # SHOW TABLES -> table_name = '"tables"'
            # SHOW ALL TABLES -> table_name = '__show_tables_expanded'
            if table_name in ('"tables"', "__show_tables_expanded"):
                return True

        return False
    except (ValueError, KeyError, TypeError, IndexError) as e:
        logger.debug("Failed to check if query is table discovery: %s", e)
        return False


def is_database_discovery_query(query: str) -> bool:
    """
    Check if a SQL query is a database discovery query (SHOW DATABASES).

    Uses DuckDB's AST parser to detect this query rather than string matching.

    Parameters
    ----------
    query : str
        SQL query to check.

    Returns
    -------
    bool
        True if the query is SHOW DATABASES, False otherwise.
    """
    ast = _get_sql_ast(query)
    if ast is None:
        return False

    try:
        statements = ast.get("statements", [])
        if not statements:
            return False

        node = statements[0].get("node", {})
        from_table = node.get("from_table", {})

        # SHOW DATABASES is parsed as SELECT with SHOW_REF from_table
        if from_table.get("type") == "SHOW_REF":
            table_name = from_table.get("table_name", "")
            # SHOW DATABASES -> table_name = '"databases"'
            if table_name == '"databases"':
                return True

        return False
    except (ValueError, KeyError, TypeError, IndexError) as e:
        logger.debug("Failed to check if query is database discovery: %s", e)
        return False


def extract_pushdown_from_sql(
    query: str,
    table_names: Optional[List[str]] = None,
    index_columns: Optional[List[str]] = None,
) -> Tuple[Dict[str, PushdownInfo], List[str]]:
    """
    Parse SQL and extract pushdown information for each table using AST parsing.

    This function uses DuckDB's json_serialize_sql() to parse the SQL without
    needing any tables to be registered. It extracts columns, filters, date ranges,
    and LIMIT directly from the AST.

    Parameters
    ----------
    query : str
        SQL query to analyze.
    table_names : list, optional
        List of table names to extract pushdown info for.
        If None, table names are extracted from the query.
    index_columns : list of str, optional
        Column names that correspond to the ArcticDB index (e.g. ``["Date"]``).
        SQL filters on these columns are converted to ``date_range`` pushdown
        instead of value filters, enabling segment-level skipping in storage.

    Returns
    -------
    tuple
        (mapping of table_name -> PushdownInfo, list of extracted symbols)

    Raises
    ------
    ValueError
        If no symbols could be extracted from the query.
    """
    # _get_sql_ast_or_raise handles both validation (rejects non-SELECT statements
    # like INSERT/UPDATE/DELETE with a clear error) and parsing in a single DuckDB call.
    ast = _get_sql_ast_or_raise(query)

    # Extract table alias mapping
    table_alias_map = _extract_tables_from_ast(ast)

    # Extract unique symbols from the alias map
    seen = set()
    extracted_symbols = []
    for tbl_name in table_alias_map.values():
        if tbl_name.lower() not in seen:
            seen.add(tbl_name.lower())
            extracted_symbols.append(tbl_name)

    if not extracted_symbols:
        raise ValueError(
            "Could not extract symbol names from query. "
            "Ensure query contains FROM or JOIN clauses with symbol names, "
            "or use duckdb() to register symbols explicitly."
        )

    # Use provided table_names or extracted ones
    if table_names is None:
        table_names = extracted_symbols

    result = {}

    # Initialize default PushdownInfo for each table
    for table in table_names:
        result[table] = PushdownInfo()

    # Ensure all requested tables are in the alias map
    for table in table_names:
        if table.lower() not in table_alias_map:
            table_alias_map[table.lower()] = table

    # Get SELECT node
    try:
        select_node = ast["statements"][0]["node"]
    except (KeyError, IndexError):
        return result, extracted_symbols

    # Extract LIMIT
    limit = _extract_limit_from_ast(ast)

    # Extract columns from SELECT list
    select_list = select_node.get("select_list", [])

    # Check for SELECT * and whether the SELECT list is simple (only column refs or *)
    is_select_star = False
    is_simple_select = True
    for item in select_list:
        if item.get("class") == "STAR":
            is_select_star = True
        elif item.get("class") == "COLUMN_REF":
            pass  # Simple column reference
        else:
            is_simple_select = False

    # Disable column/filter pushdown for complex queries where the outer SELECT/WHERE
    # doesn't reflect all columns needed:
    # - Multi-table (JOINs): JOIN conditions may reference columns not in SELECT/WHERE
    # - CTEs (WITH): the CTE body may reference columns not visible in the outer query
    is_multi_table = len(table_names) > 1
    has_ctes = bool(_extract_cte_names(ast))
    disable_pushdown = is_multi_table or has_ctes

    # Determine whether LIMIT can safely be pushed to storage as row_range.
    # This is only safe for simple scans where the first N storage rows are the
    # first N result rows.  It is NOT safe when:
    #   - ORDER BY: DuckDB needs all rows to sort before applying LIMIT
    #   - GROUP BY / DISTINCT: LIMIT applies to aggregated/deduplicated result
    #   - Multi-table / CTEs: LIMIT applies to joined/composed result
    #   - WHERE clause: value filters may discard rows, so the first N storage
    #     rows may yield fewer than N result rows (date_range is fine — it
    #     restricts the scan window but doesn't reduce count within it)
    has_group_by = bool(select_node.get("group_expressions"))
    has_distinct = any(m.get("type") == "DISTINCT_MODIFIER" for m in select_node.get("modifiers", []))
    has_where = select_node.get("where_clause") is not None
    can_push_limit = limit is not None and not (
        disable_pushdown or _has_order_by(ast) or has_group_by or has_distinct or has_where
    )

    if not is_select_star and not disable_pushdown:
        # Extract specific columns only for simple single-table queries
        select_columns = _extract_columns_from_select_list(select_list, table_alias_map)
    else:
        select_columns = {}

    # Extract columns and filters from WHERE clause (only for simple single-table queries)
    where_clause = select_node.get("where_clause")
    if where_clause and not disable_pushdown:
        where_columns = _extract_columns_from_where(where_clause, table_alias_map)
    else:
        where_columns = {}

    # Parse filters from WHERE clause
    parsed_filters = _ast_to_filters(where_clause) if where_clause else []
    # Track whether the parser could handle all WHERE conditions.
    # _ast_to_filters silently drops unparseable nodes (OR, FUNCTION, etc.),
    # so a non-empty WHERE with no parsed filters means some conditions were lost.
    all_where_conditions_parsed = where_clause is None or bool(parsed_filters)

    # Extract date range from index filters
    date_range, remaining_filters, has_strict_date_op = _extract_date_range(parsed_filters, index_columns=index_columns)

    # Build QueryBuilder from remaining filters
    query_builder = _build_query_builder(remaining_filters) if remaining_filters else None

    # Build PushdownInfo for each table
    for table in table_names:
        pushdown = PushdownInfo()

        # Merge columns from SELECT and WHERE for this table
        table_columns = set()
        if not is_select_star:
            table_columns.update(select_columns.get(table, set()))
        table_columns.update(where_columns.get(table, set()))

        if table_columns and not is_select_star:
            pushdown.columns = list(table_columns)
            pushdown.columns_pushed_down = list(table_columns)

        # Apply date range
        if date_range:
            pushdown.date_range = date_range
            pushdown.date_range_pushed_down = True

        # Apply query builder
        if query_builder:
            pushdown.query_builder = query_builder
            pushdown.filter_pushed_down = True

        # Apply LIMIT — only push to storage when safe (see can_push_limit above)
        if can_push_limit:
            pushdown.limit = limit
            pushdown.limit_pushed_down = limit

        # Determine if the entire query can be handled by ArcticDB's eager
        # read path (date_range + columns + query_builder), making DuckDB
        # unnecessary.  This is only possible for simple single-table queries
        # with no GROUP BY, ORDER BY, DISTINCT, LIMIT, JOINs, or CTEs, and
        # where all WHERE filters were successfully pushed to the QueryBuilder.
        # Strict date operators (< / >) also require DuckDB because ArcticDB's
        # date_range is always inclusive.
        all_filters_pushed = not pushdown.unpushed_filters
        no_complex_ops = not (is_multi_table or has_ctes or has_group_by or has_distinct or _has_order_by(ast))
        no_limit = limit is None
        pushdown.fully_pushed = (
            no_complex_ops
            and all_filters_pushed
            and no_limit
            and not has_strict_date_op
            and is_simple_select
            and all_where_conditions_parsed
        )

        result[table] = pushdown

    return result, extracted_symbols
