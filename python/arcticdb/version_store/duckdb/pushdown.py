"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from arcticdb.version_store.processing import QueryBuilder

logger = logging.getLogger(__name__)


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


def _extract_tables_from_ast(ast: Dict) -> Dict[str, str]:
    """
    Extract table references from AST, returning mapping of alias -> table_name.

    Handles FROM clause, JOINs, and DDL queries like DESCRIBE/SHOW.
    """
    alias_map: Dict[str, str] = {}

    def extract_tables_recursive(node: Any) -> None:
        """Recursively search for BASE_TABLE nodes anywhere in the AST."""
        if not isinstance(node, dict):
            return

        node_type = node.get("type", "")

        if node_type == "BASE_TABLE":
            table_name = node.get("table_name", "")
            alias = node.get("alias", "") or table_name
            if table_name:
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

    match (node_class, node_type):
        case ("CONJUNCTION", "CONJUNCTION_AND"):
            # Flatten AND conjunctions into list of filters
            filters = []
            for child in node.get("children", []):
                child_filters = _ast_to_filters(child)
                if child_filters:
                    filters.extend(child_filters)
            return filters

        case ("CONJUNCTION", "CONJUNCTION_OR"):
            # OR conjunctions cannot be pushed to ArcticDB
            return []

        case ("COMPARISON", _):
            return _parse_comparison_node(node)

        case ("OPERATOR", "OPERATOR_IS_NULL"):
            return _parse_null_check_node(node, is_not=False)

        case ("OPERATOR", "OPERATOR_IS_NOT_NULL"):
            return _parse_null_check_node(node, is_not=True)

        case ("OPERATOR", "COMPARE_IN"):
            return _parse_in_node(node, is_not=False)

        case ("OPERATOR", "COMPARE_NOT_IN"):
            return _parse_in_node(node, is_not=True)

        case ("BETWEEN", "COMPARE_BETWEEN"):
            return _parse_between_node(node)

        case ("FUNCTION", _):
            # Functions (like LIKE, UPPER, etc.) cannot be pushed down
            return []

        case _:
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

    return [{
        "column": column,
        "op": op,
        "value": value,
        "type": "comparison",
    }]


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

    return [{
        "column": column,
        "op": "IS NOT NULL" if is_not else "IS NULL",
        "value": None,
        "type": "null_check",
    }]


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

    return [{
        "column": column,
        "op": "NOT IN" if is_not else "IN",
        "value": values,
        "type": "membership",
    }]


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

    return [{
        "column": column,
        "op": "BETWEEN",
        "value": (lower, upper),
        "type": "range",
    }]


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

    # Handle CAST nodes (e.g., '2024-01-01'::TIMESTAMP_NS)
    match (node_class, node_type):
        case ("CAST", "OPERATOR_CAST"):
            child = node.get("child", {})
            cast_type = node.get("cast_type", {}).get("id", "")

            child_value = _extract_constant_value(child)
            if child_value is None:
                return None

            match cast_type:
                case "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP_NS" | "TIMESTAMP_MS" | "TIMESTAMP_S" | "DATE":
                    return _convert_to_timestamp(child_value)
                case "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "UINTEGER" | "UBIGINT" | "USMALLINT" | "UTINYINT":
                    try:
                        return int(child_value)
                    except (ValueError, TypeError):
                        return None
                case "FLOAT" | "DOUBLE" | "REAL":
                    try:
                        return float(child_value)
                    except (ValueError, TypeError):
                        return None
                case _:
                    return child_value

        case ("CONSTANT", "VALUE_CONSTANT"):
            value_info = node.get("value", {})
            if value_info.get("is_null"):
                return None

            type_id = value_info.get("type", {}).get("id", "")
            raw_value = value_info.get("value")

            if raw_value is None:
                return None

            match type_id:
                case (
                    "INTEGER"
                    | "BIGINT"
                    | "SMALLINT"
                    | "TINYINT"
                    | "UINTEGER"
                    | "UBIGINT"
                    | "USMALLINT"
                    | "UTINYINT"
                    | "HUGEINT"
                    | "UHUGEINT"
                ):
                    return int(raw_value)
                case "FLOAT" | "DOUBLE" | "REAL":
                    return float(raw_value)
                case "DECIMAL":
                    # DECIMAL stores value as integer, need to apply scale
                    type_info = value_info.get("type", {}).get("type_info", {})
                    scale = type_info.get("scale", 0)
                    return float(raw_value) / (10**scale)
                case "BOOLEAN":
                    return bool(raw_value)
                case "VARCHAR":
                    return raw_value
                case (
                    "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP_NS" | "TIMESTAMP_MS" | "TIMESTAMP_S" | "DATE"
                ):
                    return _convert_to_timestamp(raw_value)
                case _:
                    return raw_value

        case _:
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
            match (ftype, op):
                case ("comparison", "="):
                    expr = q[col] == value
                case ("comparison", "!="):
                    expr = q[col] != value
                case ("comparison", "<"):
                    expr = q[col] < value
                case ("comparison", ">"):
                    expr = q[col] > value
                case ("comparison", "<="):
                    expr = q[col] <= value
                case ("comparison", ">="):
                    expr = q[col] >= value
                case ("membership", "IN"):
                    expr = q[col].isin(*value)
                case ("membership", "NOT IN"):
                    expr = q[col].isnotin(*value)
                case ("null_check", "IS NULL"):
                    expr = q[col].isnull()
                case ("null_check", "IS NOT NULL"):
                    expr = q[col].notnull()
                case ("range", _):
                    low, high = value
                    expr = (q[col] >= low) & (q[col] <= high)
                case _:
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


def _extract_date_range(parsed_filters: List[Dict]) -> Tuple[Optional[Tuple[Any, Any]], List[Dict]]:
    """
    Extract date_range from filters on 'index' column.

    Returns
    -------
    tuple
        (date_range or None, remaining filters)
    """
    import pandas as pd

    date_range = [None, None]
    remaining = []

    for f in parsed_filters:
        col = f.get("column", "").lower()
        if col != "index":
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
                elif op in ("<=", "<"):
                    date_range[1] = ts
                else:
                    remaining.append(f)
            else:
                remaining.append(f)
        except (ValueError, TypeError):
            remaining.append(f)

    if date_range[0] is not None or date_range[1] is not None:
        return tuple(date_range), remaining

    return None, parsed_filters


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
                    logger.debug("DuckDB returned error parsing SQL: %s", ast.get("error"))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("Failed to parse SQL to AST: %s", e)

    return None


def is_table_discovery_query(query: str) -> bool:
    """
    Check if a SQL query is a table discovery query (SHOW TABLES, SHOW ALL TABLES).

    Uses DuckDB's AST parser to detect these queries rather than string matching.

    Parameters
    ----------
    query : str
        SQL query to check.

    Returns
    -------
    bool
        True if the query is SHOW TABLES or SHOW ALL TABLES, False otherwise.
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

    Returns
    -------
    tuple
        (mapping of table_name -> PushdownInfo, list of extracted symbols)

    Raises
    ------
    ValueError
        If no symbols could be extracted from the query.
    """
    ast = _get_sql_ast(query)
    if ast is None:
        raise ValueError(
            "Could not parse SQL query. " "Ensure query is valid SQL, or use duckdb() to register symbols explicitly."
        )

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

    # Check for SELECT *
    is_select_star = False
    for item in select_list:
        if item.get("class") == "STAR":
            is_select_star = True
            break

    # For multi-table queries (JOINs), disable column pushdown since JOIN conditions
    # may reference columns not in SELECT/WHERE. Extracting JOIN condition columns
    # is complex and error-prone, so we conservatively read all columns.
    is_multi_table = len(table_names) > 1

    if not is_select_star and not is_multi_table:
        # Extract specific columns only for single-table queries
        select_columns = _extract_columns_from_select_list(select_list, table_alias_map)
    else:
        select_columns = {}

    # Extract columns and filters from WHERE clause (only for single-table queries)
    where_clause = select_node.get("where_clause")
    if where_clause and not is_multi_table:
        where_columns = _extract_columns_from_where(where_clause, table_alias_map)
    else:
        where_columns = {}

    # Parse filters from WHERE clause
    parsed_filters = _ast_to_filters(where_clause) if where_clause else []

    # Extract date range from index filters
    date_range, remaining_filters = _extract_date_range(parsed_filters)

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

        # Apply LIMIT
        if limit is not None:
            pushdown.limit = limit
            pushdown.limit_pushed_down = limit

        result[table] = pushdown

    return result, extracted_symbols
