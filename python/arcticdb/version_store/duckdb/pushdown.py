"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from arcticdb.version_store.processing import QueryBuilder


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
    except Exception:
        return None


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

    def extract_column_refs(node: Dict) -> None:
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
                extract_column_refs(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        extract_column_refs(item)

    for item in select_list:
        extract_column_refs(item)

    return columns_by_table


def _extract_columns_from_where(where_clause: Dict, table_alias_map: Dict[str, str]) -> Dict[str, Set[str]]:
    """Extract column references from WHERE clause, grouped by table."""
    columns_by_table: Dict[str, Set[str]] = {}

    def extract_column_refs(node: Dict) -> None:
        if not isinstance(node, dict):
            return

        if node.get("class") == "COLUMN_REF":
            col_names = node.get("column_names", [])
            if col_names:
                if len(col_names) >= 2:
                    table_ref = col_names[0]
                    col_name = col_names[-1]
                    table_name = table_alias_map.get(table_ref.lower(), table_ref)
                else:
                    col_name = col_names[0]
                    table_name = None

                if table_name:
                    if table_name not in columns_by_table:
                        columns_by_table[table_name] = set()
                    columns_by_table[table_name].add(col_name)
                else:
                    for tbl in table_alias_map.values():
                        if tbl not in columns_by_table:
                            columns_by_table[tbl] = set()
                        columns_by_table[tbl].add(col_name)
            return

        for key, value in node.items():
            if isinstance(value, dict):
                extract_column_refs(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        extract_column_refs(item)

    if where_clause:
        extract_column_refs(where_clause)

    return columns_by_table


def _extract_tables_from_ast(ast: Dict) -> Dict[str, str]:
    """
    Extract table references from AST, returning mapping of alias -> table_name.

    Handles FROM clause and JOINs.
    """
    alias_map: Dict[str, str] = {}

    try:
        statements = ast.get("statements", [])
        if not statements:
            return alias_map

        node = statements[0].get("node", {})
        from_table = node.get("from_table", {})

        def extract_tables(table_node: Dict) -> None:
            if not isinstance(table_node, dict):
                return

            node_type = table_node.get("type", "")

            if node_type == "BASE_TABLE":
                table_name = table_node.get("table_name", "")
                alias = table_node.get("alias", "") or table_name
                if table_name:
                    alias_map[alias.lower()] = table_name
                    alias_map[table_name.lower()] = table_name

            elif node_type in ("JOIN", "INNER_JOIN", "LEFT_JOIN", "RIGHT_JOIN", "FULL_JOIN", "CROSS_JOIN"):
                left = table_node.get("left", {})
                right = table_node.get("right", {})
                extract_tables(left)
                extract_tables(right)

            elif node_type == "SUBQUERY":
                # For subqueries, we don't push down
                pass

        extract_tables(from_table)

    except Exception:
        pass

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
                case "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "UINTEGER" | "UBIGINT" | "USMALLINT" | "UTINYINT" | "HUGEINT" | "UHUGEINT":
                    return int(raw_value)
                case "FLOAT" | "DOUBLE" | "REAL":
                    return float(raw_value)
                case "DECIMAL":
                    # DECIMAL stores value as integer, need to apply scale
                    type_info = value_info.get("type", {}).get("type_info", {})
                    scale = type_info.get("scale", 0)
                    return float(raw_value) / (10 ** scale)
                case "BOOLEAN":
                    return bool(raw_value)
                case "VARCHAR":
                    return raw_value
                case "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP_NS" | "TIMESTAMP_MS" | "TIMESTAMP_S" | "DATE":
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

        except Exception:
            # Skip filters that can't be converted
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
        # Use a connection to call json_serialize_sql as a SQL function
        conn = duckdb.connect(":memory:")
        try:
            result = conn.execute("SELECT json_serialize_sql(?)", [query]).fetchone()
            if result:
                ast = json.loads(result[0])
                if not ast.get("error"):
                    return ast
        finally:
            conn.close()
    except Exception:
        pass

    return None


def extract_pushdown_from_sql(query: str, table_names: List[str]) -> Dict[str, PushdownInfo]:
    """
    Parse SQL and extract pushdown information for each table using AST parsing.

    This function uses DuckDB's json_serialize_sql() to parse the SQL without
    needing any tables to be registered. It extracts columns, filters, date ranges,
    and LIMIT directly from the AST.

    Parameters
    ----------
    query : str
        SQL query to analyze.
    table_names : list
        List of table names to extract pushdown info for.

    Returns
    -------
    dict
        Mapping of table_name -> PushdownInfo
    """
    result = {}

    # Initialize default PushdownInfo for each table
    for table in table_names:
        result[table] = PushdownInfo()

    ast = _get_sql_ast(query)
    if ast is None:
        return result

    # Extract table alias mapping
    table_alias_map = _extract_tables_from_ast(ast)

    # Ensure all requested tables are in the alias map
    for table in table_names:
        if table.lower() not in table_alias_map:
            table_alias_map[table.lower()] = table

    # Get SELECT node
    try:
        select_node = ast["statements"][0]["node"]
    except (KeyError, IndexError):
        return result

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

    if not is_select_star:
        # Extract specific columns
        select_columns = _extract_columns_from_select_list(select_list, table_alias_map)
    else:
        select_columns = {}

    # Extract columns and filters from WHERE clause
    where_clause = select_node.get("where_clause")
    where_columns = _extract_columns_from_where(where_clause, table_alias_map) if where_clause else {}

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

    return result
