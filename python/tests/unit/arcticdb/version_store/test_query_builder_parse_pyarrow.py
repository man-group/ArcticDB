import datetime
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from arcticdb.version_store.processing import QueryBuilder, ExpressionNode
from arcticdb.util.test import assert_frame_equal


def df_with_all_column_types(num_rows=100):
    data = {
        "int_col": np.arange(num_rows, dtype=np.int64),
        "float_col": [np.nan if i%20==5 else i for i in range(num_rows)],
        "str_col": [f"str_{i}" for i in range(num_rows)],
        "bool_col": [i%2 == 0 for i in range(num_rows)],
        "datetime_col": pd.date_range(start=pd.Timestamp(2025, 1, 1), periods=num_rows)
    }
    index = pd.date_range(start=pd.Timestamp(2025, 1, 1), periods=num_rows)
    return pd.DataFrame(data=data, index=index)


def compare_against_pyarrow(pyarrow_expr_str, expected_adb_qb, lib, function_map = None, expect_equal=True):
    adb_expr = ExpressionNode.from_pyarrow_expression_str(pyarrow_expr_str, function_map)
    q = QueryBuilder()
    q = q[adb_expr]
    assert q == expected_adb_qb
    pa_expr = eval(pyarrow_expr_str)

    # Setup
    sym = "sym"
    df = df_with_all_column_types()
    lib.write(sym, df)
    pa_table = pa.Table.from_pandas(df)

    # Apply filter to adb
    adb_result = lib.read(sym, query_builder=q).data

    # Apply filter to pyarrow
    pa_result = pa_table.filter(pa_expr).to_pandas()

    if expect_equal:
        assert_frame_equal(adb_result, pa_result)
    else:
        assert len(adb_result) != len(pa_result)


def test_basic_filters(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    q = QueryBuilder()

    # Filter by boolean column
    expr = f"pc.field('bool_col')"
    expected_q = q[q['bool_col']]
    compare_against_pyarrow(expr, expected_q, lib)

    # Filter by comparison
    for op in ["<", "<=", "==", ">=", ">"]:
        expr = f"pc.field('int_col') {op} 50"
        expected_q = q[eval(f"q['int_col'] {op} 50")]
        compare_against_pyarrow(expr, expected_q, lib)

    # Filter with unary operators
    expr = "~pc.field('bool_col')"
    expected_q = q[~q['bool_col']]
    compare_against_pyarrow(expr, expected_q, lib)

    # Filter with binary operators
    for op in ["+", "-", "*", "/"]:
        expr = f"pc.field('float_col') {op} 5.0 < 50.0"
        expected_q = q[eval(f"q['float_col'] {op} 5.0 < 50.0")]
        compare_against_pyarrow(expr, expected_q, lib)

    for op in ["&", "|"]:
        expr = f"pc.field('bool_col') {op} (pc.field('int_col') < 50)"
        expected_q = q[eval(f"q['bool_col'] {op} (q['int_col'] < 50)")]
        compare_against_pyarrow(expr, expected_q, lib)

    # Filter with expression method calls
    expr = "pc.field('str_col').isin(['str_0', 'str_10', 'str_20'])"
    expected_q = q[q['str_col'].isin(['str_0', 'str_10', 'str_20'])]
    compare_against_pyarrow(expr, expected_q, lib)

    expr = "pc.field('str_col').isin(('str_0', 'str_10', 'str_20'))"
    expected_q = q[q['str_col'].isin(('str_0', 'str_10', 'str_20'))]
    compare_against_pyarrow(expr, expected_q, lib)

    expr = "pc.field('str_col').isin({'str_0', 'str_10', 'str_20'})"
    expected_q = q[q['str_col'].isin({'str_0', 'str_10', 'str_20'})]
    compare_against_pyarrow(expr, expected_q, lib)

    expr = "pc.field('float_col').is_nan()"
    expected_q = q[q['float_col'].isnull()]
    # We expect a different result between adb and pyarrow because of the different nan/null handling
    compare_against_pyarrow(expr, expected_q, lib, expect_equal=False)

    expr = "pc.field('float_col').is_null()"
    expected_q = q[q['float_col'].isnull()]
    compare_against_pyarrow(expr, expected_q, lib)

    expr = "pc.field('float_col').is_valid()"
    expected_q = q[q['float_col'].notnull()]
    compare_against_pyarrow(expr, expected_q, lib)

def test_complex_filters(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    q = QueryBuilder()

    # Nested complex filters
    expr = "((pc.field('float_col') * 2) > 20.0) & (pc.field('int_col') <= pc.scalar(60)) | pc.field('bool_col')"
    expected_q = q[(q['float_col'] * 2 > 20.0) & (q['int_col'] <= 60) | q['bool_col']]
    compare_against_pyarrow(expr, expected_q, lib)

    expr = "((pc.field('float_col') / 2) > 20.0) & (pc.field('float_col') <= pc.scalar(60)) & pc.field('str_col').isin(['str_30', 'str_41', 'str_42', 'str_53', 'str_99'])"
    expected_q = q[(q['float_col'] / 2 > 20.0) & (q['float_col'] <= 60) & q['str_col'].isin(['str_30', 'str_41', 'str_42', 'str_53', 'str_99'])]
    compare_against_pyarrow(expr, expected_q, lib)

    # Filters with function calls
    function_map = {
        "datetime.datetime": datetime.datetime,
        "abs": abs,
    }
    expr = "pc.field('datetime_col') < datetime.datetime(2025, 1, 20)"
    expected_q = q[q['datetime_col'] < datetime.datetime(2025, 1, 20)]
    compare_against_pyarrow(expr, expected_q, lib, function_map)

    expr = "(pc.field('datetime_col') < datetime.datetime(2025, 1, abs(-20))) & (pc.field('int_col') >= abs(-5))"
    expected_q = q[(q['datetime_col'] < datetime.datetime(2025, 1, abs(-20))) & (q['int_col'] >= abs(-5))]
    compare_against_pyarrow(expr, expected_q, lib, function_map)

def test_broken_filters():
    # ill-formated filter
    expr = "pc.field('float_col'"
    with pytest.raises(ValueError):
        ExpressionNode.from_pyarrow_expression_str(expr)

    # pyarrow expressions only support single comparisons
    expr = "1 < pc.field('int_col') < 10"
    with pytest.raises(ValueError):
        ExpressionNode.from_pyarrow_expression_str(expr)

    # calling a mising function
    expr = "some.missing.function(5)"
    with pytest.raises(ValueError):
        ExpressionNode.from_pyarrow_expression_str(expr)
