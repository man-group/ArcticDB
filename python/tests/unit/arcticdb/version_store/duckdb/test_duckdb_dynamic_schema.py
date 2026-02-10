"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")


def _write_dynamic_schema_symbol(lib, symbol="sym"):
    """Write a symbol where different segments have different column subsets.

    Segment 1: columns a, b
    Segment 2: columns b, c
    Result should have columns a, b, c with nulls where columns are absent.
    """
    idx1 = pd.date_range("2024-01-01", periods=5, freq="D")
    df1 = pd.DataFrame({"a": np.arange(5, dtype=np.float64), "b": np.arange(10, 15, dtype=np.float64)}, index=idx1)
    lib.write(symbol, df1)

    idx2 = pd.date_range("2024-01-06", periods=5, freq="D")
    df2 = pd.DataFrame({"b": np.arange(20, 25, dtype=np.float64), "c": np.arange(30, 35, dtype=np.float64)}, index=idx2)
    lib.append(symbol, df2)

    return df1, df2


class TestSqlDynamicSchema:
    """Tests for SQL queries on symbols with dynamic schema (different columns per segment)."""

    def test_sql_select_all(self, lmdb_library_dynamic_schema):
        """SELECT * returns all columns across all segments, with nulls for missing columns."""
        lib = lmdb_library_dynamic_schema
        df1, df2 = _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT * FROM sym ORDER BY index")

        assert len(result) == 10
        assert set(result.columns) >= {"a", "b", "c"}
        # First 5 rows: a and b have values, c is null
        assert not result["a"].iloc[:5].isna().any()
        assert not result["b"].iloc[:5].isna().any()
        assert result["c"].iloc[:5].isna().all()
        # Last 5 rows: b and c have values, a is null
        assert result["a"].iloc[5:].isna().all()
        assert not result["b"].iloc[5:].isna().any()
        assert not result["c"].iloc[5:].isna().any()

    def test_sql_select_shared_column(self, lmdb_library_dynamic_schema):
        """Selecting a column present in all segments works correctly."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT b FROM sym")

        assert len(result) == 10
        assert list(result.columns) == ["b"]
        assert not result["b"].isna().any()

    def test_sql_select_sparse_column(self, lmdb_library_dynamic_schema):
        """Selecting a column present in only some segments returns values and nulls."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT index, a FROM sym ORDER BY index")

        assert len(result) == 10
        assert not result["a"].iloc[:5].isna().any()
        assert result["a"].iloc[5:].isna().all()

    def test_sql_filter_on_shared_column(self, lmdb_library_dynamic_schema):
        """WHERE filter on a column present in all segments works."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT * FROM sym WHERE b > 15 ORDER BY index")

        assert len(result) > 0
        assert (result["b"] > 15).all()

    def test_sql_filter_on_sparse_column(self, lmdb_library_dynamic_schema):
        """WHERE filter on a column present in only some segments doesn't crash."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT * FROM sym WHERE c > 31 ORDER BY index")

        assert len(result) > 0
        assert (result["c"] > 31).all()

    def test_sql_aggregation(self, lmdb_library_dynamic_schema):
        """GROUP BY aggregation works across dynamic schema segments."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT SUM(b) as total_b FROM sym")

        expected_b = sum(range(10, 15)) + sum(range(20, 25))
        assert result["total_b"].iloc[0] == expected_b

    def test_sql_aggregation_sparse_column(self, lmdb_library_dynamic_schema):
        """SUM on a sparse column ignores null segments."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT SUM(a) as total_a FROM sym")

        expected_a = sum(range(5))
        assert result["total_a"].iloc[0] == expected_a

    def test_sql_group_by_with_column_projection(self, lmdb_library_dynamic_schema):
        """GROUP BY + SUM with column projection on dynamic schema returns correct results.

        Regression test: passing dynamic_schema=True to the C++ layer disables the
        column-slice filter. If the static col filter were incorrectly applied with
        dynamic_schema, it would produce 0 results because the bitset coordinate
        system doesn't match the index segment's start_col/end_col for dynamic schema.
        """
        lib = lmdb_library_dynamic_schema
        # Segment 1: columns a, b (rows 0-4)
        # Segment 2: columns b, c (rows 5-9)
        _write_dynamic_schema_symbol(lib)

        # Column projection pushes [b] — both segments have b
        result = lib.sql("SELECT SUM(b) as total_b FROM sym")
        expected_b = sum(range(10, 15)) + sum(range(20, 25))
        assert len(result) == 1
        assert result["total_b"].iloc[0] == expected_b

    def test_sql_group_by_non_column_sliced_dynamic_schema(self, lmdb_library_dynamic_schema):
        """GROUP BY on a wider dynamic-schema symbol without column slicing works correctly.

        Writes multiple segments with different column subsets but enough columns
        to verify column projection doesn't break. The library uses dynamic_schema=True
        with no column slicing (columns_per_segment=127 >> actual column count).
        """
        lib = lmdb_library_dynamic_schema
        idx1 = pd.date_range("2024-01-01", periods=50, freq="h")
        cats = ["X", "Y", "Z"]
        rng = np.random.default_rng(99)
        df1 = pd.DataFrame(
            {"cat": rng.choice(cats, 50), "val": rng.uniform(1, 100, 50), "extra1": rng.standard_normal(50)},
            index=idx1,
        )
        lib.write("ds", df1)

        idx2 = pd.date_range("2024-01-03T02:00", periods=50, freq="h")
        df2 = pd.DataFrame(
            {"cat": rng.choice(cats, 50), "val": rng.uniform(1, 100, 50), "extra2": rng.standard_normal(50)},
            index=idx2,
        )
        lib.append("ds", df2)

        # GROUP BY + SUM with column projection on [cat, val]
        result = lib.sql("SELECT cat, SUM(val) AS total FROM ds GROUP BY cat ORDER BY cat")

        full = pd.concat([df1, df2])
        expected = full.groupby("cat")["val"].sum().reset_index(name="total").sort_values("cat").reset_index(drop=True)
        assert len(result) == len(expected), f"Expected {len(expected)} groups, got {len(result)}"
        assert list(result["cat"]) == list(expected["cat"])
        np.testing.assert_allclose(result["total"].values, expected["total"].values, rtol=1e-10)


class TestDuckDBContextDynamicSchema:
    """Tests for lib.duckdb() context manager with dynamic schema symbols."""

    def test_context_select_all(self, lmdb_library_dynamic_schema):
        """DuckDB context manager works with dynamic schema symbols."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        with lib.duckdb() as ctx:
            ctx.register_symbol("sym")
            result = ctx.sql("SELECT * FROM sym ORDER BY index")

        assert len(result) == 10
        assert set(result.columns) >= {"a", "b", "c"}

    def test_context_with_date_range(self, lmdb_library_dynamic_schema):
        """Date range filtering works with dynamic schema in DuckDB context."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        with lib.duckdb() as ctx:
            ctx.register_symbol("sym", date_range=(pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-08")))
            result = ctx.sql("SELECT * FROM sym ORDER BY index")

        assert len(result) > 0
        assert len(result) < 10


class TestDynamicSchemaWithStrings:
    """Tests for dynamic schema with string columns."""

    def test_sql_string_columns(self, lmdb_library_dynamic_schema):
        """Dynamic schema works with string columns."""
        lib = lmdb_library_dynamic_schema
        idx1 = pd.date_range("2024-01-01", periods=3, freq="D")
        df1 = pd.DataFrame({"name": ["alice", "bob", "carol"], "val": [1.0, 2.0, 3.0]}, index=idx1)
        lib.write("sym", df1)

        idx2 = pd.date_range("2024-01-04", periods=3, freq="D")
        df2 = pd.DataFrame({"val": [4.0, 5.0, 6.0], "tag": ["x", "y", "z"]}, index=idx2)
        lib.append("sym", df2)

        result = lib.sql("SELECT * FROM sym ORDER BY index")

        assert len(result) == 6
        assert set(result.columns) >= {"name", "val", "tag"}
        # name: values in first 3 rows, null in last 3
        assert not result["name"].iloc[:3].isna().any()
        assert result["name"].iloc[3:].isna().all()
        # tag: null in first 3 rows, values in last 3
        assert result["tag"].iloc[:3].isna().all()
        assert not result["tag"].iloc[3:].isna().any()


class TestWideTableColumnSliceMerging:
    """Tests for SQL on wide tables where ArcticDB splits columns across multiple segments.

    ArcticDB stores wide tables with a default of ~127 columns per column slice.
    The lazy Arrow iterator yields one batch per slice, and the Python reader must
    merge these slices back into complete rows before passing to DuckDB.
    """

    @staticmethod
    def _write_wide_symbol(lib, n_rows=1000, n_cols=200, symbol="wide"):
        """Write a wide table that will be split into multiple column slices."""
        rng = np.random.default_rng(42)
        dates = pd.date_range("2024-01-01", periods=n_rows, freq="min")
        data = {f"f{i}": rng.standard_normal(n_rows) for i in range(n_cols - 5)}
        cats = ["A", "B", "C", "D", "E"]
        for i in range(5):
            data[f"s{i}"] = rng.choice(cats, n_rows)
        df = pd.DataFrame(data, index=pd.DatetimeIndex(dates, name="Date"))
        lib.write(symbol, df)
        return df

    def test_select_all_wide_table(self, lmdb_library):
        """SELECT * on a wide table returns all columns with correct data."""
        lib = lmdb_library
        df = self._write_wide_symbol(lib)

        result = lib.sql("SELECT * FROM wide")

        assert len(result) == len(df)
        assert set(result.columns) >= set(df.columns)

    def test_filter_on_late_column(self, lmdb_library):
        """WHERE filter on a column in a later slice works correctly.

        With 200 columns and 127 per slice, columns s0-s4 (indices 195-199)
        are in the second slice. The filter must still see the real data.
        """
        lib = lmdb_library
        df = self._write_wide_symbol(lib)

        result = lib.sql("SELECT * FROM wide WHERE s0 = 'A'")
        expected = df[df["s0"] == "A"]

        assert len(result) == len(expected)
        assert (result["s0"] == "A").all()

    def test_combined_date_and_value_filter_wide(self, lmdb_library):
        """Date range + value filter on a wide table returns correct rows."""
        lib = lmdb_library
        df = self._write_wide_symbol(lib)
        date_lo, date_hi = "2024-01-01 04:00", "2024-01-01 08:00"

        result = lib.sql(f"SELECT * FROM wide WHERE Date >= '{date_lo}' AND Date <= '{date_hi}' AND s0 = 'A'")

        mask = (df.index >= date_lo) & (df.index <= date_hi) & (df["s0"] == "A")
        expected = df[mask]
        assert len(result) == len(expected)

    def test_projection_wide_table(self, lmdb_library):
        """Column projection on a wide table returns correct subset."""
        lib = lmdb_library
        df = self._write_wide_symbol(lib)

        result = lib.sql("SELECT f0, f1, s0 FROM wide")

        assert len(result) == len(df)
        assert set(result.columns) == {"f0", "f1", "s0"}
        pd.testing.assert_series_equal(result["f0"].reset_index(drop=True), df["f0"].reset_index(drop=True))

    def test_aggregation_wide_table(self, lmdb_library):
        """Aggregation on a wide table works correctly."""
        lib = lmdb_library
        df = self._write_wide_symbol(lib)

        result = lib.sql("SELECT s0, COUNT(*) as cnt FROM wide GROUP BY s0 ORDER BY s0")

        expected = df.groupby("s0").size().reset_index(name="cnt").sort_values("s0")
        assert list(result["s0"]) == list(expected["s0"])
        assert list(result["cnt"]) == list(expected["cnt"])

    def test_wide_table_data_integrity(self, lmdb_library):
        """Verify that column data is not corrupted by slice merging."""
        lib = lmdb_library
        df = self._write_wide_symbol(lib, n_rows=100, n_cols=200)

        result = lib.sql("SELECT * FROM wide ORDER BY Date")

        # Check several columns from different slices
        for col in ["f0", "f50", "f126", "f127", "f150", "s0", "s4"]:
            pd.testing.assert_series_equal(
                result[col].reset_index(drop=True),
                df[col].reset_index(drop=True),
                check_names=False,
            )


@pytest.fixture(
    params=[False, True],
    ids=["static_schema", "dynamic_schema"],
)
def wide_multi_segment_lib(request, lmdb_library_factory):
    """Create a library with small row/column segments for both static and dynamic schema.

    Uses columns_per_segment=10 and rows_per_segment=50 so a 30-column table
    produces 3 column slices per row group and 4 row groups (200 rows), giving
    12 total segments — enough to exercise the column-slice merging path without
    creating an excessively large test dataset.
    """
    from arcticdb.options import LibraryOptions

    dynamic = request.param
    return lmdb_library_factory(LibraryOptions(rows_per_segment=50, columns_per_segment=10, dynamic_schema=dynamic))


def _write_wide_multi_segment_symbol(lib, n_rows=200, n_cols=30, symbol="wide_ms"):
    """Write a wide table that will be split into multiple column AND row slices."""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", periods=n_rows, freq="min")
    data = {f"f{i}": rng.standard_normal(n_rows) for i in range(n_cols - 6)}
    cats = ["A", "B", "C", "D", "E"]
    for i in range(5):
        data[f"s{i}"] = rng.choice(cats, n_rows)
    data["value"] = rng.uniform(100, 10000, n_rows)
    df = pd.DataFrame(data, index=pd.DatetimeIndex(dates, name="Date"))
    lib.write(symbol, df)
    return df


class TestWideTableMultiSegmentGroupBy:
    """Tests for GROUP BY on wide tables with multiple row groups AND column slices.

    Reproduces a scenario like CTA data: columns spread across multiple column
    slices and multiple row groups. The lazy streaming path must correctly skip
    irrelevant column slices during column projection and merge remaining slices
    before DuckDB performs aggregation.

    Parametrized over static and dynamic schema to ensure both paths work.
    """

    def test_group_by_sum_wide_multi_segment(self, wide_multi_segment_lib):
        """GROUP BY + SUM on a wide table with multiple row groups returns correct results."""
        lib = wide_multi_segment_lib
        df = _write_wide_multi_segment_symbol(lib)

        result = lib.sql("SELECT s0, SUM(value) AS total FROM wide_ms GROUP BY s0 ORDER BY s0")

        expected = df.groupby("s0")["value"].sum().reset_index(name="total").sort_values("s0").reset_index(drop=True)
        assert len(result) == len(expected), f"Expected {len(expected)} groups, got {len(result)}"
        assert list(result["s0"]) == list(expected["s0"])
        np.testing.assert_allclose(result["total"].values, expected["total"].values, rtol=1e-10)

    def test_group_by_count_wide_multi_segment(self, wide_multi_segment_lib):
        """GROUP BY + COUNT on a wide table with multiple row groups returns correct results."""
        lib = wide_multi_segment_lib
        df = _write_wide_multi_segment_symbol(lib)

        result = lib.sql("SELECT s0, COUNT(*) AS cnt FROM wide_ms GROUP BY s0 ORDER BY s0")

        expected = df.groupby("s0").size().reset_index(name="cnt").sort_values("s0").reset_index(drop=True)
        assert len(result) == len(expected), f"Expected {len(expected)} groups, got {len(result)}"
        assert list(result["s0"]) == list(expected["s0"])
        assert list(result["cnt"]) == list(expected["cnt"])

    def test_filter_and_group_by_wide_multi_segment(self, wide_multi_segment_lib):
        """WHERE + GROUP BY on a wide table with multiple row groups returns correct results."""
        lib = wide_multi_segment_lib
        df = _write_wide_multi_segment_symbol(lib)

        result = lib.sql("""SELECT s0, SUM(value) AS total FROM wide_ms WHERE s1 = 'B' GROUP BY s0 ORDER BY s0""")

        filtered = df[df["s1"] == "B"]
        expected = (
            filtered.groupby("s0")["value"].sum().reset_index(name="total").sort_values("s0").reset_index(drop=True)
        )
        assert len(result) == len(expected), f"Expected {len(expected)} groups, got {len(result)}"
        assert list(result["s0"]) == list(expected["s0"])
        np.testing.assert_allclose(result["total"].values, expected["total"].values, rtol=1e-10)

    def test_weighted_average_wide_multi_segment(self, wide_multi_segment_lib):
        """Weighted average (SUM(a*b)/SUM(b)) on a wide multi-segment table."""
        lib = wide_multi_segment_lib
        df = _write_wide_multi_segment_symbol(lib)

        result = lib.sql("""SELECT s0,
                      SUM(f0 * value) / SUM(value) AS wavg
               FROM wide_ms
               GROUP BY s0
               ORDER BY s0""")

        expected = (
            df.assign(_p=df["f0"] * df["value"])
            .groupby("s0")
            .agg(_ps=("_p", "sum"), _ds=("value", "sum"))
            .assign(wavg=lambda g: g["_ps"] / g["_ds"])[["wavg"]]
            .reset_index()
            .sort_values("s0")
            .reset_index(drop=True)
        )
        assert len(result) == len(expected), f"Expected {len(expected)} groups, got {len(result)}"
        assert list(result["s0"]) == list(expected["s0"])
        np.testing.assert_allclose(result["wavg"].values, expected["wavg"].values, rtol=1e-10)

    def test_cross_slice_filter_and_group_by(self, wide_multi_segment_lib):
        """WHERE filter on a column in one slice + GROUP BY on a column in a different slice.

        Regression test: with columns_per_segment=10 and 30 columns, f0 is in
        slice 0 while s0 is in slice 2.  The C++ FilterClause is evaluated
        per-segment: when it runs on a slice that doesn't contain the filter
        column (e.g. evaluating ``s0 = 'A'`` on the slice holding f0-f8), it
        must not crash.  DuckDB should still produce the correct result because
        column-slice merging reassembles complete rows before the SQL engine
        applies the WHERE.
        """
        lib = wide_multi_segment_lib
        df = _write_wide_multi_segment_symbol(lib)

        # f0 is in slice 0, s0 is in slice 2 — they are in *different* column slices
        result = lib.sql("SELECT f0, SUM(value) AS total FROM wide_ms WHERE s0 = 'A' GROUP BY f0 ORDER BY f0")

        filtered = df[df["s0"] == "A"]
        expected = (
            filtered.groupby("f0")["value"].sum().reset_index(name="total").sort_values("f0").reset_index(drop=True)
        )
        assert len(result) == len(expected), f"Expected {len(expected)} groups, got {len(result)}"
        np.testing.assert_allclose(result["total"].values, expected["total"].values, rtol=1e-10)

    def test_select_all_wide_multi_segment(self, wide_multi_segment_lib):
        """SELECT * on a wide multi-segment table returns all rows and columns."""
        lib = wide_multi_segment_lib
        df = _write_wide_multi_segment_symbol(lib)

        result = lib.sql("SELECT * FROM wide_ms ORDER BY Date")

        assert len(result) == len(df)
        assert set(result.columns) >= set(df.columns)
