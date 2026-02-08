"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

"""
Tests validating examples from the SQL queries tutorial (docs/mkdocs/docs/tutorials/sql_queries.md)
and the lib.explain() pushdown introspection API.
"""

import pandas as pd
import pytest

# Skip all tests if duckdb is not installed
duckdb = pytest.importorskip("duckdb")


class TestDocumentationExamples:
    """Tests for examples from the SQL queries documentation (docs/mkdocs/docs/tutorials/sql_queries.md)."""

    def test_quick_start_aggregation(self, lmdb_library):
        """Test the Quick Start example with GROUP BY aggregation."""
        lib = lmdb_library

        trades = pd.DataFrame(
            {
                "ticker": ["AAPL", "GOOG", "AAPL", "MSFT"],
                "price": [150.0, 2800.0, 151.0, 300.0],
                "quantity": [100, 50, 200, 75],
            }
        )
        lib.write("trades", trades)

        result = lib.sql("""
            SELECT ticker, AVG(price) as avg_price, SUM(quantity) as total_qty
            FROM trades
            GROUP BY ticker
            ORDER BY total_qty DESC
        """)

        assert len(result) == 3
        # AAPL has total_qty 300, should be first
        assert result.iloc[0]["ticker"] == "AAPL"
        assert result.iloc[0]["total_qty"] == 300
        assert result.iloc[0]["avg_price"] == pytest.approx(150.5)

    def test_join_with_market_value(self, lmdb_library):
        """Test JOIN example calculating market value."""
        lib = lmdb_library

        trades = pd.DataFrame(
            {
                "ticker": ["AAPL", "GOOG", "AAPL", "MSFT"],
                "price": [150.0, 2800.0, 151.0, 300.0],
                "quantity": [100, 50, 200, 75],
            }
        )
        prices = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "current_price": [155.0, 2850.0, 310.0]})
        lib.write("trades", trades)
        lib.write("prices", prices)

        result = lib.sql("""
            SELECT t.ticker, t.quantity, p.current_price,
                   t.quantity * p.current_price as market_value
            FROM trades t
            JOIN prices p ON t.ticker = p.ticker
        """)

        assert len(result) == 4  # All trades have matching prices
        assert "market_value" in result.columns
        # Check one calculation: AAPL 100 * 155 = 15500
        aapl_rows = result[result["ticker"] == "AAPL"]
        assert 15500.0 in list(aapl_rows["market_value"])

    def test_window_function_lag_daily_returns(self, lmdb_library):
        """Test Financial Analytics example: daily returns with LAG window function."""
        lib = lmdb_library

        # Create price data with dates
        prices = pd.DataFrame(
            {
                "ticker": ["AAPL", "AAPL", "AAPL", "GOOG", "GOOG", "GOOG"],
                "date": pd.to_datetime(
                    ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-01", "2024-01-02", "2024-01-03"]
                ),
                "close": [150.0, 152.0, 151.0, 2800.0, 2850.0, 2820.0],
            }
        )
        lib.write("prices", prices)

        result = lib.sql("""
            SELECT
                ticker,
                date,
                close,
                (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) /
                    LAG(close) OVER (PARTITION BY ticker ORDER BY date) as daily_return
            FROM prices
            ORDER BY ticker, date
        """)

        assert len(result) == 6
        assert "daily_return" in result.columns
        # First day of each ticker should have NULL return
        aapl_returns = result[result["ticker"] == "AAPL"]["daily_return"].tolist()
        assert pd.isna(aapl_returns[0])  # First day has no previous
        # Second day: (152 - 150) / 150 = 0.0133...
        assert aapl_returns[1] == pytest.approx(2.0 / 150.0)

    def test_portfolio_value_calculation(self, lmdb_library):
        """Test Financial Analytics example: portfolio value with positions and prices."""
        lib = lmdb_library

        positions = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "shares": [100, 50, 75]})
        prices = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "price": [155.0, 2850.0, 310.0]})
        lib.write("positions", positions)
        lib.write("prices", prices)

        with lib.duckdb() as ddb:
            ddb.register_symbol("positions")
            ddb.register_symbol("prices")

            result = ddb.sql("""
                SELECT
                    pos.ticker,
                    pos.shares,
                    p.price,
                    pos.shares * p.price as market_value
                FROM positions pos
                JOIN prices p ON pos.ticker = p.ticker
            """)

        assert len(result) == 3
        # AAPL: 100 * 155 = 15500
        aapl_row = result[result["ticker"] == "AAPL"].iloc[0]
        assert aapl_row["market_value"] == pytest.approx(15500.0)
        # Total portfolio value
        total_value = result["market_value"].sum()
        # 100*155 + 50*2850 + 75*310 = 15500 + 142500 + 23250 = 181250
        assert total_value == pytest.approx(181250.0)

    def test_time_series_ohlc_resampling(self, lmdb_library):
        """Test Time Series Analysis example: resample to daily OHLC."""
        lib = lmdb_library

        # Create tick data with timestamps
        ticks = pd.DataFrame(
            {
                "price": [100.0, 102.0, 99.0, 101.0, 105.0, 103.0, 102.0, 108.0],
                "volume": [1000, 500, 800, 1200, 600, 900, 700, 1100],
            },
            index=pd.to_datetime(
                [
                    "2024-01-01 09:30:00",
                    "2024-01-01 10:00:00",
                    "2024-01-01 11:00:00",
                    "2024-01-01 16:00:00",
                    "2024-01-02 09:30:00",
                    "2024-01-02 10:00:00",
                    "2024-01-02 11:00:00",
                    "2024-01-02 16:00:00",
                ]
            ),
        )
        lib.write("ticks", ticks)

        result = lib.sql("""
            SELECT
                DATE_TRUNC('day', index) as date,
                FIRST(price) as open,
                MAX(price) as high,
                MIN(price) as low,
                LAST(price) as close,
                SUM(volume) as volume
            FROM ticks
            GROUP BY DATE_TRUNC('day', index)
            ORDER BY date
        """)

        assert len(result) == 2  # Two days
        day1 = result.iloc[0]
        day2 = result.iloc[1]

        # Day 1: open=100, high=102, low=99, close=101, volume=3500
        assert day1["open"] == pytest.approx(100.0)
        assert day1["high"] == pytest.approx(102.0)
        assert day1["low"] == pytest.approx(99.0)
        assert day1["close"] == pytest.approx(101.0)
        assert day1["volume"] == 3500

        # Day 2: open=105, high=108, low=102, close=108, volume=3300
        assert day2["open"] == pytest.approx(105.0)
        assert day2["high"] == pytest.approx(108.0)
        assert day2["low"] == pytest.approx(102.0)
        assert day2["close"] == pytest.approx(108.0)
        assert day2["volume"] == 3300

    def test_data_quality_find_gaps(self, lmdb_library):
        """Test Data Quality example: find gaps in time series using window functions."""
        lib = lmdb_library

        # Create data with a gap (missing Jan 3)
        prices = pd.DataFrame(
            {"price": [100.0, 101.0, 103.0, 104.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-04", "2024-01-05"]),  # Note: Jan 3 is missing
        )
        lib.write("prices", prices)

        # Use duckdb() context to avoid CTE name being treated as a symbol
        with lib.duckdb() as ddb:
            ddb.register_symbol("prices")
            result = ddb.sql("""
                WITH date_series AS (
                    SELECT DISTINCT DATE_TRUNC('day', index) as date FROM prices
                )
                SELECT
                    date,
                    LEAD(date) OVER (ORDER BY date) as next_date,
                    LEAD(date) OVER (ORDER BY date) - date as gap
                FROM date_series
                ORDER BY date
            """)

        assert len(result) == 4
        # Check that we can detect the gap between Jan 2 and Jan 4
        gaps = result.dropna(subset=["gap"])
        # The gap column might be returned as interval or integer days
        # Find the row with the 2-day gap (gap > 1 day)
        gap_values = gaps["gap"]
        if hasattr(gap_values.iloc[0], "days"):
            # Timedelta/interval type
            large_gaps = gaps[gap_values.apply(lambda x: x.days > 1)]
        else:
            # Integer days
            large_gaps = gaps[gap_values > 1]
        assert len(large_gaps) == 1
        assert pd.Timestamp(large_gaps.iloc[0]["date"]).date() == pd.Timestamp("2024-01-02").date()

    def test_version_selection_as_of(self, lmdb_library):
        """Test Version Selection example: query specific version."""
        lib = lmdb_library

        # Write multiple versions
        trades_v0 = pd.DataFrame({"ticker": ["AAPL"], "price": [150.0]})
        trades_v1 = pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [155.0, 2800.0]})

        lib.write("trades", trades_v0)  # version 0
        lib.write("trades", trades_v1)  # version 1

        # Query version 0
        result_v0 = lib.sql("SELECT * FROM trades", as_of=0)
        assert len(result_v0) == 1

        # Query latest (version 1)
        result_v1 = lib.sql("SELECT * FROM trades")
        assert len(result_v1) == 2

    def test_dict_as_of_per_symbol_versions(self, lmdb_library):
        """Test dict-based as_of to read different versions of different symbols."""
        lib = lmdb_library

        # Write two versions of trades
        lib.write("trades", pd.DataFrame({"ticker": ["AAPL"], "price": [100.0]}))  # v0
        lib.write("trades", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [150.0, 2800.0]}))  # v1

        # Write two versions of prices
        lib.write("prices", pd.DataFrame({"ticker": ["AAPL"], "close": [99.0]}))  # v0
        lib.write("prices", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "close": [149.0, 2799.0]}))  # v1

        # trades@v0 has 1 row, prices@v1 has 2 rows
        result = lib.sql(
            "SELECT t.ticker, t.price, p.close FROM trades t JOIN prices p ON t.ticker = p.ticker",
            as_of={"trades": 0, "prices": 1},
        )
        # Only AAPL in trades v0, so join produces 1 row
        assert len(result) == 1
        assert result["price"].iloc[0] == 100.0  # trades v0 price
        assert result["close"].iloc[0] == 149.0  # prices v1 close

    def test_dict_as_of_missing_symbol_uses_latest(self, lmdb_library):
        """Test that symbols not in the as_of dict default to latest version."""
        lib = lmdb_library

        lib.write("trades", pd.DataFrame({"ticker": ["AAPL"], "price": [100.0]}))  # v0
        lib.write("trades", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [150.0, 2800.0]}))  # v1

        lib.write("prices", pd.DataFrame({"ticker": ["AAPL"], "close": [99.0]}))  # v0
        lib.write("prices", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "close": [149.0, 2799.0]}))  # v1

        # Only pin trades to v0; prices should use latest (v1)
        result = lib.sql(
            "SELECT t.ticker, t.price, p.close FROM trades t JOIN prices p ON t.ticker = p.ticker",
            as_of={"trades": 0},
        )
        assert len(result) == 1  # Only AAPL in trades v0
        assert result["price"].iloc[0] == 100.0  # trades v0
        assert result["close"].iloc[0] == 149.0  # prices latest (v1)

    def test_dict_as_of_single_symbol(self, lmdb_library):
        """Test dict-based as_of with a single symbol query."""
        lib = lmdb_library

        lib.write("trades", pd.DataFrame({"value": [1]}))  # v0
        lib.write("trades", pd.DataFrame({"value": [2]}))  # v1
        lib.write("trades", pd.DataFrame({"value": [3]}))  # v2

        result = lib.sql("SELECT * FROM trades", as_of={"trades": 1})
        assert result["value"].iloc[0] == 2

    def test_dict_as_of_with_timestamp(self, lmdb_library):
        """Test dict-based as_of using datetime values."""
        lib = lmdb_library
        import time

        lib.write("trades", pd.DataFrame({"value": [1]}))  # v0
        time.sleep(0.1)
        t_between = pd.Timestamp.now()
        time.sleep(0.1)
        lib.write("trades", pd.DataFrame({"value": [2]}))  # v1

        # Query as of the timestamp between the two writes
        result = lib.sql("SELECT * FROM trades", as_of={"trades": t_between})
        assert result["value"].iloc[0] == 1

    def test_dict_as_of_case_insensitive_lookup(self, lmdb_library):
        """Test that dict keys match case-insensitively against SQL names."""
        lib = lmdb_library

        lib.write("Trades", pd.DataFrame({"value": [1]}))  # v0
        lib.write("Trades", pd.DataFrame({"value": [2]}))  # v1

        # SQL uses lowercase 'trades', dict uses actual symbol name 'Trades'
        result = lib.sql("SELECT * FROM trades", as_of={"Trades": 0})
        assert result["value"].iloc[0] == 1


class TestExplain:
    """Tests for lib.explain() pushdown introspection."""

    def test_explain_returns_query_and_symbols(self, lmdb_library):
        """Test explain() always returns query and symbols keys."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"price": [1.0, 2.0]}))

        info = lib.explain("SELECT * FROM trades")

        assert info["query"] == "SELECT * FROM trades"
        assert info["symbols"] == ["trades"]

    def test_explain_column_pushdown(self, lmdb_library):
        """Test explain() detects column projection pushdown."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"price": [1.0], "volume": [100]}))

        info = lib.explain("SELECT price FROM trades")

        assert "columns_pushed_down" in info
        assert "price" in info["columns_pushed_down"]

    def test_explain_filter_pushdown(self, lmdb_library):
        """Test explain() detects WHERE filter pushdown."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"price": [1.0, 2.0]}))

        info = lib.explain("SELECT * FROM trades WHERE price > 1.0")

        assert info.get("filter_pushed_down") is True

    def test_explain_limit_pushdown(self, lmdb_library):
        """Test explain() detects LIMIT pushdown."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"price": [1.0, 2.0, 3.0]}))

        info = lib.explain("SELECT * FROM trades LIMIT 10")

        assert info.get("limit_pushed_down") == 10

    def test_explain_no_pushdown(self, lmdb_library):
        """Test explain() for a query with no pushdowns (SELECT *)."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"price": [1.0]}))

        info = lib.explain("SELECT * FROM trades")

        assert "query" in info
        assert "symbols" in info
        # SELECT * means all columns, no column pushdown
        assert "columns_pushed_down" not in info

    def test_explain_multi_symbol(self, lmdb_library):
        """Test explain() with a multi-symbol JOIN query."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"ticker": ["A"], "qty": [100]}))
        lib.write("prices", pd.DataFrame({"ticker": ["A"], "price": [50.0]}))

        info = lib.explain("SELECT t.ticker, p.price FROM trades t JOIN prices p ON t.ticker = p.ticker")

        assert set(info["symbols"]) == {"trades", "prices"}

    def test_explain_does_not_read_data(self, lmdb_library):
        """Test explain() works even when the symbol has no data (just schema)."""
        lib = lmdb_library
        lib.write("empty", pd.DataFrame({"x": pd.Series([], dtype="float64")}))

        # Should not raise - explain doesn't read data
        info = lib.explain("SELECT x FROM empty WHERE x > 0 LIMIT 5")

        assert info["symbols"] == ["empty"]

    def test_explain_invalid_sql_raises(self, lmdb_library):
        """Test explain() raises for invalid SQL."""
        lib = lmdb_library

        with pytest.raises(ValueError):
            lib.explain("INSERT INTO trades VALUES (1, 2)")
