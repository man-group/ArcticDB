# SQL Queries with DuckDB

ArcticDB integrates with [DuckDB](https://duckdb.org/) to enable SQL queries directly on your data. This provides a familiar SQL interface while leveraging ArcticDB's efficient storage and streaming capabilities.

## Installation

DuckDB is an optional dependency. Install it with:

```bash
pip install duckdb
```

## Quick Start: `lib.sql()`

For simple queries, use `lib.sql()` which automatically extracts symbol names from your query:

```python
import arcticdb as adb
import pandas as pd

# Setup
ac = adb.Arctic("lmdb://my_database")
lib = ac.get_library("market_data", create_if_missing=True)

# Write some data
trades = pd.DataFrame({
    "ticker": ["AAPL", "GOOG", "AAPL", "MSFT"],
    "price": [150.0, 2800.0, 151.0, 300.0],
    "quantity": [100, 50, 200, 75]
})
lib.write("trades", trades)

# Query with SQL
result = lib.sql("""
    SELECT ticker, AVG(price) as avg_price, SUM(quantity) as total_qty
    FROM trades
    GROUP BY ticker
    ORDER BY total_qty DESC
""")

print(result.data)
#   ticker  avg_price  total_qty
# 0   AAPL      150.5        300
# 1   MSFT      300.0         75
# 2   GOOG     2800.0         50
```

### JOIN Queries

`lib.sql()` supports JOIN queries across multiple symbols:

```python
# Write additional data
prices = pd.DataFrame({
    "ticker": ["AAPL", "GOOG", "MSFT"],
    "current_price": [155.0, 2850.0, 310.0]
})
lib.write("prices", prices)

# JOIN query
result = lib.sql("""
    SELECT t.ticker, t.quantity, p.current_price,
           t.quantity * p.current_price as market_value
    FROM trades t
    JOIN prices p ON t.ticker = p.ticker
""")
```

### Output Formats

Results can be returned in different formats:

```python
from arcticdb.options import OutputFormat

# Pandas DataFrame (default)
result = lib.sql("SELECT * FROM trades")
df = result.data  # pandas.DataFrame

# PyArrow Table
result = lib.sql("SELECT * FROM trades", output_format=OutputFormat.PYARROW)
arrow_table = result.data  # pyarrow.Table

# Polars DataFrame (requires polars package)
result = lib.sql("SELECT * FROM trades", output_format=OutputFormat.POLARS)
polars_df = result.data  # polars.DataFrame
```

### Version Selection

Query a specific version of your data:

```python
# Write multiple versions
lib.write("trades", trades_v1)  # version 0
lib.write("trades", trades_v2)  # version 1

# Query specific version
result = lib.sql("SELECT * FROM trades", as_of=0)
```

!!! note
    When using `lib.sql()` with JOINs, the `as_of` parameter applies to **all** symbols in the query. For per-symbol version control, use the `duckdb()` context manager.

## Advanced: `lib.duckdb()` Context Manager

For complex scenarios requiring fine-grained control, use the `duckdb()` context manager:

```python
with lib.duckdb() as ddb:
    ddb.register_symbol("trades")
    ddb.register_symbol("prices")
    result = ddb.query("""
        SELECT t.ticker, t.quantity * p.current_price as value
        FROM trades t
        JOIN prices p ON t.ticker = p.ticker
    """)
```

### When to Use `duckdb()` vs `sql()`

| Scenario | Use `sql()` | Use `duckdb()` |
|----------|-------------|----------------|
| Simple single-symbol queries | ✅ | |
| Basic JOINs | ✅ | |
| Different versions per symbol | | ✅ |
| Same symbol with different filters | | ✅ |
| Multiple queries on same data | | ✅ |
| Custom table aliases | | ✅ |
| Pre-filtering with QueryBuilder | | ✅ |

### Different Versions Per Symbol

Join current prices with historical trades:

```python
with lib.duckdb() as ddb:
    # Historical trades from version 0
    ddb.register_symbol("trades", as_of=0)
    # Latest prices
    ddb.register_symbol("prices", as_of=-1)

    result = ddb.query("""
        SELECT t.ticker, t.quantity, p.current_price
        FROM trades t
        JOIN prices p ON t.ticker = p.ticker
    """)
```

### Same Symbol with Different Filters (Period Comparison)

Compare data from different time periods:

```python
import pandas as pd

with lib.duckdb() as ddb:
    # January data
    ddb.register_symbol(
        "prices",
        alias="jan_prices",
        date_range=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
    )
    # February data
    ddb.register_symbol(
        "prices",
        alias="feb_prices",
        date_range=(pd.Timestamp("2024-02-01"), pd.Timestamp("2024-02-29"))
    )

    result = ddb.query("""
        SELECT
            j.ticker,
            j.price as jan_price,
            f.price as feb_price,
            f.price - j.price as change
        FROM jan_prices j
        JOIN feb_prices f ON j.ticker = f.ticker
    """)
```

### Multiple Queries on Same Data

Avoid re-reading data when running multiple queries:

```python
with lib.duckdb() as ddb:
    ddb.register_symbol("large_dataset")

    # First query - data is read once
    summary = ddb.query("""
        SELECT category, COUNT(*) as cnt, AVG(value) as avg_val
        FROM large_dataset
        GROUP BY category
    """)

    # Second query - reuses already-registered data
    top_records = ddb.query("""
        SELECT * FROM large_dataset
        WHERE value > 1000
        ORDER BY value DESC
        LIMIT 100
    """)
```

### Pre-filtering with QueryBuilder

Apply ArcticDB's efficient filtering before SQL processing:

```python
from arcticdb.version_store.processing import QueryBuilder

# Create a filter
qb = QueryBuilder()
qb = qb[qb["status"] == "active"]

with lib.duckdb() as ddb:
    # Data is filtered at storage level before reaching DuckDB
    ddb.register_symbol("orders", query_builder=qb)

    result = ddb.query("""
        SELECT product, SUM(amount) as total
        FROM orders
        GROUP BY product
    """)
```

### Row Range Selection

Read only specific rows:

```python
with lib.duckdb() as ddb:
    # Read rows 1000-2000 only
    ddb.register_symbol("large_table", row_range=(1000, 2000))
    result = ddb.query("SELECT * FROM large_table")
```

### Column Subset

Read only specific columns (reduces I/O):

```python
with lib.duckdb() as ddb:
    # Only read ticker and price columns
    ddb.register_symbol("trades", columns=["ticker", "price"])
    result = ddb.query("SELECT ticker, AVG(price) FROM trades GROUP BY ticker")
```

### Access to DuckDB Connection

For advanced DuckDB features, access the underlying connection:

```python
with lib.duckdb() as ddb:
    ddb.register_symbol("trades")

    # Create views, temporary tables, etc.
    ddb.execute("CREATE VIEW active_trades AS SELECT * FROM trades WHERE quantity > 0")

    # Use DuckDB-specific features
    result = ddb.query("SELECT * FROM active_trades")

    # Direct connection access for advanced usage
    conn = ddb.connection
    conn.execute("SET threads=4")
```

### External DuckDB Connections

Join ArcticDB data with other data sources by providing your own DuckDB connection:

```python
import duckdb

# Create a DuckDB connection with external data
conn = duckdb.connect()
conn.execute("CREATE TABLE benchmarks AS SELECT * FROM 'benchmarks.parquet'")
conn.execute("CREATE TABLE sectors AS SELECT * FROM 's3://bucket/sectors.csv'")

# Use it with ArcticDB - join ArcticDB data with external tables
with lib.duckdb(connection=conn) as ddb:
    ddb.register_symbol("portfolio_returns")
    result = ddb.query("""
        SELECT
            r.date,
            r.ticker,
            s.sector,
            r.return - b.return as alpha
        FROM portfolio_returns r
        JOIN benchmarks b ON r.date = b.date
        JOIN sectors s ON r.ticker = s.ticker
    """)

# Connection is still open - ArcticDB did NOT close it
# You can continue using it
more_results = conn.execute("SELECT * FROM benchmarks WHERE date > '2024-01-01'").df()
```

!!! note
    When you provide an external connection, ArcticDB will **not** close it when the context exits. This allows you to continue using the connection for other queries. When no connection is provided, ArcticDB creates and manages its own connection.

This is useful for:

- **Joining with Parquet/CSV files**: Load external files into DuckDB and join with ArcticDB data
- **Cross-database queries**: Query data from multiple sources in a single SQL statement
- **Persistent connections**: Reuse a connection across multiple ArcticDB context managers
- **DuckDB extensions**: Configure DuckDB extensions (httpfs, postgres, etc.) before using with ArcticDB

## Performance Considerations

### Automatic Pushdown Optimization

`lib.sql()` automatically optimizes queries by pushing operations down to ArcticDB's storage layer:

- **Column projection**: Only referenced columns are read from storage
- **Date range filters**: Filters on the index column skip irrelevant segments
- **Row limits**: `LIMIT` clauses reduce data read

```python
# Only reads 'price' column, filters at storage level, limits rows
result = lib.sql("""
    SELECT price FROM trades
    WHERE index >= '2024-01-01' AND index < '2024-02-01'
    LIMIT 1000
""")
```

!!! note
    Column pushdown is disabled for JOIN queries to ensure correctness (JOIN conditions may reference columns not in SELECT/WHERE).

### Memory Efficiency

Data is streamed to DuckDB using Arrow record batches, avoiding full materialization in memory. This allows querying datasets larger than available RAM.

## Limitations

### Unsupported Data Types

The following Arrow/Parquet types are not yet supported:

- DECIMAL types (use FLOAT64 as workaround)
- Timestamp precisions other than nanoseconds
- DATE, TIME, DURATION types
- BINARY/BLOB types
- Nested types (LIST, STRUCT, MAP)

Queries involving these types will raise an error.

### Read-Only

SQL queries are read-only. To write data, use `lib.write()`, `lib.append()`, or `lib.update()`.

## Examples

### Financial Analytics

```python
# Calculate daily returns
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

# Portfolio value calculation
with lib.duckdb() as ddb:
    ddb.register_symbol("positions")
    ddb.register_symbol("prices", as_of=-1)  # Latest prices

    result = ddb.query("""
        SELECT
            pos.ticker,
            pos.shares,
            p.price,
            pos.shares * p.price as market_value
        FROM positions pos
        JOIN prices p ON pos.ticker = p.ticker
    """)
```

### Time Series Analysis

```python
# Resample to daily OHLC
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
```

### Data Quality Checks

```python
# Find gaps in time series
result = lib.sql("""
    WITH dates AS (
        SELECT DISTINCT DATE_TRUNC('day', index) as date FROM prices
    )
    SELECT
        date,
        LEAD(date) OVER (ORDER BY date) as next_date,
        LEAD(date) OVER (ORDER BY date) - date as gap
    FROM dates
    WHERE LEAD(date) OVER (ORDER BY date) - date > INTERVAL '1 day'
""")
```
