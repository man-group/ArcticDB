"""Write a 100k-row, 10-column DataFrame of mixed numeric and string types to LMDB."""
import numpy as np
import pandas as pd

from arcticdb import Arctic

LMDB_URI = "lmdb:///tmp/arcticdb_malloc_trim_lmdb"
LIBRARY = "malloc_trim"
SYMBOL = "data"

NUM_ROWS = 100_000


def make_dataframe(num_rows: int) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    index = pd.date_range("2020-01-01", periods=num_rows, freq="s")
    string_pool = np.array([f"s{i:05d}" for i in range(1000)])
    return pd.DataFrame(
        {
            "int8_col": rng.integers(-128, 127, num_rows, dtype=np.int8),
            "int32_col": rng.integers(-(2**31), 2**31 - 1, num_rows, dtype=np.int32),
            "int64_col": rng.integers(-(2**63), 2**63 - 1, num_rows, dtype=np.int64),
            "uint16_col": rng.integers(0, 2**16, num_rows, dtype=np.uint16),
            "uint64_col": rng.integers(0, 2**63, num_rows, dtype=np.uint64),
            "float32_col": rng.standard_normal(num_rows).astype(np.float32),
            "float64_col": rng.standard_normal(num_rows),
            "bool_col": rng.integers(0, 2, num_rows, dtype=np.int8).astype(bool),
            "str_low_card": string_pool[rng.integers(0, 10, num_rows)],
            "str_high_card": string_pool[rng.integers(0, len(string_pool), num_rows)],
        },
        index=index,
    )


def main() -> None:
    arctic = Arctic(LMDB_URI)
    if LIBRARY in arctic.list_libraries():
        arctic.delete_library(LIBRARY)
    arctic.create_library(LIBRARY)
    library = arctic[LIBRARY]

    df = make_dataframe(NUM_ROWS)
    library.write(SYMBOL, df)
    print(f"Wrote symbol {SYMBOL!r} with {len(df)} rows and {len(df.columns)} columns to {LMDB_URI}/{LIBRARY}")


if __name__ == "__main__":
    main()
