import numpy as np
import pandas as pd

from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal

def test_resampling(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_resampling"
    index=pd.date_range("1970-01-01", end="1971-01-01", freq="S")
    df = pd.DataFrame(
        {
            "col_1": np.arange(len(index), dtype=np.uint64),
            "col_2": np.arange(1_000_000, 1_000_000 + len(index), dtype=np.uint64),
            "col_3": np.arange(1_000_000_000, 1_000_000_000 + len(index), dtype=np.uint64),
        },
        index=index,
    )
    expected = df.resample("T").sum()
    lib.write(sym, df)
    q = QueryBuilder()
    q = q.resample("T", (index[0], index[len(index) - 1] + pd.Timedelta(1, unit="m"))).agg({"col_1": "sum", "col_2": "sum", "col_3": "sum"})
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)
