import numpy as np
import pandas as pd

from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal

def test_resampling(lmdb_version_store_big_map):
    lib = lmdb_version_store_big_map
    sym = "test_resampling"
    index=pd.date_range("1970-01-01", end="1970-01-02", freq="S")
    df = pd.DataFrame(
        {
            "col": np.arange(len(index), dtype=np.uint64)
        },
        index=index,
    )
    expected = df.resample("T").sum()
    lib.write(sym, df)
    q = QueryBuilder()
    q = q.resample("T", (index[0], index[len(index) - 1] - pd.Timedelta(1, unit="ns"))).agg({"col": "sum"})
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)
