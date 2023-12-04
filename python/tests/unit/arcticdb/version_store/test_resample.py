import numpy as np
import pandas as pd

from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal

def test_resampling(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_resampling"
    index=pd.date_range("1970-01-01", periods=120, freq="S")
    df = pd.DataFrame(
        {
            "col_1": np.arange(len(index), dtype=np.uint64),
        },
        index=index,
    )
    expected = df.resample("T", label="right").sum()
    lib.write(sym, df)
    q = QueryBuilder()
    q = q.resample("T", (pd.Timestamp('1969-12-31T23:59:00'), pd.Timestamp('1970-01-01T00:02:00')), label="right").agg({"col_1": "sum",})
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)