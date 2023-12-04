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
            "col_1": np.arange(len(index), dtype=np.float64),
        },
        index=index,
    )
    df.iloc[5, 0] = np.nan
    expected = df.resample("T").sum()
    lib.write(sym, df)
    q = QueryBuilder()
    q = q.resample("T", (index[0], index[len(index) - 1] + pd.Timedelta(1, unit="m"))).agg({"col_1": "sum",})
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)