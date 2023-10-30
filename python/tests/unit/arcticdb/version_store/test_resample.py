import numpy as np
import pandas as pd

from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal

def test_resampling(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    sym = "test_resampling"
    num_rows = 4
    index=pd.date_range("1970-01-01", periods=num_rows, freq="29S")
    df = pd.DataFrame(
        {
            "col": np.arange(10, 10 + num_rows)
        },
        index=index,
    )
    lib.write(sym, df)
    expected = df.iloc[:4].resample("T").sum()
    q = QueryBuilder()
    q = q.resample("T", (index[0], index[0] + pd.Timedelta(2, unit="m"))).agg({"col": "sum"})
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

