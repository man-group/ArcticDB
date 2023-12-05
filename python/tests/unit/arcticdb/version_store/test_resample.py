import numpy as np
import pandas as pd
import pytest

from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal

@pytest.mark.skip
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
    expected = df.resample("T").sum()
    lib.write(sym, df)
    q = QueryBuilder()
    q = q.resample("T").agg({"col_1": "sum",})
    received = lib.read(sym, date_range=(index[0] + pd.Timedelta(30, unit="S"), index[len(index) - 1] - pd.Timedelta(30, unit="S")), query_builder=q).data
    assert_frame_equal(expected, received)