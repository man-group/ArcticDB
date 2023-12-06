import numpy as np
import pandas as pd

from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal

def generic_resample_test(lib, sym, rule, aggregations, date_range=None, closed=None, label=None):
    read_df = lib.read(sym, date_range=date_range).data
    pandas_resampled_df = read_df.resample(rule, closed=closed, label=label).agg(aggregations)

    q = QueryBuilder()
    q = q.resample(rule, closed=closed, label=label).agg(aggregations)
    arcticdb_resampled_df = lib.read(sym, date_range=date_range, query_builder=q).data
    print(f"Raw:\n{read_df}")
    print(f"Pandas resampled:\n{pandas_resampled_df}")
    print(f"ArcticDB resampled:\n{arcticdb_resampled_df}")
    assert_frame_equal(pandas_resampled_df, arcticdb_resampled_df)


def test_resampling(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_resampling"
    index=pd.date_range("2023-01-01", "2023-12-31T23:59:59", freq="S")
    rng = np.random.default_rng()
    df = pd.DataFrame(
        {
            "ints": rng.integers(0, 100, len(index)),
            "floats": rng.random(len(index)),
        },
        index=index,
    )
    lib.write(sym, df)

    print("MINUTE BARS")
    generic_resample_test(lib, sym, "T", {"ints": "sum", "floats": "sum"})

    print("15 MINUTE BARS")
    generic_resample_test(lib, sym, "15T", {"ints": "sum", "floats": "sum"})

    print("HOURLY BARS")
    generic_resample_test(lib, sym, "H", {"ints": "sum", "floats": "sum"})

    print("DATE RANGE SUBSET")
    generic_resample_test(lib, sym, "T", {"ints": "sum", "floats": "sum"},
                          date_range=(pd.Timestamp("2023-02-01"), pd.Timestamp("2023-02-28T23:59:59")))

    print("CLOSED RIGHT BOUNDARIES")
    generic_resample_test(lib, sym, "T", {"ints": "sum", "floats": "sum"},
                          date_range=(pd.Timestamp("2023-02-01"), pd.Timestamp("2023-02-28T23:59:59")),
                          closed="right")

    print("LABELLED BY RIGHT BOUNDARIES")
    generic_resample_test(lib, sym, "T", {"ints": "sum", "floats": "sum"},
                          date_range=(pd.Timestamp("2023-02-01"), pd.Timestamp("2023-02-28T23:59:59")),
                          closed="right",
                          label="right")

