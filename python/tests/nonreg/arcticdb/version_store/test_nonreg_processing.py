"""nonreg tests for the processing pipeline (QueryBuilder functionality)"""
import pandas as pd
import numpy as np

from arcticdb.util.test import generic_resample_test


def test_resample_mean_large_arithmetic_error_repro(lmdb_version_store_v1):
    """The hypothesis test python.tests.hypothesis.arcticdb.test_resample.test_resample failed with a superset of this example on master.

    @reproduce_failure('6.72.4', b'AXicY2RgZGRgWMwKJEAIxGEGEowsTCAOIxPDfH8wxTCfhRGiposJSIBVMjIAmYxAbAbmQAGICeQ3wLggifr///9/Ps0FtoEJpAuhHAQA5XYJAA==')

    We had to increase the tolerance of assert_dfs_approximate as a result.
    """
    lib = lmdb_version_store_v1
    sym = "sym"
    rule = "1min"
    origin = "start"
    df = pd.DataFrame({"col_int": [-513, -9223372036854775808, -513, 9223372036649978369]}, dtype=np.int64)
    df.index = pd.date_range("2025-01-01", periods=4, freq="s")
    lib.write(sym, df)

    agg = {'col_int_mean': ('col_int', 'mean')}
    generic_resample_test(
        lib,
        sym,
        rule,
        agg,
        df,
        origin=origin)
