import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal


def get_random_series() -> pd.Series:
    dates = pd.date_range("2018-08-17", "2019-01-10", name="date")
    securities = pd.Index(np.arange(100), name="security_id")
    index = pd.MultiIndex.from_product([dates, securities])
    np.random.seed(42)
    random_series = pd.Series(np.random.randn(len(index)), index=index, name="stuff")
    return random_series


def test_batch_roundtrip(s3_version_store_v1):
    df = get_random_series()
    symbols = [f"symbol_{i}" for i in range(500)]
    data_vector = [df for _ in symbols]
    s3_version_store_v1.batch_write(symbols, data_vector)
    s3_version_store_v1.batch_read(symbols)
