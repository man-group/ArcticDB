import time
import os
import arcticdb as adb
import pandas as pd
import numpy as np
from multiprocessing import Pool
from datetime import datetime, timedelta


AWS_ACCESS_KEY_ID = 'MZEDTTRBRIG0TDWQ4F8M'
AWS_SECRET_ACCESS_KEY = 'SDQMvXWrpBu+jHECovLJcoqpmqa8sn+wTQvggBYs'
user = 'MZEDTTRBRIG0TDWQ4F8M'
secret = 'SDQMvXWrpBu+jHECovLJcoqpmqa8sn+wTQvggBYs'
bucket = 'user-wdealtry-dev'
endpoint = 's3.vast.gdc.storage.dev.m'


arctic_uri = "s3://{}:{}?access={}&secret={}".format(endpoint, bucket, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

arctic_library_name = "ticks"
def generate_and_write_data(sec_ids, days, ticks_per_day, trading_days, adb_uri, adb_lib):
    ac = adb.Arctic(adb_uri)
    lib = ac.get_library(adb_lib, create_if_missing=True)

    timestamps = np.tile(trading_days, ticks_per_day)
    random_seconds = np.random.randint(0, 7 * 3600, days * ticks_per_day)
    timestamps = pd.to_datetime(timestamps) + pd.to_timedelta(random_seconds, unit='s')
    timestamps = timestamps.sort_values()

    tick_data = []
    for sec_id in sec_ids:
        initial_price = np.random.uniform(50, 150)
        price_changes = np.random.normal(0, 0.1, days * ticks_per_day)
        prices = initial_price + np.cumsum(price_changes)
        prices = np.where(prices < 5, 2 * 5 -prices, prices)

        volumes = np.random.randint(1, 100, days * ticks_per_day)

        tick_types = np.random.choice(["BID", "ASK"], days * ticks_per_day)

        tick_data_df = pd.DataFrame({
            'EVENT_PRICE': prices,
            'EVENT_SIZE': volumes,
            "TICK_TYPE": tick_types,
        }, index=timestamps)
        tick_data.append(adb.WritePayload(f"Security_{sec_id}", tick_data_df))

    lib.write_batch(tick_data)

def generate_tick_data(num_symbols=4000, days=252, ticks_per_day=15000, batch_size=30, adb_uri=arctic_uri, adb_lib=arctic_library_name):
    trading_days = [datetime.now() - timedelta(days=days-i) for i in range(days)]

    sec_batches = [list(range(i, min(i + batch_size, num_symbols + 1))) for i in range(1, num_symbols+1, batch_size)]

    args = [(batch, days, ticks_per_day, trading_days, adb_uri, adb_lib) for batch in sec_batches]

    pool_size = 5
    with Pool(pool_size) as pool:
        pool.starmap(generate_and_write_data, args)


def test_create_data():
    generate_tick_data()