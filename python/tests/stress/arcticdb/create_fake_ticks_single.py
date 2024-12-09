import time
import os
import arcticdb as adb
import pandas as pd
import numpy as np
from multiprocessing import Pool
from datetime import datetime, timedelta


arctic_uri = "s3://{}:{}?access={}&secret={}".format(endpoint, bucket, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

arctic_library_name = "ticks2"


def generate_and_write_data(sec_ids, days, ticks_per_day, trading_days, lib):
    timestamps = np.tile(trading_days, ticks_per_day)
    random_seconds = np.random.randint(0, 7 * 3600, days * ticks_per_day)
    timestamps = pd.to_datetime(timestamps) + pd.to_timedelta(random_seconds, unit='s')
    timestamps = timestamps.sort_values()

    for sec_id in sec_ids:
        print("Generating data for {}".format(sec_id))
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
        print("Writing data for {}".format(sec_id))
        lib.write(sec_id, tick_data_df)
        print("Done")


def generate_tick_data(num_symbols=3800, days=252, ticks_per_day=15000, batch_size=30, adb_uri=arctic_uri, adb_lib=arctic_library_name):
    trading_days = [datetime.now() - timedelta(days=days-i) for i in range(days)]

    ac = adb.Arctic(adb_uri)
    lib = ac.get_library(adb_lib, create_if_missing=True)

    sec_ids = ["symbol_{}".format(x) for x in range(52, num_symbols)]
    generate_and_write_data(sec_ids, days, ticks_per_day, trading_days, lib)


def test_create_data():
    generate_tick_data()