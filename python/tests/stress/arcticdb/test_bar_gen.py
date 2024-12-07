import arcticdb as adb
from datetime import datetime

from arcticdb_ext import set_config_int

import os
os.environ['ARCTICDB_schedule_loglevel'] = 'DEBUG'

set_config_int("VersionStore.NumCPUThreads", 64)
set_config_int("VersionStore.NumIOThreads", 120)

AWS_ACCESS_KEY_ID = 'MZEDTTRBRIG0TDWQ4F8M'
AWS_SECRET_ACCESS_KEY = 'SDQMvXWrpBu+jHECovLJcoqpmqa8sn+wTQvggBYs'
user = 'MZEDTTRBRIG0TDWQ4F8M'
secret = 'SDQMvXWrpBu+jHECovLJcoqpmqa8sn+wTQvggBYs'
bucket = 'user-wdealtry-dev'
endpoint = 's3.vast.gdc.storage.dev.m'

arctic_uri = "s3://{}:{}?access={}&secret={}".format(endpoint, bucket, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
arctic_library_name = "ticks"


def _query() -> adb.QueryBuilder:
    q = adb.QueryBuilder()
    q = q[q["TICK_TYPE"] == "BID"]
    q = q.resample("min", closed="right")
    q = q.agg({
        "num_ticks": ("EVENT_PRICE", "count"),
        "volume": ("EVENT_SIZE", "sum"),
        "close": ("EVENT_PRICE", "last"),
        "low": ("EVENT_PRICE", "min"),
        "high": ("EVENT_PRICE", "max"),
        "open": ("EVENT_PRICE", "first"),
    })
    q = q[~q["close"].isna()]
    q = q[~q["low"].isna()]
    q = q[~q["high"].isna()]
    q = q[~q["open"].isna()]
    return q


def generate_bars(symbols, library):
    query = _query()
    bars = library.read_batch(symbols, query_builder=query)
    return [(bar.symbol, bar.data) for bar in bars if isinstance(bar, adb.VersionedItem) and not bar.data.empty]


def test_gen_bars():
    ac = adb.Arctic(arctic_uri)
    lib = ac.get_library(arctic_library_name, create_if_missing=True)

    #all_symbols = lib.list_symbols()
    symbols = ['Security_2023',
        #'Security_2968',
        #'Security_1153',
        #'Security_2586',
        #'Security_3245',
        #'Security_3588',
        #'Security_677',
        #'Security_2437',
        #'Security_2211',
        #'Security_1185',
        #'Security_3860',
        #'Security_3911',
        #'Security_3160',
        #'Security_2216',
        #'Security_2680',
        #'Security_2345',
        #'Security_1751',
        #'Security_3414',
        #'Security_3688',
        #'Security_2768'
        ]
    print("Reading symbols {}".format(symbols))
    start_time = datetime.now()
    bars = generate_bars(symbols, lib)
    print("Read took {}".format(datetime.now() - start_time))