import arcticdb as adb
from datetime import datetime

import os
os.environ['ARCTICDB_schedule_loglevel'] = 'DEBUG'

def _query() -> adb.QueryBuilder:
    q = adb.QueryBuilder()
    q = q[q["TICK_TYPE"]=="BID"]
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

def generate_bars(symbols, arctic_uri, arctic_library):
    ac = adb.Arctic(arctic_uri)
    library = ac.get_library(arctic_library)
    query = _query()
    bars = library.read_batch(symbols, query_builder=query)
    return [(bar.symbol, bar.data) for bar in bars if isinstance(bar, adb.VersionedItem) and not bar.data.empty]


from multiprocessing import Pool


def gen_bars_m(symbols):
    return generate_bars(symbols, arctic_uri=arctic_uri, arctic_library=arctic_library_name)


def test_gen_bars():
    pool_size = 35
    ac = adb.Arctic(arctic_uri)
    lib = ac.get_library(arctic_library_name, create_if_missing=True)

    symbols = lib.list_symbols()
    start_time = datetime.now()
    with Pool(pool_size) as pool:
        data = pool.map(gen_bars_m, [[symbol] for symbol in symbols], 10)

    print("Read took {}".format(datetime.now() - start_time))