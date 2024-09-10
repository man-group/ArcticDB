import arcticdb as adb
from datetime import datetime

arctic_uri = "lmdb:///opt/arcticdb/python"

arctic_library_name = "ticks2"


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

    all_symbols = lib.list_symbols()
    symbols = all_symbols[:4000]
    print("Reading {} symbols".format(len(symbols)))
    start_time = datetime.now()
    bars = generate_bars(symbols, lib)
    print("Read took {}".format(datetime.now() - start_time))