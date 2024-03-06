import pandas as pd
import numpy as np
import time

def test_metadata_cache_simple(lmdb_version_store_metadata_cache):
    lib = lmdb_version_store_metadata_cache
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("symbol", df)
    cache = lib.version_store.get_symbol_info_cache(["symbol"], int(pd.Timestamp.utcnow().value), 1000)
    assert len(cache) == 1
    assert cache["symbol"].total_rows == 10


def test_metadata_cache_compact(lmdb_version_store_metadata_cache):
    lib = lmdb_version_store_metadata_cache
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("symbol", df)
    lib.version_store.compact_symbol_info_cache()
    cache = lib.version_store.get_symbol_info_cache(["symbol"], int(pd.Timestamp.utcnow().value), 1000)
    assert len(cache) == 1
    assert cache["symbol"].total_rows == 10


def test_metadata_cache_stress(lmdb_version_store_metadata_cache):
    lib = lmdb_version_store_metadata_cache
    num_symbols = 100000;
    dfs = []
    symbols = []
    df = pd.DataFrame({"x": np.arange(10)})
    for i in range(num_symbols):
        dfs.append(df)
        symbols.append("symbol_{}".format(i))

    lib.batch_write(symbols, dfs)

    time.sleep(2)
    start = time.time()
    vit = lib.version_store.compact_symbol_info_cache()
    elapsed = time.time() - start
    print("Compaction time: {}".format(elapsed))
    print(vit)

    lib.version_store.get_symbol_info_cache(["symbol"], int(pd.Timestamp.utcnow().value), 1000)
    elapsed = time.time() - start
    print("Read time: {}".format(elapsed))


def test_metadata_cache_batch_write(lmdb_version_store_metadata_cache):
    lib = lmdb_version_store_metadata_cache
    num_symbols = 1000;
    dfs = []
    symbols = []
    df = pd.DataFrame({"x": np.arange(10)})
    for i in range(num_symbols):
        dfs.append(df)
        symbols.append("symbol_{}".format(i))

    lib.batch_write(symbols, dfs)
    time.sleep(2)

    lib.version_store.compact_symbol_info_cache()

    cache = lib.version_store.get_symbol_info_cache(["symbol"], int(pd.Timestamp.utcnow().value), 1)
    assert len(cache) == num_symbols