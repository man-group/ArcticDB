"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import string
from arcticdb.util.test import assert_frame_equal, sample_dataframe, get_wide_dataframe, get_pickle, random_integers

import pandas as pd
import random
import numpy as np
from datetime import datetime
from arcticdb import log

LARGE_DF_SIZE = 100000
SLEEP = 1
MAX_MEM_USAGE = 1000000


# These tasks without asserts are designed to be used to check syncing, where the check will be inter-lib
def write_small_df(lib, symbol):
    return lib.write(symbol, sample_dataframe(size=3, seed=None))


def write_small_df_and_prune_previous(lib, symbol):
    return lib.write(symbol, sample_dataframe(size=3, seed=None), prune_previous=True)


def append_small_df(lib, symbol):
    return lib.append(symbol, sample_dataframe(size=3, seed=None), write_if_missing=True)


def append_small_df_and_prune_previous(lib, symbol):
    return lib.append(symbol, sample_dataframe(size=3, seed=None), write_if_missing=True, prune_previous=True)


def delete_symbol(lib, symbol=None):
    if symbol is not None:
        lib.delete(symbol)
        return f"Deleted symbol {symbol}"

    symbols = lib.list_symbols()
    if len(symbols) > 0:
        symbol = random.choice(symbols)
        lib.delete(symbol)
        return f"Deleted symbol {symbol}"

    return "No symbols to delete"


def delete_specific_version(lib, symbol=None):
    versions = lib.list_versions(symbol)
    # Generate a dict from symbols to lists of undeleted versions
    undeleted_versions = dict()
    for version_info in versions:
        if not version_info["deleted"]:
            sym = version_info["symbol"]
            version_list = undeleted_versions.get(sym, [])
            version_list.append(version_info["version"])
            undeleted_versions[sym] = version_list
    if len(undeleted_versions) > 0:
        symbol = random.choice(list(undeleted_versions.keys()))
        version = random.choice(undeleted_versions[symbol])
        lib.delete_version(symbol, version)
        return f"Deleted version {version} of symbol {symbol}"

    return "No versions to delete"


def snapshot_new_name(lib, _):
    if len(lib.list_symbols()) == 0:
        return "No symbols to snapshot"
    snapshot_name = "snapshot-{}".format(datetime.utcnow().isoformat())
    lib.snapshot(snapshot_name)
    syms = lib.list_symbols()
    return f"Created snapshot {snapshot_name} with symbols {syms}"


def snapshot_existing_name(lib, _):
    existing_snapshots = list(lib.list_snapshots().keys())
    if len(existing_snapshots) > 0:
        snapshot_name = random.choice(existing_snapshots)
        lib.delete_snapshot(snapshot_name)
        lib.snapshot(snapshot_name)
        syms = lib.list_symbols()
        return f"Resnapshotted snapshot {snapshot_name} with symbols {syms}"

    return "No snapshots to rename"


def delete_snapshot(lib, _):
    existing_snapshots = list(lib.list_snapshots().keys())
    if len(existing_snapshots) > 0:
        snapshot_name = random.choice(existing_snapshots)
        lib.delete_snapshot(snapshot_name)
        syms = lib.list_symbols()
        return f"Deleted snapshot {snapshot_name} with symbols {syms}"

    return "No snapshots to delete"


def read_write_sample(lib, symbol):
    lib.write(symbol, "blah")
    assert lib.read(symbol).data == "blah"


def get_int_col_dataframe(size=10000, seed=0):
    np.random.seed(seed)
    return pd.DataFrame({"uint32": random_integers(size, np.uint32)})


def write_and_prune_simple_df(lib, symbol):
    df = get_int_col_dataframe(10000)
    lib.write(symbol, df, metadata={"a": 1})
    assert_frame_equal(lib.read(symbol).data, df)
    assert lib.read(symbol).metadata == {"a": 1}


def write_and_append_simple_df(lib, symbol):
    df = get_int_col_dataframe(10000)
    lib.write(symbol, df, metadata={"a": 1})
    assert_frame_equal(lib.read(symbol).data, df)
    assert lib.read(symbol).metadata == {"a": 1}

    lib.append(symbol, pd.DataFrame({"uint32": random_integers(10000, np.uint32)}))
    num_tiny_appends = 10
    for _ in range(num_tiny_appends):
        lib.append(symbol, pd.DataFrame({"uint32": random_integers(1, np.uint32)}))

    assert len(lib.read(symbol).data) == 20000 + num_tiny_appends


def write_large_mixed_df_prune(lib, symbol):
    df = get_wide_dataframe(LARGE_DF_SIZE)
    lib.write(symbol, df, metadata={"something"}, prune_previous=True)
    assert_frame_equal(lib.read(symbol).data, df)


def write_large_mixed_df(lib, symbol):
    df = get_wide_dataframe(LARGE_DF_SIZE)
    lib.write(symbol, df, metadata={"something"})
    assert_frame_equal(lib.read(symbol).data, df)


def write_pickle(lib, symbol):
    data = get_pickle()
    lib.write(symbol, data)
    assert lib.read(symbol).data == data


def delete_random_symbols(lib, unused):
    all_symbols = lib.list_symbols()
    if len(all_symbols) < 5:
        return
    for sym in random.sample(all_symbols, 3):
        lib.delete(sym)


def read_random_symbol_version(lib, unused):
    all_symbols = lib.list_symbols()
    if len(all_symbols) < 5:
        return
    for sym in random.sample(all_symbols, 3):
        versions = lib.list_versions(sym)
        for version_info in random.sample(versions, 1):
            lib.read(sym, as_of=int(version_info["version"]))


def get_new_symbol(func, lib):
    return func.__name__ + datetime.utcnow().isoformat()


def get_existing_symbol(func, lib):
    func_symbols = lib.list_symbols()
    if len(func_symbols) == 0:
        return get_new_symbol(func, lib)

    return random.choice(func_symbols)


SYMBOL_FUNCTIONS = [get_new_symbol, get_existing_symbol]


def get_symbol(func, lib):
    symbol_func = random.choice(SYMBOL_FUNCTIONS)
    return symbol_func(func, lib)


def run_scenario(func, lib, with_snapshots, verbose):
    try:
        symbol = get_symbol(func, lib)
        if verbose:
            log.storage.info("Running function {} symbol {}".format(func.__name__, symbol))
        res = func(lib, symbol)
        if verbose:
            log.storage.info("Function {} symbol {} returned {}".format(func.__name__, symbol, res))
        if with_snapshots and lib.list_symbols():
            rand_id = "".join(random.choices(string.ascii_letters, k=5))
            lib.snapshot(rand_id + "snapshot" + datetime.utcnow().isoformat())
            # clean up old snapshots - more than 3 hours old
            for s in lib.list_snapshots():
                t = datetime.strptime(s[len(rand_id):], "snapshot%Y-%m-%dT%H:%M:%S.%f")
                if (datetime.utcnow() - t).seconds > 60 * 60 * 3:
                    lib.delete_snapshot(s)
    except Exception as e:
        log.storage.error("Running", func.__name__, "failed due to: ", e)
        raise e
