"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

# Reproduces the read-time regression introduced by the folly upgrade in commit 38909e54a
# between ArcticDB 5.10.0 and 6.1.0.
#
#   ┌─────────┬─────────────┐
#   │ version │ time_read   │
#   ├─────────┼─────────────┤
#   │ 5.10.0  │ 44.6 ± 1 ms │
#   ├─────────┼─────────────┤
#   │ 6.13.0  │ 59.3 ± 1 ms │
#   └─────────┴─────────────┘

import string
import time

import numpy as np
import pandas as pd
from asv_runner.benchmarks.mark import SkipNotImplemented

from arcticdb.util.logger import get_logger
from benchmarks.environment_setup import Storage, create_libraries

SEED = 20260421
STORAGES = [Storage.LMDB, Storage.AMAZON]


def _make_strings(n, rng, length=8):
    alphabet = np.array(list(string.ascii_lowercase), dtype="<U1")
    chars = rng.choice(alphabet, size=(n, length))
    return chars.view(f"<U{length}").reshape(n)


def _make_sparse_strings(n, rng, none_fraction=0.99, length=8):
    arr = np.full(n, None, dtype=object)
    num_strings = n - int(n * none_fraction)
    if num_strings > 0:
        idx = rng.choice(n, size=num_strings, replace=False)
        arr[idx] = _make_strings(num_strings, rng, length=length)
    return arr


def _generate_random(n):
    rng = np.random.default_rng(SEED)
    df = pd.DataFrame(
        {
            "col_int": rng.integers(0, 1_000_000, size=n, dtype=np.int64),
            "col_float1": rng.standard_normal(n),
            "col_float2": rng.standard_normal(n),
            "col_float3": rng.standard_normal(n),
            "col_float4": rng.standard_normal(n),
            "col_string": _make_strings(n, rng),
            "col_string_sparse": _make_sparse_strings(n, rng),
            "col_none": np.full(n, np.nan, dtype=np.float64),
        },
        index=pd.date_range("2026-01-01", periods=n, freq="s"),
    )
    df.index.name = "col_datetime"
    return df


def _lib_name(rows):
    return f"nonreg_random_{rows}"


def _symbol(rows):
    return f"sym_{rows}"


class NonRegRandomRead:
    timeout = 600
    sample_time = 2
    rounds = 2
    repeat = (1, 10, 20.0)
    warmup_time = 0.2

    num_rows = 1_000_000

    params = [[num_rows], STORAGES]
    param_names = ["rows", "storage"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        libs_for_storage = {}
        library_names = [_lib_name(self.num_rows)]
        df = _generate_random(self.num_rows)
        for storage in STORAGES:
            libs = create_libraries(storage, library_names)
            libs_for_storage[storage] = dict(zip(library_names, libs))
            lib = libs_for_storage[storage][library_names[0]]
            if lib is None:
                continue
            lib.write(_symbol(self.num_rows), df)
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")
        return libs_for_storage

    def setup(self, libs_for_storage, rows, storage):
        self.lib = libs_for_storage[storage][_lib_name(rows)]
        if self.lib is None:
            raise SkipNotImplemented
        self.symbol = _symbol(rows)

    def time_read(self, libs_for_storage, rows, storage):
        self.lib.read(self.symbol).data

    def peakmem_read(self, libs_for_storage, rows, storage):
        self.lib.read(self.symbol).data
