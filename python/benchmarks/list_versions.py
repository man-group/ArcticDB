"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random
import time
from arcticdb import Arctic

from benchmarks.common import *


class ListVersions:
    number = 5
    rounds = 1
    timeout = 6000
    warmup_time = 0

    param_names = [
        "num_symbols",
        "num_versions",
        "num_snapshots",
        "symbol",
        "snapshot",
        "latest_only",
        "skip_snapshots",
    ]
    params = [
        [100, 1_000],
        [1, 10],
        [0, 10, 100],
        [None, "0_sym"],
        [None, "0_snap"],
        [True, False],
        [True, False],
    ]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _lib_name(self, num_symbols, num_versions, num_snapshots):
        return f"{num_symbols}_num_symbols_{num_versions}_num_versions_{num_snapshots}_num_snapshots"

    def _sym_name(self, sym_idx):
        return f"{sym_idx}_sym"

    def _snap_name(self, snap_idx):
        return f"{snap_idx}_snap"

    def _setup_cache(self):
        self.ac = Arctic("lmdb://list_versions")

        num_symbols = self.params[0]
        num_versions = self.params[1]
        num_snapshots = self.params[2]
        for syms in num_symbols:
            for versions in num_versions:
                for snapshots in num_snapshots:
                    lib_name = self._lib_name(syms, versions, snapshots)
                    self.ac.delete_library(lib_name)
                    lib = self.ac.create_library(lib_name)
                    for sym in range(syms):
                        for _ in range(versions):
                            lib.write_pickle(self._sym_name(sym), 0)
                    for snapshot in snapshots:
                        lib.snapshot(
                            self._snap_name(snapshot),
                            {self._sym_name(sym): random.randint(0, versions - 1) for sym in range(syms)},
                        )

    def teardown(self, num_symbols, num_versions, num_snapshots, symbol, snapshot, latest_only, skip_snapshots):
        pass

    def setup(self, num_symbols, num_versions, num_snapshots, symbol, snapshot, latest_only, skip_snapshots):
        self.ac = Arctic("lmdb://list_versions")
        self.lib = self.ac[self._lib_name(num_symbols, num_versions, num_snapshots)]

    def time_list_versions(
        self, num_symbols, num_versions, num_snapshots, symbol, snapshot, latest_only, skip_snapshots
    ):
        self.lib.list_versions(symbol=symbol, snapshot=snapshot, latest_only=latest_only, skip_snapshots=skip_snapshots)

    def peakmem_list_versions(
        self, num_symbols, num_versions, num_snapshots, symbol, snapshot, latest_only, skip_snapshots
    ):
        self.lib.list_versions(symbol=symbol, snapshot=snapshot, latest_only=latest_only, skip_snapshots=skip_snapshots)
