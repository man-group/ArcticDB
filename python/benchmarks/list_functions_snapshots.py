"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb import Arctic

from .common import *


class SnaphotFunctions:
    number = 5
    timeout = 6000

    params = [500, 1000]
    num_snapshots_per_symbol = [10, 20]
    param_names = ["num_symbols"]

    ARCTIC_URL = "lmdb://list_functions"

    rows = 5

    def setup_cache(self):
        self.ac = Arctic(SnaphotFunctions.ARCTIC_URL)

        num_symbols = SnaphotFunctions.params
        iter = 0
        for syms in num_symbols:
            lib_name = f"{syms}_num_symbols"
            self.ac.delete_library(lib_name)
            lib = self.ac.create_library(lib_name)
            for sym in range(syms):
                symbol_name = f"{sym}_sym"
                for snap_no in SnaphotFunctions.num_snapshots_per_symbol:
                    df = generate_benchmark_df(SnaphotFunctions.rows)
                    lib.write(symbol=symbol_name, data=df, metadata=df.to_dict())
                    snap_name = f"{sym}_sym_{snap_no}"
                    lib.snapshot(snap_name)
            iter += 1

    def teardown(self, num_symbols):
        pass

    def setup(self, num_symbols):
        self.ac = Arctic(SnaphotFunctions.ARCTIC_URL)
        self.lib = self.ac[f"{num_symbols}_num_symbols"]

    def time_list_snapshots(self, num_symbols):
        list = self.lib.list_snapshots()

    def time_list_snapshots_metadata(self, num_symbols):
        list = self.lib.list_snapshots(load_metadata=True)
