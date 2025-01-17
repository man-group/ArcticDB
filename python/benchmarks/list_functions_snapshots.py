"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import time
from arcticdb import Arctic

from arcticdb.util.utils import CachedDFGenerator
from benchmarks.common import *


class SnaphotFunctions:
    number = 5
    timeout = 6000

    params = [250, 500]
    num_snapshots_per_symbol = [10, 20]
    param_names = ["num_symbols" ]

    ARCTIC_URL = "lmdb://list_functions"

    rows = 10

    def setup_cache(self):
        start = time.time()
        self.ac = Arctic(SnaphotFunctions.ARCTIC_URL)

        self.create_test_library(self.ac, True)
        self.create_test_library(self.ac, False)

        print(f"Libraries generation took [{time.time() - start}]")

    def get_lib_name(swlf, num_syms: int, with_metadata: bool):
        return f"{num_syms}_num_symbols_meta_{with_metadata}"
    
    def create_test_library(self, ac: Arctic, with_metadata: bool):
        num_symbols = SnaphotFunctions.params
        iter = 0
        for syms in num_symbols:
            lib_name = self.get_lib_name(syms, with_metadata)
            self.ac.delete_library(lib_name)
            lib = self.ac.create_library(lib_name)
            for sym in range(syms):
                symbol_name = f"{sym}_sym"
                for snap_no in SnaphotFunctions.num_snapshots_per_symbol:
                    df = generate_benchmark_df(SnaphotFunctions.rows)
                    meta = None
                    if with_metadata:
                        meta = df.to_dict()
                    lib.write(symbol=symbol_name, data=df)
                    snap_name = f"{sym}_sym_{snap_no}"
                    lib.snapshot(snap_name, metadata=meta)
            iter += 1

    def teardown(self, num_symbols):
        pass

    def setup(self, num_symbols):
        self.ac = Arctic(SnaphotFunctions.ARCTIC_URL)
        self.lib = self.ac[self.get_lib_name(num_symbols, True)]
        self.lib_no_snaps = self.ac[self.get_lib_name(num_symbols, False)]

    def time_snapshots_with_metadata_list_without_load_meta(self, num_symbols):
        list = self.lib.list_snapshots()

    def time_snapshots_no_metadata_list_without_load_meta(self, num_symbols):
        list = self.lib_no_snaps.list_snapshots()

    def time_snapshots_with_metadata_list_with_load_meta(self, num_symbols):
        list = self.lib.list_snapshots(load_metadata=True)

    def time_snapshots_no_metadata_list_with_load_meta(self, num_symbols):
        list = self.lib_no_snaps.list_snapshots(load_metadata=True)

    def peakmem_snapshots_with_metadata_list_with_load_meta(self, num_symbols):
        list = self.lib.list_snapshots(load_metadata=True)

    def peakmem_snapshots_no_metadata_list_with_load_meta(self, num_symbols):
        list = self.lib_no_snaps.list_snapshots(load_metadata=True)


