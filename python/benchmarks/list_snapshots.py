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
    """
    The class contains test for time and peak memory measurements tests for
    list_snapshots.
    As list_snapshots depends on following variables we try to see each one's influence
     - number of snapshots 
     - are snapshots with metadata or not
     - are we getting list of snapshots with or without metadata
    Ideally the pattern of  time and mem increase should be linear at best or better and not exponential
    """

    number = 5
    timeout = 6000

    # For each test param n1xn2 defined:
    #   n1 - the number of symbols in the library
    #   n2 - Number of snapshots per each symbol 
    #        (We create also snapshot for each version of the symbol)
    params = ["20x10", "40x20"]
    param_names = ["symbols_x_snaps_per_sym"]

    ARCTIC_URL = "lmdb://list_functions"

    rows = 10

    def setup_cache(self):
        start = time.time()
        self.ac = Arctic(SnaphotFunctions.ARCTIC_URL)

        self.create_test_library(True)
        self.create_test_library(False)

        print(f"Libraries generation took [{time.time() - start}]")

    def get_lib_name(self, num_syms: int, with_metadata: bool):
        return f"{num_syms}_num_symbols_meta_{with_metadata}"
    
    def get_symbols(self, symbols_x_snaps_per_sym:str):
        return int(symbols_x_snaps_per_sym.split('x')[0]) 
    
    def create_test_library(self, with_metadata: bool):
        """
            Creates as many libraries as the len(SnaphotFunctions.params)
            Each library will have the a number of symbols defined by the
            value in the array.
            For each symbol we will create as meny versions as defined
            in corresponding value of 'num_versions_per_symbol' array
            and for each version we create one snapshot with metadata or not
            specified by caller
        """
        mdata = generate_benchmark_df(SnaphotFunctions.rows)
        df = mdata.sample(1)
        st = time.time()
        for param in SnaphotFunctions.params:
            number_symbols = self.get_symbols(param)
            snapshots_per_symbol = int(param.split('x')[1])
            lib_name = self.get_lib_name(number_symbols, with_metadata)
            self.ac.delete_library(lib_name)
            lib = self.ac.create_library(lib_name)
            start = time.time()
            print("Generating data for library: ", lib_name)
            print("  # Snapshots: ", number_symbols * snapshots_per_symbol)
            for symbol_number in range(number_symbols):
                symbol_name = f"{symbol_number}_sym"
                for number_snap in range(snapshots_per_symbol):
                    meta = None
                    if with_metadata:
                        meta = mdata.to_dict()
                    lib.write(symbol=symbol_name, data=df)
                    snap_name = f"{symbol_number}_sym_{number_snap}"
                    lib.snapshot(snap_name, metadata=meta)
            print("  Time: ", time.time() - start)
        print("Generation took (sec): ", time.time() - st)

    def teardown(self, symbols_x_snaps_per_sym):
        pass

    def setup(self, symbols_x_snaps_per_sym):
        num_symbols = self.get_symbols(symbols_x_snaps_per_sym)
        self.ac = Arctic(SnaphotFunctions.ARCTIC_URL)
        self.lib = self.ac[self.get_lib_name(num_symbols, True)]
        self.lib_no_meta = self.ac[self.get_lib_name(num_symbols, False)]

    def time_snapshots_with_metadata_list_without_load_meta(self, symbols_x_snaps_per_sym):
        list = self.lib.list_snapshots(load_metadata=False)

    def time_snapshots_with_metadata_list_with_load_meta(self, symbols_x_snaps_per_sym):
        list = self.lib.list_snapshots(load_metadata=True)

    def time_snapshots_no_metadata_list(self, symbols_x_snaps_per_sym):
        list = self.lib_no_meta.list_snapshots()

    def peakmem_snapshots_with_metadata_list_with_load_meta(self, symbols_x_snaps_per_sym):
        list = self.lib.list_snapshots(load_metadata=True)

    def peakmem_snapshots_no_metadata_list(self, symbols_x_snaps_per_sym):
        list = self.lib_no_meta.list_snapshots(load_metadata=False)

