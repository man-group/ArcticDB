"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb import Arctic

from .common import *

class ListFunctions:
    number = 5
    timeout = 6000

    params = ([500, 1000])
    param_names = ['num_symbols']

    rows = 1000

    def __init__(self):
        self.ac = Arctic("lmdb://list_functions")

        num_symbols = ListFunctions.params
        for syms in num_symbols:
            lib_name = f"{syms}_num_symbols"
            self.ac.delete_library(lib_name)
            self.ac.create_library(lib_name)
            lib = self.ac[lib_name]
            for sym in range(syms):
                lib.write(f"{sym}_sym", generate_pseudo_random_dataframe(ListFunctions.rows))

    def __del__(self):
        for lib in self.ac.list_libraries():
            self.ac.delete_library(lib)

    def setup(self, num_symbols):
        pass

    def time_list_symbols(self, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        _ = lib.list_symbols()

    def peakmem_list_symbols(self, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        _ = lib.list_symbols()

    def time_list_versions(self, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        _ = lib.list_versions()

    def peakmem_list_versions(self, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        _ = lib.list_versions()

    def time_has_symbol(self, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        _ = lib.has_symbol("500_sym")
