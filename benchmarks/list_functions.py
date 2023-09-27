from arcticdb import Arctic

from .common import *

class ListFunctions:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    number = 5
    timeout = 6000

    params = ([500, 1000])
    param_names = ['num_symbols']

    rows = 1000

    def __init__(self):
        self.ac = Arctic("lmdb://list_functions")

        num_symbols = ListFunctions.params
        for syms in num_symbols:
            lib = f"{syms}_num_symbols"
            self.ac.delete_library(lib)
            self.ac.create_library(lib)
            lib = self.ac[lib]
            for sym in range(syms):
                lib.write(f"{sym}_sym", generate_pseudo_random_dataframe(ListFunctions.rows))

    def setup(self, _):
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
