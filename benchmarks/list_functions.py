from arcticdb import Arctic

from .common import *

class BasicFunctions:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    number = 5
    timeout = 6000

    params = (1000, [500, 1000])
    param_names = ['rows', 'num_symbols']

    def __init__(self):
        self.ac = Arctic("lmdb://basic_functions")

        num_rows, num_symbols = BasicFunctions.params
        for syms in num_symbols:
            lib = f"{syms}_num_symbols"
            self.ac.delete_library(lib)
            self.ac.create_library(lib)
            lib = self.ac[lib]
            for sym in range(syms):
                lib.write(f"{sym}_sym", generate_pseudo_random_dataframe(num_rows))

    def setup(self, rows, num_symbols):
        pass

    def time_list_symbols(self, rows, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        syms = lib.list_symbols()

    def peakmem_list_symbols(self, rows, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        syms = lib.list_symbols()

    def time_list_versions(self, rows, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        syms = lib.list_versions()

    def peakmem_list_versions(self, rows, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        syms = lib.list_versions()

    def time_has_symbol(self, _, num_symbols):
        lib = self.ac[f"{num_symbols}_num_symbols"]
        sym = lib.has_symbol("500_sym")
