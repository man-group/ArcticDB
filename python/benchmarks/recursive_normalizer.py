"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
from functools import lru_cache

from arcticdb.util.logger import get_logger
from arcticdb import Arctic


@lru_cache(maxsize=None)
def get_data(num_dict_entry):
    return {
        f"{i}": {"a": pd.DataFrame({"a": [1, 2, 3]}), "b": pd.DataFrame({"b": [1, 2, 3, 4, 5, 6]})}
        for i in range(num_dict_entry)
    }


def get_symbol_name(dict_entry, symbol_idx):
    return f"nested_dict_sym_{dict_entry}_{symbol_idx}"


class LMDBRecursiveNormalizer:
    rounds = 1
    number = 3
    repeat = 1
    min_run_count = 1

    timeout = 1200

    param_names = ["num_dict_entries", "num_symbols"]
    params = [[1000], [5]]

    def __init__(self):
        self.logger = get_logger()
        self.ac = None
        self.lib = None

    def setup_cache(self):
        ac = Arctic("lmdb://recursive_normalizer")
        if "lib" in ac:
            ac.delete_library("lib")

        lib = ac.create_library("lib")

        max_num_symbols = max(self.params[1])
        for num_dict_entry in self.params[0]:
            for symbol_idx in range(max_num_symbols):
                symbol_name = get_symbol_name(num_dict_entry, symbol_idx)
                lib._nvs.write(symbol_name, get_data(num_dict_entry), recursive_normalizers=True)

    def setup(self, num_dict_entry, num_symbols):
        self.lib = Arctic("lmdb://recursive_normalizer").get_library("lib")

    def teardown(self, num_dict_entry, num_symbols):
        pass

    def time_read_nested_dict(self, num_dict_entry, num_symbols):
        self.lib.read(get_symbol_name(num_dict_entry, 0))

    def peakmem_read_nested_dict(self, num_dict_entry, num_symbols):
        self.lib.read(get_symbol_name(num_dict_entry, 0))

    def time_read_batch_nested_dict(self, num_dict_entry, num_symbols):
        self.lib.read_batch([get_symbol_name(num_dict_entry, i) for i in range(num_symbols)])

    def peakmem_read_batch_nested_dict(self, num_dict_entry, num_symbols):
        self.lib.read_batch([get_symbol_name(num_dict_entry, i) for i in range(num_symbols)])

    def time_write_nested_dict(self, num_dict_entry, num_symbols):
        self.lib._nvs.write(
            f"nested_dict_time_write_nested_dict_{num_dict_entry}",
            get_data(num_dict_entry),
            recursive_normalizers=True,
        )

    def peakmem_write_nested_dict(self, num_dict_entry, num_symbols):
        self.lib._nvs.write(
            f"nested_dict_peakmem_write_nested_dict_{num_dict_entry}",
            get_data(num_dict_entry),
            recursive_normalizers=True,
        )
