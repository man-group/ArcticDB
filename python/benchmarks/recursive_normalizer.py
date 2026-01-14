"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd

from arcticdb.util.logger import get_logger
from arcticdb import Arctic


def get_data(num_dict_entry):
    return {
        f"{i}": {"a": pd.DataFrame({"a": [1, 2, 3]}), "b": pd.DataFrame({"b": [1, 2, 3, 4, 5, 6]})}
        for i in range(num_dict_entry)
    }


def get_symbol_name(dict_entry, symbol_idx):
    return f"nested_dict_sym_{dict_entry}_{symbol_idx}"


class LMDBRecursiveNormalizerRead:
    param_names = ["num_dict_entries", "num_symbols"]
    params = [[1000], [5]]
    ARCTIC_URI = "lmdb://recursive_normalizer_read"

    def __init__(self):
        self.logger = get_logger()
        self.ac = None
        self.lib = None

    def setup_cache(self):
        ac = Arctic(LMDBRecursiveNormalizerRead.ARCTIC_URI)
        if "lib" in ac:
            ac.delete_library("lib")

        lib = ac.create_library("lib")

        max_num_symbols = max(self.params[1])
        for num_dict_entry in self.params[0]:
            data = get_data(num_dict_entry)
            for symbol_idx in range(max_num_symbols):
                symbol_name = get_symbol_name(num_dict_entry, symbol_idx)
                lib._nvs.write(symbol_name, data, recursive_normalizers=True)

    def setup(self, num_dict_entry, num_symbols):
        self.lib = Arctic(LMDBRecursiveNormalizerRead.ARCTIC_URI).get_library("lib")

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


class LMDBRecursiveNormalizerWrite:
    param_names = ["num_dict_entries"]
    params = [[10]]
    ARCTIC_URI = "lmdb://recursive_normalizer_write"

    def __init__(self):
        self.logger = get_logger()
        self.ac = None
        self.lib = None
        self.data = None

    def setup_cache(self):
        ac = Arctic(LMDBRecursiveNormalizerWrite.ARCTIC_URI)
        if "lib" in ac:
            ac.delete_library("lib")

        ac.create_library("lib")

    def setup(self, num_dict_entry):
        self.lib = Arctic(LMDBRecursiveNormalizerWrite.ARCTIC_URI).get_library("lib")
        self.data = get_data(num_dict_entry)

    def teardown(self, num_dict_entry):
        pass

    def time_write_nested_dict(self, num_dict_entry):
        assert len(self.data) == num_dict_entry
        self.lib._nvs.write(
            f"nested_dict_time_write_nested_dict_{num_dict_entry}",
            self.data,
            recursive_normalizers=True,
        )

    def peakmem_write_nested_dict(self, num_dict_entry):
        assert len(self.data) == num_dict_entry
        self.lib._nvs.write(
            f"nested_dict_peakmem_write_nested_dict_{num_dict_entry}",
            self.data,
            recursive_normalizers=True,
        )
