"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from asv_runner.benchmarks.mark import SkipNotImplemented

from .environment_setup import Storage, create_libraries_across_storages
from benchmarks.common import *

from arcticdb.version_store.library import WritePayload


class ListSymbolsWithoutCache:
    """Measure how long symbol list operations take before the symbol list cache is compacted."""

    rounds = 1
    number = 1  # run only once within a setup-teardown
    timeout = 600
    warmup_time = 0

    storages = [Storage.LMDB, Storage.AMAZON]
    num_symbols = [100, 1000]

    params = [num_symbols, storages]
    param_names = ["num_symbols", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.test_counter = (
            None  # used to ensure each test runs once within a setup-teardown cycle, so that the cache is uncompacted
        )

    def setup_cache(self):
        lib_for_storage = create_libraries_across_storages(self.storages)
        return lib_for_storage

    def teardown(self, *args):
        if self.lib is not None:
            self.lib._nvs.version_store.clear()

    def setup(self, lib_for_storage, num_symbols, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented
        self.test_counter = 1

        simple_df = pd.DataFrame({"a": [1]})
        write_payloads = [WritePayload(f"{i}", simple_df) for i in range(num_symbols)]
        self.lib.write_batch(write_payloads)

    def time_list_symbols(self, *args):
        self._check_test_counter()
        self.test_counter += 1
        self.lib.list_symbols()

    def peakmem_list_symbols(self, *args):
        self._check_test_counter()
        self.lib.list_symbols()

    def time_has_symbol(self, *args):
        self._check_test_counter()
        self.lib.has_symbol("250_sym")

    def _check_test_counter(self):
        assert self.test_counter == 1
        self.test_counter += 1
