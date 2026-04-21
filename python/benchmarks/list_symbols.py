"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from asv_runner.benchmarks.mark import SkipNotImplemented

from .environment_setup import Storage, create_libraries_across_storages
from benchmarks.common import *
from arcticdb.util.logger import get_logger

from arcticdb.version_store.library import WritePayload


class ListSymbolsWithoutCache:
    """Measure how long symbol list operations take before the symbol list cache is compacted."""

    rounds = 1
    number = 1  # run only once within a setup-teardown
    timeout = 600
    warmup_time = 0

    storages = [Storage.LMDB, Storage.AMAZON]
    num_symbols = [1000]

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
        self.lib.list_symbols()

    def peakmem_list_symbols(self, *args):
        self._check_test_counter()
        self.lib.list_symbols()

    def time_has_symbol(self, *args):
        self._check_test_counter()
        for i in range(50, 150):
            self.lib.has_symbol(f"{i}")

    def _check_test_counter(self):
        assert self.test_counter == 1
        self.test_counter += 1


class ListSymbolsWithCompactedCache:
    """Measure list_symbols when the cache is already compacted with some journal entries on top.

    This is the common production scenario: the cache has been compacted and a moderate number
    of writes/deletes have happened since the last compaction."""

    rounds = 1
    number = 3
    timeout = 600
    warmup_time = 0

    storages = [Storage.LMDB, Storage.AMAZON]
    num_symbols = [1_000]
    num_journal_entries = [0, 100]

    params = [num_symbols, num_journal_entries, storages]
    param_names = ["num_symbols", "num_journal_entries", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None

    def setup_cache(self):
        return create_libraries_across_storages(self.storages)

    def teardown(self, *args):
        if self.lib is not None:
            self.lib._nvs.version_store.clear()

    def setup(self, lib_for_storage, num_symbols, num_journal_entries, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented

        simple_df = pd.DataFrame({"a": [1]})

        # Write symbols in batches to avoid very large payloads
        batch_size = 1000
        for start in range(0, num_symbols, batch_size):
            end = min(start + batch_size, num_symbols)
            payloads = [WritePayload(f"sym_{i}", simple_df) for i in range(start, end)]
            self.lib.write_batch(payloads)

        # Trigger compaction by calling list_symbols
        self.lib.list_symbols()

        # Add journal entries on top of the compacted cache
        for i in range(num_journal_entries):
            self.lib.write(f"sym_{i}", simple_df)

    def time_list_symbols(self, *args):
        self.lib.list_symbols()

    def peakmem_list_symbols(self, *args):
        self.lib.list_symbols()


class ListSymbolsCompaction:
    """Measure list_symbols when compaction is triggered with many uncompacted journal entries.

    This is the scenario where streaming optimization matters most: many journal entries
    need to be merged during compaction."""

    rounds = 1
    number = 1
    timeout = 1200
    warmup_time = 0

    storages = [Storage.LMDB, Storage.AMAZON]
    num_symbols = [1_000]
    num_versions = [10]

    params = [num_symbols, num_versions, storages]
    param_names = ["num_symbols", "num_versions", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None

    def setup_cache(self):
        return create_libraries_across_storages(self.storages)

    def teardown(self, *args):
        if self.lib is not None:
            self.lib._nvs.version_store.clear()

    def setup(self, lib_for_storage, num_symbols, num_versions, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented

        simple_df = pd.DataFrame({"a": [1]})

        # Write multiple versions per symbol to create many journal entries
        # Total entries = num_symbols * num_versions
        for v in range(num_versions):
            batch_size = 1000
            for start in range(0, num_symbols, batch_size):
                end = min(start + batch_size, num_symbols)
                payloads = [WritePayload(f"sym_{i}", simple_df) for i in range(start, end)]
                self.lib.write_batch(payloads)

    def time_list_symbols(self, *args):
        # This triggers compaction since there are many uncompacted entries
        self.lib.list_symbols()

    def peakmem_list_symbols(self, *args):
        self.lib.list_symbols()


class ListSymbolsWithDeletes:
    """Measure list_symbols with a mix of adds and deletes since last compaction.

    Tests the merge path where some symbols have been deleted and new ones added."""

    rounds = 1
    number = 3
    timeout = 600
    warmup_time = 0

    storages = [Storage.LMDB, Storage.AMAZON]
    num_symbols = [1_000]

    params = [num_symbols, storages]
    param_names = ["num_symbols", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None

    def setup_cache(self):
        return create_libraries_across_storages(self.storages)

    def teardown(self, *args):
        if self.lib is not None:
            self.lib._nvs.version_store.clear()

    def setup(self, lib_for_storage, num_symbols, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented

        simple_df = pd.DataFrame({"a": [1]})

        batch_size = 1000
        for start in range(0, num_symbols, batch_size):
            end = min(start + batch_size, num_symbols)
            payloads = [WritePayload(f"sym_{i}", simple_df) for i in range(start, end)]
            self.lib.write_batch(payloads)

        # Compact
        self.lib.list_symbols()

        # Delete 10% of symbols and add new ones
        num_to_delete = num_symbols // 10
        for i in range(num_to_delete):
            self.lib.delete(f"sym_{i}")
        for i in range(num_to_delete):
            self.lib.write(f"sym_new_{i}", simple_df)

    def time_list_symbols(self, *args):
        self.lib.list_symbols()

    def peakmem_list_symbols(self, *args):
        self.lib.list_symbols()
