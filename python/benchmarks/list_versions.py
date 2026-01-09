"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import itertools
import random
from arcticdb import WritePayload
from asv_runner.benchmarks.mark import SkipNotImplemented

from benchmarks.common import *
from benchmarks.environment_setup import Storage, create_libraries_across_storages, is_storage_enabled, create_libraries


class ListVersions:
    number = 5
    rounds = 1
    timeout = 500
    warmup_time = 0.1

    param_names = [
        "storage",
        "num_symbols",
        "num_versions",
        "num_snapshots",
        "symbol",
        "snapshot",
        "latest_only",
        "skip_snapshots",
    ]
    params = [
        [Storage.LMDB, Storage.AMAZON],
        [10, 100],
        [1, 10],
        [0, 100],
        [None, "0_sym"],
        [None, "0_snap"],
        [True, False],
        [True, False],
    ]

    def __init__(self):
        self.logger = get_logger()

    def check_if_skipped_parameter(
        self, storage, num_symbols, num_versions, num_snapshots, symbol, snapshot, latest_only, skip_snapshots
    ):
        """At the time of writing there are two choices for each parameter. This is 128 parameterizations in total if
        we do not skip any.

        This function skips less interesting combinations, so that the benchmark executes faster."""
        if not is_storage_enabled(storage):
            raise SkipNotImplemented

        if symbol and num_symbols != 10:
            # Cost is independent of the number of symbols when symbol is specified
            raise SkipNotImplemented

        if snapshot and num_snapshots == 0:
            raise SkipNotImplemented

        if latest_only and snapshot:
            # snapshot has no effect when latest_only is specified
            raise SkipNotImplemented

    def _lib_name(self, num_symbols, num_versions, num_snapshots):
        return f"{num_symbols}_num_symbols_{num_versions}_num_versions_{num_snapshots}_num_snapshots"

    def _sym_name(self, sym_idx):
        return f"{sym_idx}_sym"

    def _snap_name(self, snap_idx):
        return f"{snap_idx}_snap"

    def setup_cache(self):
        storages = self.params[0]
        num_symbols = self.params[1]
        num_versions = self.params[2]
        num_snapshots = self.params[3]

        libs_for_storage = dict()

        library_names = [self._lib_name(n_symbols, n_versions, n_snaps) for n_symbols, n_versions, n_snaps in
                         itertools.product(num_symbols, num_versions, num_snapshots)]

        for storage in storages:
            if not is_storage_enabled(storage):
                continue
            libraries = create_libraries(storage, library_names)
            libs_for_storage[storage] = dict(zip(library_names, libraries))
            for syms in num_symbols:
                write_payloads = [WritePayload(self._sym_name(sym), 0) for sym in range(syms)]
                for versions in num_versions:
                    for snapshots in num_snapshots:
                        lib_name = self._lib_name(syms, versions, snapshots)
                        lib = libs_for_storage[storage][lib_name]
                        for _ in range(versions):
                            lib.write_pickle_batch(write_payloads)

                        for snapshot in range(snapshots):
                            lib.snapshot(
                                self._snap_name(snapshot),
                                versions={self._sym_name(sym): random.randint(0, versions - 1) for sym in range(syms)},
                            )

        return libs_for_storage

    def teardown(self, *args):
        pass

    def setup(self, lib_for_storage, storage, num_symbols, num_versions, num_snapshots, symbol, snapshot, latest_only, skip_snapshots):
        self.check_if_skipped_parameter(
            storage, num_symbols, num_versions, num_snapshots, symbol, snapshot, latest_only, skip_snapshots
        )
        self.lib = lib_for_storage[storage][self._lib_name(num_symbols, num_versions, num_snapshots)]
        if self.lib is None:
            raise SkipNotImplemented

    def time_list_versions(
        self, lib_for_storage, storage, num_symbols, num_versions, num_snapshots, symbol, snapshot, latest_only, skip_snapshots
    ):
        assert self.lib.list_versions(symbol=symbol, snapshot=snapshot, latest_only=latest_only, skip_snapshots=skip_snapshots)

    def peakmem_list_versions(
        self, lib_for_storage, storage, num_symbols, num_versions, num_snapshots, symbol, snapshot, latest_only, skip_snapshots
    ):
        assert self.lib.list_versions(symbol=symbol, snapshot=snapshot, latest_only=latest_only, skip_snapshots=skip_snapshots)
