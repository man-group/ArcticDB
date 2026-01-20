"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import itertools
import multiprocessing
import sys

from arcticdb import WritePayload

from arcticdb.util.test_utils import CachedDFGenerator
from asv_runner.benchmarks.mark import SkipNotImplemented
from benchmarks.common import *

from benchmarks.environment_setup import Storage, create_libraries, is_storage_enabled


def get_metadata(n_entries: int):
    return {f"{i}": [i, sys.maxsize - i] for i in range(n_entries)}


def get_lib_name(num_syms: int, num_snaps: int, metadata_size: str):
    return f"n_syms-{num_syms}__n_snaps-{num_snaps}__md_size-{metadata_size}"


def create_snapshot(lib, i, metadata_size):
    metadata = get_metadata(metadata_size)
    lib.snapshot(f"snap_{i}", metadata=metadata)


class Snapshots:
    storages = [Storage.LMDB, Storage.AMAZON]
    num_symbols = [1, 1_000]
    num_snapshots = [1, 1_000]
    metadata_entries = [0, 10_000]
    load_metadata = [True, False]
    timeout = 1_000

    params = [storages, num_symbols, num_snapshots, metadata_entries, load_metadata]
    param_names = ["storage", "num_symbols", "num_snapshots", "metadata_entries", "load_metadata"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        write_parameters = list(itertools.product(self.num_symbols, self.num_snapshots, self.metadata_entries))
        assert write_parameters
        libs_for_storage = dict()
        library_names = [
            get_lib_name(num_syms=n_syms, num_snaps=n_snaps, metadata_size=md_size)
            for n_syms, n_snaps, md_size in write_parameters
        ]
        simple_df = pd.DataFrame({"a": [1]})

        for storage in self.storages:
            libraries = create_libraries(storage, library_names)
            libs_for_storage[storage] = dict(zip(library_names, libraries))
            if not is_storage_enabled(storage):
                continue

            for n_syms, n_snaps, md_size in write_parameters:
                lib_name = get_lib_name(n_syms, n_snaps, md_size)
                lib = libs_for_storage[storage][lib_name]
                print(f"lib_name={lib_name}, lib={lib}", file=sys.stderr)
                if lib is None:
                    continue
                writes = [WritePayload(f"sym_{i}", simple_df) for i in range(n_syms)]

                lib.write_batch(writes)

                inputs = [(lib, i, md_size) for i in range(n_snaps)]
                with multiprocessing.get_context("spawn").Pool(10) as p:
                    p.starmap(create_snapshot, inputs)

        return libs_for_storage

    def setup(self, libs_for_storage, storage, num_symbols, num_snapshots, metadata_entries, load_metadata):
        self.lib = libs_for_storage[storage][get_lib_name(num_symbols, num_snapshots, metadata_entries)]
        if self.lib is None:
            raise SkipNotImplemented

    def time_list_snapshots(
        self, libs_for_storage, storage, num_symbols, num_snapshots, metadata_entries, load_metadata
    ):
        res = self.lib.list_snapshots(load_metadata=load_metadata)
        assert len(res) == num_snapshots, f"Expected {num_snapshots} snapshots but were {len(res)}"

    def peakmem_list_snapshots(
        self, libs_for_storage, storage, num_symbols, num_snapshots, metadata_entries, load_metadata
    ):
        res = self.lib.list_snapshots(load_metadata=load_metadata)
        assert len(res) == num_snapshots, f"Expected {num_snapshots} snapshots but were {len(res)}"
