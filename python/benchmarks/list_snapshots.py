"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import itertools
import multiprocessing
import sys

from arcticdb import WritePayload

from arcticdb.config import set_config_int
from arcticdb.util.test_utils import CachedDFGenerator
from asv_runner.benchmarks.mark import SkipNotImplemented
from benchmarks.common import *
from arcticdb.util.logger import get_logger

from benchmarks.environment_setup import Storage, create_libraries, is_storage_enabled


def get_metadata(n_entries: int):
    return {f"{i}": [i, sys.maxsize - i] for i in range(n_entries)}


def get_lib_name(num_syms: int, num_snaps: int, metadata_size: str):
    return f"n_syms-{num_syms}__n_snaps-{num_snaps}__md_size-{metadata_size}"


# `VersionMap.ReloadInterval`, in nanoseconds. ArcticDB's default is two seconds
# (DEFAULT_RELOAD_INTERVAL, cpp/arcticdb/version/version_map.hpp:112).
_POPULATION_VERSION_CACHE_NS = 24 * 60 * 60 * 1_000_000_000


class Snapshots:
    storages = [Storage.LMDB, Storage.AMAZON]
    num_symbols = [1, 1_000]
    num_snapshots = [1, 1_000]
    metadata_entries = [0, 10_000]
    load_metadata = [True, False]
    timeout = 3_000

    params = [storages, num_symbols, num_snapshots, metadata_entries, load_metadata]
    param_names = ["storage", "num_symbols", "num_snapshots", "metadata_entries", "load_metadata"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        # `lib.snapshot()` with no explicit `versions` snapshots the latest version of
        # every symbol, which means reading the version ref key of every symbol in the
        # library (PythonVersionStore::snapshot -> batch_get_latest_version). Those
        # reads come from the version map's in-process cache, but the cache is only
        # honoured for `VersionMap.ReloadInterval`, two seconds by default. Against
        # real S3 a 1000-symbol pass takes longer than that, so the cache has always
        # expired by the next snapshot and each one pays ~1000 extra GETs: 1004
        # storage requests per snapshot instead of 4. At S3's ~25ms round trip and
        # ArcticDB's default six IO threads that is ~4.4s per snapshot rather than
        # ~0.13s, so each of the two 1000-symbol x 1000-snapshot libraries takes over
        # an hour to populate and asv kills the whole setup (see setup_cache.timeout).
        #
        # Nothing writes to a library after its own write_batch below, so the cached
        # versions cannot go stale during the snapshot loop and holding them for the
        # duration is safe. It puts the 1000-symbol libraries back on the same four
        # requests per snapshot as the single-symbol ones. asv runs setup_cache in its
        # own process, so this does not reach the benchmarks themselves.
        set_config_int("VersionMap.ReloadInterval", _POPULATION_VERSION_CACHE_NS)

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

                metadata = get_metadata(md_size)
                for i in range(n_snaps):
                    lib.snapshot(f"snap_{i}", metadata=metadata)

        return libs_for_storage

    # asv's budget for setup_cache. This is deliberately not the `timeout` class
    # attribute above and not a `setup_cache_timeout` class attribute either: asv
    # reads the setup_cache budget off the setup_cache function object
    # (asv_runner/benchmarks/_base.py, Benchmark.__init__ -> `setup_cache_timeout =
    # _get_first_attr([self._setup_cache], "timeout", None)`), so a class attribute of
    # that name is silently ignored and the class `timeout` is used instead.
    #
    # Note also that asv applies it as an *idle* timeout on the child's output rather
    # than a wall clock limit (asv/util.py, `select.select(..., timeout)` in
    # _run_recursive). setup_cache prints only once per library, so in practice this
    # caps the population of a single library. The slowest is ~2.5 minutes against
    # real S3, so 30 minutes leaves a wide margin while still failing a regression in
    # half the time the unbudgeted 3000s above took to fail.
    setup_cache.timeout = 1_800

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
