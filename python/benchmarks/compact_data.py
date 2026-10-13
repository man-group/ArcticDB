"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import shutil
import time

import numpy as np
import pandas as pd
import random

from arcticdb import Arctic, LibraryOptions, WritePayload
from arcticdb.config import set_config_int
from arcticdb.options import ModifiableLibraryOption
from arcticdb.util.logger import get_logger
from arcticdb.util.test import random_strings_of_length

from benchmarks.common import lib_name

random.seed(42)
rng = np.random.default_rng(42)

# ---- EXPERIMENT ONLY (compact-strings-experiment): per-call diagnostics, never timed ----
import sys

_AW_LINUX = sys.platform.startswith("linux")
# Pin every thread of the benchmark process to a small fixed CPU set (set True on the -pin branch)
_AW_PIN = False
_AW_PIN_COUNT = 4


def _aw_pin_set():
    """Up to _AW_PIN_COUNT logical CPUs, skipping CPUs 0-1, each on a distinct physical core."""
    chosen, cores = [], set()
    for cpu in sorted(os.sched_getaffinity(0)):
        if cpu < 2:
            continue
        try:
            base = f"/sys/devices/system/cpu/cpu{cpu}/topology"
            core = (open(f"{base}/physical_package_id").read().strip(), open(f"{base}/core_id").read().strip())
        except OSError:
            core = ("?", str(cpu))
        if core in cores:
            continue
        cores.add(core)
        chosen.append(cpu)
        if len(chosen) == _AW_PIN_COUNT:
            break
    return set(chosen)


_AW_ALL_CPUS = sorted(os.sched_getaffinity(0)) if _AW_LINUX else []
_AW_PIN_SET = _aw_pin_set() if (_AW_LINUX and _AW_PIN) else set()
_AW_PIN_LOGGED = False


def _aw_pin_all_threads():
    global _AW_PIN_LOGGED
    bad = []
    tids = os.listdir("/proc/self/task")
    for tid in tids:
        try:
            os.sched_setaffinity(int(tid), _AW_PIN_SET)
            if os.sched_getaffinity(int(tid)) != _AW_PIN_SET:
                bad.append(tid)
        except OSError:
            pass
    if not _AW_PIN_LOGGED or bad:
        _AW_PIN_LOGGED = True
        print(
            f"AWPIN pid={os.getpid()} all={_AW_ALL_CPUS} pin={sorted(_AW_PIN_SET)} threads={len(tids)} mismatched={bad}",
            file=sys.stderr,
            flush=True,
        )


def _aw_threads():
    """{tid: (ns on CPU, last CPU)} for every thread of this process."""
    out = {}
    for tid in os.listdir("/proc/self/task"):
        try:
            with open(f"/proc/self/task/{tid}/schedstat") as f:
                ns = int(f.read().split()[0])
            with open(f"/proc/self/task/{tid}/stat") as f:
                last_cpu = int(f.read().rsplit(")", 1)[1].split()[36])
            out[tid] = (ns, last_cpu)
        except (OSError, ValueError, IndexError):
            pass
    return out



class CompactDataBase:
    def __init__(self):
        self.logger = get_logger()
        self.SYM = "sym"
        # Do not interleave benchmarks as they are using the same LMDB directory for actually running the benchmarks
        self.rounds = 1
        # These two parameters are important, because compaction is a destructive process, we must call setup before
        # each measurement
        self.number = 1
        self.warmup_time = 0
        # Each derived benchmark class takes less than 2 minutes total locally on an SSD
        self.repeat = 15
        self.ac = None
        self.lib = None
        self.base_param_names = [
            "(num_rows, initial_rows_per_segment, target_rows_per_segment)",
            "num_columns",
            "column_slicing",
        ]

    def finish_init(self):
        # Base LMDB instance that will be populated by setup_cache. Relevant libraries will then be copied from here
        # to another directory for actual compaction
        self.LMDB_BASE_DIR = f"{self.LMDB_DIR}_base"
        self.CONNECTION_STRING_BASE = f"lmdb://{self.LMDB_BASE_DIR}"
        self.CONNECTION_STRING = f"lmdb://{self.LMDB_DIR}"

    def _setup_cache_base(self, ac, lib_name, rows_per_segment, columns_per_segment, dfs):
        ac.delete_library(lib_name)
        lib = ac.create_library(
            lib_name,
            LibraryOptions(
                dynamic_schema=self.DYNAMIC_SCHEMA,
                rows_per_segment=rows_per_segment,
                columns_per_segment=columns_per_segment,
            ),
        )
        for df in dfs:
            lib.append(self.SYM, df)

    def _setup(self, lib_name, target_rows_per_segment):
        if _AW_PIN_SET:
            _aw_pin_all_threads()
        os.mkdir(self.LMDB_DIR)
        # Copy the config database and the relevant library database for these benchmark parameters to the actual
        # LMDB directory where compaction will happen
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, "_arctic_cfg"), os.path.join(self.LMDB_DIR, "_arctic_cfg"))
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, lib_name), os.path.join(self.LMDB_DIR, lib_name))
        # Create a new Arctic instance, otherwise we will be holding a reference to the previous iteration's .mdb files
        # and the deletion and recreation won't be noticed by Arctic
        del self.ac
        # Skip LMDB's fsync on every commit (MDB_NOSYNC | MDB_NOMETASYNC). compact_data commits each key in its own
        # transaction, so with syncing on, the low-cardinality parameters measure ~10-20 disk flushes rather than
        # compaction (>90% of their runtime), and flush latency on the CI disks is bimodal. This data is throwaway.
        set_config_int("LMDBStorage.ExtraFlags", 0x50000)
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(lib_name)
        # Check the compaction will actually do something!
        assert self.lib.compact_data_explain_plan(self.SYM, rows_per_segment=target_rows_per_segment).will_do_work
        # read the symbol to warm up the cache
        self.lib.read(self.SYM)
        if _AW_LINUX:
            self._aw_lib = lib_name.replace(" ", "")
            self._aw_before = _aw_threads()
            self._aw_t0 = time.perf_counter()

    def _teardown(self):
        if _AW_LINUX and getattr(self, "_aw_t0", None) is not None:
            wall = time.perf_counter() - self._aw_t0
            after = _aw_threads()
            used = {
                tid: (ns - self._aw_before.get(tid, (0, 0))[0], cpu)
                for tid, (ns, cpu) in after.items()
                if ns - self._aw_before.get(tid, (0, 0))[0] > 200_000
            }
            cpu_ms = sum(ns for ns, _ in used.values()) / 1e6
            print(
                f"AWCALL {self._aw_lib} wall_ms={wall * 1e3:.3f} cpu_ms={cpu_ms:.3f} "
                f"threads_used={len(used)} threads={len(after)} lastcpus={sorted({c for _, c in used.values()})}",
                file=sys.stderr,
                flush=True,
            )
            self._aw_t0 = None
        shutil.rmtree(self.LMDB_DIR)

    def compact_data(self, target_rows_per_segment):
        # Prune previous disabled so we are not also measuring memory/time for the deletion step
        self.lib.compact_data(self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False)


class CompactDataNumericStaticSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = False
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "compact_data_numeric_static_schema"
        super().finish_init()
        self.param_names = self.base_param_names
        self.params = [
            [
                (1_000_000, 10_000, 100_000),
                (100_000, 100_000, 10_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [2, 10, 100],  # num_columns
            [False, True],  # column_slicing
        ]

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, _ = row_params
            for num_columns in self.params[1]:
                for column_slicing in self.params[2]:
                    # Create one library per combination of benchmark parameters, as they don't all use the same slicing
                    df = pd.DataFrame(
                        {f"col_{i}": np.arange(i * num_rows, (i + 1) * num_rows) for i in range(num_columns)}
                    )
                    self._setup_cache_base(
                        ac,
                        lib_name(row_params, num_columns, column_slicing),
                        initial_rows_per_segment,
                        num_columns // 2 if column_slicing else num_columns * 2,
                        [df],
                    )

    def setup(self, row_params, num_columns, column_slicing):
        self._setup(lib_name(row_params, num_columns, column_slicing), row_params[2])

    def teardown(self, row_params, num_columns, column_slicing):
        self._teardown()

    def time_compact_data(self, row_params, num_columns, column_slicing):
        self.compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns, column_slicing):
        self.compact_data(row_params[2])


class CompactDataStringsStaticSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = False
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "compact_data_strings_static_schema"
        super().finish_init()
        self.param_names = self.base_param_names + ["num_unique_strings"]
        self.params = [
            [
                (1_000_000, 10_000, 100_000),
                (100_000, 100_000, 10_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [2, 10],  # num_columns
            [False, True],  # column_slicing
            [2, 10, 100_000],  # num_unique_strings
        ]
        # sorted: random_strings_of_length(unique=True) returns list(set(...)), whose order follows the per-process string
        # hash seed, so without sorting every run would pick different strings for the low-cardinality parameters
        self.unique_strings = sorted(
            random_strings_of_length(max(self.params[3]), length=10, unique=True, kind="ascii")
        )

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, _ = row_params
            for num_columns in self.params[1]:
                for column_slicing in self.params[2]:
                    for num_unique_strings in self.params[3]:
                        # Create one library per combination of benchmark parameters, as they don't all use the same
                        # slicing
                        strings = self.unique_strings[:num_unique_strings]
                        df = pd.DataFrame({f"col_{i}": rng.choice(strings, num_rows) for i in range(num_columns)})
                        self._setup_cache_base(
                            ac,
                            lib_name(row_params, num_columns, column_slicing, num_unique_strings),
                            initial_rows_per_segment,
                            num_columns // 2 if column_slicing else num_columns * 2,
                            [df],
                        )

    def setup(self, row_params, num_columns, column_slicing, num_unique_strings):
        self._setup(lib_name(row_params, num_columns, column_slicing, num_unique_strings), row_params[2])

    def teardown(self, row_params, num_columns, column_slicing, num_unique_strings):
        self._teardown()

    def time_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        self.compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        target_rows_per_segment = row_params[2]
        self.compact_data(row_params[2])


class CompactDataNumericDynamicSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = True
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "compact_data_numeric_dynamic_schema"
        super().finish_init()
        self.param_names = self.base_param_names[:2]
        self.params = [
            [
                (1_000, 10, 1_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [100, 1_000, 10_000],  # num_columns
        ]

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, _ = row_params
            for num_columns in self.params[1]:
                # Create one library per combination of benchmark parameters, as they don't all use the same slicing
                num_row_slices = num_rows // initial_rows_per_segment
                column_names = [f"col_{idx}" for idx in range(num_columns)]
                dfs = []
                for _ in range(num_row_slices):
                    columns = rng.choice(column_names, num_columns // 2, replace=False)
                    dfs.append(pd.DataFrame({column: np.arange(initial_rows_per_segment) for column in columns}))
                self._setup_cache_base(
                    ac,
                    lib_name(row_params, num_columns),
                    initial_rows_per_segment,
                    0,  # Column slicing doesn't apply to dynamic schema
                    dfs,
                )

    def setup(self, row_params, num_columns):
        self._setup(lib_name(row_params, num_columns), row_params[2])

    def teardown(self, row_params, num_columns):
        self._teardown()

    def time_compact_data(self, row_params, num_columns):
        self.compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns):
        self.compact_data(row_params[2])


class AppendCompactDataBase:
    def __init__(self):
        self.logger = get_logger()
        # Do not interleave benchmarks as they are using the same LMDB directory for actually running the benchmarks
        self.rounds = 1
        # These two parameters are important, because appending with compact_data=True is a destructive process, we must
        # call setup before each measurement
        self.number = 1
        self.warmup_time = 0
        # Total runtime of the 3 derived benchmark classes is ~10m locally on an SSD
        self.repeat = 7
        self.ac = None
        self.lib = None
        self.base_param_names = ["num_symbols", "existing_data_fragmented", "append_rows"]

    def finish_init(self):
        self.SYMS = [f"sym_{i}" for i in range(self.params[0][-1])]
        # Base LMDB instance that will be populated by setup_cache. Relevant libraries will then be copied from here
        # to another directory for actual compaction
        self.LMDB_BASE_DIR = f"{self.LMDB_DIR}_base"
        self.CONNECTION_STRING_BASE = f"lmdb://{self.LMDB_BASE_DIR}"
        self.CONNECTION_STRING = f"lmdb://{self.LMDB_DIR}"

    def _setup_cache_base(self, ac, lib_name, dfs):
        ac.delete_library(lib_name)
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=self.DYNAMIC_SCHEMA))
        for df in dfs:
            lib.append_batch([WritePayload(sym, df) for sym in self.SYMS])

    def _setup(self, lib_name):
        os.mkdir(self.LMDB_DIR)
        # Copy the config database and the relevant library database for these benchmark parameters to the actual
        # LMDB directory where compaction will happen
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, "_arctic_cfg"), os.path.join(self.LMDB_DIR, "_arctic_cfg"))
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, lib_name), os.path.join(self.LMDB_DIR, lib_name))
        # Create a new Arctic instance, otherwise we will be holding a reference to the previous iteration's .mdb files
        # and the deletion and recreation won't be noticed by Arctic
        del self.ac
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(lib_name)

    def _teardown(self):
        shutil.rmtree(self.LMDB_DIR)

    def append(self, num_symbols):
        # Prune previous disabled so we are not also measuring memory/time for the deletion step
        if num_symbols == 1:
            self.lib.append(self.SYMS[0], self.append_df, prune_previous_versions=False, compact_data=True)
        else:
            self.lib.append_batch(
                [WritePayload(sym, self.append_df) for sym in self.SYMS[:num_symbols]],
                prune_previous_versions=False,
                compact_data=True,
            )


class AppendCompactDataNumericStaticSchema(AppendCompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = False
        self.NUM_COLUMNS = 10
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "append_compact_data_numeric_static_schema"
        self.param_names = self.base_param_names
        self.params = [
            [1, 10],  # num_symbols
            [False, True],  # existing_data_fragmented
            [1, 50_000, 1_000_000],  # append_rows
        ]
        super().finish_init()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        num_rows = 1_000_000
        df = pd.DataFrame({f"col_{i}": np.arange(i * num_rows, (i + 1) * num_rows) for i in range(self.NUM_COLUMNS)})
        for existing_data_fragmented in self.params[1]:
            if existing_data_fragmented:
                dfs = [df[i * 10_000 : (i + 1) * 10_000] for i in range(num_rows // 10_000)]
            else:
                dfs = [df]
            self._setup_cache_base(ac, lib_name(existing_data_fragmented), dfs)

    def setup(self, num_symbols, existing_data_fragmented, append_rows):
        self.append_df = pd.DataFrame(
            {f"col_{i}": np.arange(i * append_rows, (i + 1) * append_rows) for i in range(self.NUM_COLUMNS)}
        )
        self._setup(lib_name(existing_data_fragmented))

    def teardown(self, num_symbols, existing_data_fragmented, append_rows):
        self._teardown()

    def time_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)

    def peakmem_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)


class AppendCompactDataStringsStaticSchema(AppendCompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = False
        self.NUM_COLUMNS = 10
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "append_compact_data_strings_static_schema"
        self.param_names = self.base_param_names
        self.params = [
            [1, 10],  # num_symbols
            [False, True],  # existing_data_fragmented
            [1, 50_000, 1_000_000],  # append_rows
        ]
        super().finish_init()
        self.unique_strings = sorted(random_strings_of_length(10, length=10, unique=True, kind="ascii"))

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        num_rows = 1_000_000
        df = pd.DataFrame({f"col_{i}": rng.choice(self.unique_strings, num_rows) for i in range(self.NUM_COLUMNS)})
        for existing_data_fragmented in self.params[1]:
            if existing_data_fragmented:
                dfs = [df[i * 10_000 : (i + 1) * 10_000] for i in range(num_rows // 10_000)]
            else:
                dfs = [df]
            self._setup_cache_base(ac, lib_name(existing_data_fragmented), dfs)

    def setup(self, num_symbols, existing_data_fragmented, append_rows):
        self.append_df = pd.DataFrame(
            {f"col_{i}": rng.choice(self.unique_strings, append_rows) for i in range(self.NUM_COLUMNS)}
        )
        self._setup(lib_name(existing_data_fragmented))

    def teardown(self, num_symbols, existing_data_fragmented, append_rows):
        self._teardown()

    def time_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)

    def peakmem_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)


class AppendCompactDataNumericDynamicSchema(AppendCompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = True
        self.NUM_COLUMNS = 10_000
        self.COLUMN_NAMES = [f"col_{idx}" for idx in range(self.NUM_COLUMNS)]
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "append_compact_data_numeric_dynamic_schema"
        self.param_names = self.base_param_names
        self.params = [
            [1, 10],  # num_symbols
            [False, True],  # existing_data_fragmented
            [1, 1_000],  # append_rows
        ]
        super().finish_init()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        num_rows = 1_000
        for existing_data_fragmented in self.params[1]:
            num_row_slices = 10 if existing_data_fragmented else 1

            dfs = []
            for _ in range(num_row_slices):
                columns = rng.choice(self.COLUMN_NAMES, self.NUM_COLUMNS // 2, replace=False)
                dfs.append(pd.DataFrame({column: np.arange(num_rows // num_row_slices) for column in columns}))
            self._setup_cache_base(ac, lib_name(existing_data_fragmented), dfs)

    def setup(self, num_symbols, existing_data_fragmented, append_rows):
        columns = rng.choice(self.COLUMN_NAMES, self.NUM_COLUMNS // 2, replace=False)
        self.append_df = pd.DataFrame({column: np.arange(append_rows) for column in columns})
        self._setup(lib_name(existing_data_fragmented))

    def teardown(self, num_symbols, existing_data_fragmented, append_rows):
        self._teardown()

    def time_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)

    def peakmem_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)
