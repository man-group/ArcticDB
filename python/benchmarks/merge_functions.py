"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import random
import uuid

import numpy as np
import pandas as pd

from arcticc.pb2.s3_storage_pb2 import Config as S3Config
from arcticdb import Arctic
from arcticdb.version_store.library import MergeStrategy
from arcticdb.util.test import random_strings_of_length
from arcticdb_ext.storage import CONFIG_LIBRARY_NAME
from asv_runner.benchmarks.mark import SkipNotImplemented

from benchmarks.seaweed_utils import (
    ARCTICDB_CACHE_BUCKET,
    arctic_uri,
    copy_bucket,
    create_bucket,
    delete_bucket,
    list_buckets,
    reset_arcticdb_cache_bucket,
)

MIN_DATE = pd.Timestamp("1960-01-01")
MAX_DATE = pd.Timestamp("2025-01-01")

# merge_experimental is destructive, so every measurement runs against a pristine copy of the
# data: setup copies the config library and the relevant data library from the cache bucket into
# a brand new bucket and the merge runs there; teardown hard-deletes that bucket by dropping its
# backing SeaweedFS collection.

# Short-lived per-measurement bucket; each class creates its own in setup() and deletes it in
# teardown(). The WORK_BUCKET_PREFIX sweep in _run_setup_cache() cleans up any that a killed
# benchmark process (e.g. an ASV timeout) failed to tear down
WORK_BUCKET_PREFIX = "arcticdb-merge-bench-"


def _env_repeat(default):
    # One knob to shrink local experiment runs; leaves CI defaults untouched
    return int(os.getenv("ARCTICDB_MERGE_BENCH_REPEAT", default))


_LIBRARY_PREFIX_CACHE = {}


def _cache_library_prefix(lib_name):
    """S3 key prefix of a library in the cache bucket. Libraries created through Arctic get a
    unique "<name><nanos>" prefix, so it must be read back from the library config."""
    if lib_name not in _LIBRARY_PREFIX_CACHE:
        lib = Arctic(arctic_uri(ARCTICDB_CACHE_BUCKET)).get_library(lib_name)
        config = lib._nvs.lib_cfg()
        storage = config.storage_by_id[config.lib_desc.storage_ids[0]]
        s3_config = S3Config()
        storage.config.Unpack(s3_config)
        _LIBRARY_PREFIX_CACHE[lib_name] = s3_config.prefix
    return _LIBRARY_PREFIX_CACHE[lib_name]


def _random_values(rng, num_rows, value_dtype, num_unique_strings):
    if value_dtype == "string":
        random.seed(0)
        strings = sorted(random_strings_of_length(num_unique_strings, length=10, unique=True, kind="ascii"))
        return rng.choice(strings, num_rows)
    return rng.random(num_rows, dtype=np.float64)


def _random_dates(rng, num_rows):
    return pd.to_datetime(rng.integers(MIN_DATE.value, MAX_DATE.value, size=num_rows), unit="ns")


def generate_merge_target(num_rows, num_value_cols, value_dtype, index_kind, num_unique_strings=None):
    rng = np.random.default_rng(0)
    df = pd.DataFrame(
        {f"val_{j}": _random_values(rng, num_rows, value_dtype, num_unique_strings) for j in range(num_value_cols)}
    )
    if index_kind == "datetime":
        df.index = _random_dates(rng, num_rows).sort_values()
    else:
        df.index = pd.RangeIndex(num_rows)
    return df


def generate_merge_source(target, source_size, matched_count, on=None, value_dtype="float", num_unique_strings=None):
    on_cols = on or []
    index_kind = "datetime" if isinstance(target.index, pd.DatetimeIndex) else "rowrange"
    match_cols = on_cols + (["index"] if index_kind == "datetime" else [])
    rng = np.random.default_rng(1)
    picks = rng.choice(len(target), size=matched_count, replace=False)
    matched = target.iloc[picks]
    matched = matched[~matched.reset_index().duplicated(subset=match_cols, keep="first").values].copy()
    for col in target.columns:
        if col not in on_cols:
            matched[col] = _random_values(rng, len(matched), value_dtype, num_unique_strings)

    rest = pd.DataFrame(
        {
            col: _random_values(rng, source_size - len(matched), value_dtype, num_unique_strings)
            for col in target.columns
        }
    )
    if index_kind == "datetime":
        rest.index = _random_dates(rng, len(rest))
    source = pd.concat([matched, rest])
    source = source[~source.reset_index().duplicated(subset=match_cols, keep="first").values]
    if index_kind == "datetime":
        return source.sort_index()
    source.index = pd.RangeIndex(len(source))
    return source


class MergeBase:

    STRATEGIES = {
        "update": MergeStrategy(matched="update", not_matched_by_target="do_nothing"),
        "insert": MergeStrategy(matched="do_nothing", not_matched_by_target="insert"),
        "update_and_insert": MergeStrategy(matched="update", not_matched_by_target="insert"),
    }

    def __init__(self):
        self.SYM = "sym"
        self.rounds = 1
        # merge_experimental is destructive, so we must restore a fresh base and run once per measurement
        self.number = 1
        self.warmup_time = 0
        self.repeat = _env_repeat(10)
        self.timeout = 600
        self.ac = None
        self.lib = None
        self.target = None
        self.value_dtype = None
        self.source = None
        self.on = None
        self.work_bucket = None

    def lib_name(self, scenario, index_kind, *extra):
        num_rows, num_value_cols = scenario
        return "_".join([str(num_rows), str(num_value_cols), index_kind, *[str(e) for e in extra]])

    def _cache_arctic(self):
        return Arctic(arctic_uri(ARCTICDB_CACHE_BUCKET))

    def _setup_cache_base(self, ac, lib_name, target):
        # reset_arcticdb_cache_bucket() has just emptied the bucket, so the library cannot exist
        lib = ac.create_library(lib_name)
        lib.write(self.SYM, target)

    # ASV caches setup_cache results keyed by the function's source location, so every class must
    # define its own setup_cache (delegating here); inheriting one shared method would make ASV
    # reuse the first class's cache bucket for all of them.
    def _run_setup_cache(self):
        reset_arcticdb_cache_bucket()
        for bucket in list_buckets():
            if bucket.startswith(WORK_BUCKET_PREFIX):
                delete_bucket(bucket)
        self._setup_cache()

    def merge(self, strategy):
        self.lib.merge_experimental(self.SYM, self.source, strategy=self.STRATEGIES[strategy], on=self.on)

    def _select_on(self, on_count, num_value_cols):
        cols = np.random.default_rng(0).choice(num_value_cols, size=on_count, replace=False)
        return [f"val_{i}" for i in cols]


class MergeThin(MergeBase):

    def __init__(self):
        super().__init__()
        self.value_dtype = "float"
        self.param_names = ["scenario", "strategy", "index_kind", "on_count", "source_size", "matched_pct"]
        self.params = [
            [(10_000_000, 2)],  # scenario: (num_rows, num_value_cols) — long-thin
            ["update", "insert", "update_and_insert"],  # strategy
            ["datetime", "rowrange"],  # index_kind
            [0, 1],  # on_count
            [1_000, 500_000],  # source_size (source row count)
            [20, 80],  # matched_pct (percent of source rows matching an existing target row)
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        ac = self._cache_arctic()
        for scenario in self.params[0]:
            for index_kind in self.params[2]:
                num_rows, num_value_cols = scenario
                target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, index_kind)
                self._setup_cache_base(ac, self.lib_name(scenario, index_kind), target)

    def setup(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        num_rows, num_value_cols = scenario
        if index_kind == "rowrange" and strategy != "update":
            raise SkipNotImplemented  # not_matched_by_target=insert has no row-range implementation
        if index_kind == "rowrange" and on_count == 0:
            raise SkipNotImplemented  # row-range indexes cannot be a join key on their own
        lib_name = self.lib_name(scenario, index_kind)
        # Create a new Arctic instance every sample so nothing cached from the previous sample's
        # (since deleted) bucket survives
        del self.ac
        self.work_bucket = f"{WORK_BUCKET_PREFIX}{uuid.uuid4().hex[:12]}"
        create_bucket(self.work_bucket)
        # The URI-based storage override redirects the copied library config to the new bucket
        copy_bucket(
            ARCTICDB_CACHE_BUCKET,
            self.work_bucket,
            prefixes=[f"{CONFIG_LIBRARY_NAME}/", f"{_cache_library_prefix(lib_name)}/"],
        )
        self.ac = Arctic(arctic_uri(self.work_bucket))
        self.lib = self.ac.get_library(lib_name)
        # Read the symbol both to warm up the cache and to have real target rows to build the source from.
        self.target = self.lib.read(self.SYM).data
        matched_count = round(source_size * matched_pct / 100)
        self.on = None if on_count == 0 else self._select_on(on_count, num_value_cols)
        self.source = generate_merge_source(
            self.target, source_size, matched_count, on=self.on, value_dtype=self.value_dtype
        )

    def teardown(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        if self.work_bucket is not None:
            delete_bucket(self.work_bucket)
            self.work_bucket = None

    def time_merge(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        self.merge(strategy)


class MergeThinString(MergeBase):

    def __init__(self):
        super().__init__()
        self.value_dtype = "string"
        self.param_names = [
            "scenario",
            "strategy",
            "index_kind",
            "on_count",
            "source_size",
            "matched_pct",
            "num_unique_strings",
        ]
        self.params = [
            [(1_000_000, 2)],  # scenario: (num_rows, num_value_cols)
            ["update", "insert", "update_and_insert"],  # strategy
            ["datetime", "rowrange"],  # index_kind
            [0, 1],  # on_count
            [1_000, 500_000],  # source_size (source row count)
            [20, 80],  # matched_pct
            [100, 100_000],  # num_unique_strings (size of the pool each column is drawn from)
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        # Populate the base libraries only once. index_kind and num_unique_strings are shared across
        # every strategy, so each (scenario, index_kind, num_unique_strings) library is built once.
        ac = self._cache_arctic()
        for scenario in self.params[0]:
            for index_kind in self.params[2]:
                for num_unique_strings in self.params[6]:
                    num_rows, num_value_cols = scenario
                    target = generate_merge_target(
                        num_rows, num_value_cols, self.value_dtype, index_kind, num_unique_strings=num_unique_strings
                    )
                    self._setup_cache_base(ac, self.lib_name(scenario, index_kind, num_unique_strings), target)

    def setup(self, scenario, strategy, index_kind, on_count, source_size, matched_pct, num_unique_strings):
        num_rows, num_value_cols = scenario
        if index_kind == "rowrange" and strategy != "update":
            raise SkipNotImplemented  # not_matched_by_target=insert has no row-range implementation
        if index_kind == "rowrange" and on_count == 0:
            raise SkipNotImplemented  # row-range indexes cannot be a join key on their own
        lib_name = self.lib_name(scenario, index_kind, num_unique_strings)
        # Create a new Arctic instance every sample so nothing cached from the previous sample's
        # (since deleted) bucket survives
        del self.ac
        self.work_bucket = f"{WORK_BUCKET_PREFIX}{uuid.uuid4().hex[:12]}"
        create_bucket(self.work_bucket)
        # The URI-based storage override redirects the copied library config to the new bucket
        copy_bucket(
            ARCTICDB_CACHE_BUCKET,
            self.work_bucket,
            prefixes=[f"{CONFIG_LIBRARY_NAME}/", f"{_cache_library_prefix(lib_name)}/"],
        )
        self.ac = Arctic(arctic_uri(self.work_bucket))
        self.lib = self.ac.get_library(lib_name)
        # Read the symbol both to warm up the cache and to have real target rows to build the source from.
        self.target = self.lib.read(self.SYM).data
        matched_count = round(source_size * matched_pct / 100)
        self.on = None if on_count == 0 else self._select_on(on_count, num_value_cols)
        self.source = generate_merge_source(
            self.target,
            source_size,
            matched_count,
            on=self.on,
            value_dtype=self.value_dtype,
            num_unique_strings=num_unique_strings,
        )

    def teardown(self, scenario, strategy, index_kind, on_count, source_size, matched_pct, num_unique_strings):
        if self.work_bucket is not None:
            delete_bucket(self.work_bucket)
            self.work_bucket = None

    def time_merge(self, scenario, strategy, index_kind, on_count, source_size, matched_pct, num_unique_strings):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, index_kind, on_count, source_size, matched_pct, num_unique_strings):
        self.merge(strategy)


class MergeWide(MergeBase):

    def __init__(self):
        super().__init__()
        self.value_dtype = "float"
        self.param_names = ["scenario", "strategy", "index_kind", "on_count", "source_size", "matched_pct"]
        self.repeat = _env_repeat(5)
        self.params = [
            [(5_000, 10_000)],  # scenario: (num_rows, num_value_cols)
            ["update", "insert", "update_and_insert"],  # strategy
            ["datetime", "rowrange"],  # index_kind
            [0, 1, 1000],  # on_count
            [100],  # source_size
            [20, 80],  # matched_pct
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        # Populate the base libraries only once. index_kind is shared across every strategy, so each
        # (scenario, index_kind) base library is built exactly once and reused by all strategies.
        ac = self._cache_arctic()
        for scenario in self.params[0]:
            for index_kind in self.params[2]:
                num_rows, num_value_cols = scenario
                target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, index_kind)
                self._setup_cache_base(ac, self.lib_name(scenario, index_kind), target)

    def setup(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        if strategy != "update" and on_count == 1:
            raise SkipNotImplemented  # see class docstring: grid-size cost decision, not unsupported
        num_rows, num_value_cols = scenario
        if index_kind == "rowrange" and strategy != "update":
            raise SkipNotImplemented  # not_matched_by_target=insert has no row-range implementation
        if index_kind == "rowrange" and on_count == 0:
            raise SkipNotImplemented  # row-range indexes cannot be a join key on their own
        lib_name = self.lib_name(scenario, index_kind)
        # Create a new Arctic instance every sample so nothing cached from the previous sample's
        # (since deleted) bucket survives
        del self.ac
        self.work_bucket = f"{WORK_BUCKET_PREFIX}{uuid.uuid4().hex[:12]}"
        create_bucket(self.work_bucket)
        # The URI-based storage override redirects the copied library config to the new bucket
        copy_bucket(
            ARCTICDB_CACHE_BUCKET,
            self.work_bucket,
            prefixes=[f"{CONFIG_LIBRARY_NAME}/", f"{_cache_library_prefix(lib_name)}/"],
        )
        self.ac = Arctic(arctic_uri(self.work_bucket))
        self.lib = self.ac.get_library(lib_name)
        # Read the symbol both to warm up the cache and to have real target rows to build the source from.
        self.target = self.lib.read(self.SYM).data
        matched_count = round(source_size * matched_pct / 100)
        self.on = None if on_count == 0 else self._select_on(on_count, num_value_cols)
        self.source = generate_merge_source(
            self.target, source_size, matched_count, on=self.on, value_dtype=self.value_dtype
        )

    def teardown(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        if self.work_bucket is not None:
            delete_bucket(self.work_bucket)
            self.work_bucket = None

    def time_merge(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        self.merge(strategy)


class MergeLongWide(MergeBase):
    """All merge strategies, long-wide numeric dataframe (2M rows x 130 value cols)."""

    def __init__(self):
        super().__init__()
        self.repeat = _env_repeat(5)
        self.value_dtype = "float"
        self.param_names = ["scenario", "strategy", "index_kind", "on_count", "source_size", "matched_pct"]
        self.params = [
            [(2_000_000, 130)],  # scenario: (num_rows, num_value_cols)
            ["update", "insert", "update_and_insert"],  # strategy
            ["datetime", "rowrange"],  # index_kind
            [0, 1, 50],  # on_count: datetime runs {0, 50} (0 = match on index only), rowrange runs {1, 50}
            [1_000, 500_000],  # source_size (source row count)
            [20, 80],  # matched_pct
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        ac = self._cache_arctic()
        for scenario in self.params[0]:
            for index_kind in self.params[2]:
                num_rows, num_value_cols = scenario
                target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, index_kind)
                self._setup_cache_base(ac, self.lib_name(scenario, index_kind), target)

    def setup(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        num_rows, num_value_cols = scenario
        if index_kind == "rowrange" and strategy != "update":
            raise SkipNotImplemented  # not_matched_by_target=insert has no row-range implementation
        if index_kind == "rowrange" and on_count == 0:
            raise SkipNotImplemented  # row-range indexes cannot be a join key on their own
        if index_kind == "datetime" and on_count == 1:
            raise SkipNotImplemented  # grid-size cost decision: datetime measures on_count 0 and 50 only
        lib_name = self.lib_name(scenario, index_kind)
        # Create a new Arctic instance every sample so nothing cached from the previous sample's
        # (since deleted) bucket survives
        del self.ac
        self.work_bucket = f"{WORK_BUCKET_PREFIX}{uuid.uuid4().hex[:12]}"
        create_bucket(self.work_bucket)
        # The URI-based storage override redirects the copied library config to the new bucket
        copy_bucket(
            ARCTICDB_CACHE_BUCKET,
            self.work_bucket,
            prefixes=[f"{CONFIG_LIBRARY_NAME}/", f"{_cache_library_prefix(lib_name)}/"],
        )
        self.ac = Arctic(arctic_uri(self.work_bucket))
        self.lib = self.ac.get_library(lib_name)
        # Read the symbol both to warm up the cache and to have real target rows to build the source from.
        self.target = self.lib.read(self.SYM).data
        matched_count = round(source_size * matched_pct / 100)
        self.on = None if on_count == 0 else self._select_on(on_count, num_value_cols)
        self.source = generate_merge_source(
            self.target, source_size, matched_count, on=self.on, value_dtype=self.value_dtype
        )

    def teardown(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        if self.work_bucket is not None:
            delete_bucket(self.work_bucket)
            self.work_bucket = None

    def time_merge(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, index_kind, on_count, source_size, matched_pct):
        self.merge(strategy)
