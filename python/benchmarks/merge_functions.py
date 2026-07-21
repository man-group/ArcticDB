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

from arcticdb import Arctic
from arcticdb.version_store.library import MergeStrategy
from arcticdb.util.test import random_strings_of_length
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

# Matches the LibraryOptions default the base libraries are written with, so slice i of a
# datetime target covers target.iloc[i*ROWS_PER_SEGMENT:(i+1)*ROWS_PER_SEGMENT] in sorted order.
# Used to build sources that touch a controlled fraction of the target's row slices.
ROWS_PER_SEGMENT = 100_000

# merge_experimental only mutates the target (it writes a new version), so every measurement runs
# the merge against an isolated copy of the target: setup copies the target's known path_prefix
# subtree (config + data) from the cache bucket into a brand new bucket and the merge runs there;
# teardown hard-deletes that bucket by dropping its backing SeaweedFS collection. The read-only
# source is never copied — it lives under its own path_prefix in the cache bucket and is read
# directly from there.

# Short-lived per-measurement bucket; each class creates its own in setup() and deletes it in
# teardown(). The WORK_BUCKET_PREFIX sweep in _run_setup_cache() cleans up any that a killed
# benchmark process (e.g. an ASV timeout) failed to tear down
WORK_BUCKET_PREFIX = "arcticdb-merge-bench-"


def _env_repeat(default):
    # One knob to shrink local experiment runs; leaves CI defaults untouched
    return int(os.getenv("ARCTICDB_MERGE_BENCH_REPEAT", default))


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
    # Only ever called for row-range targets (datetime classes use
    # generate_merge_source_by_segments), so the index is always a RangeIndex.
    on_cols = on or []
    rng = np.random.default_rng(1)
    picks = rng.choice(len(target), size=matched_count, replace=False)
    matched = target.iloc[picks]
    matched = matched[~matched.reset_index().duplicated(subset=on_cols, keep="first").values].copy()
    for col in target.columns:
        if col not in on_cols:
            matched[col] = _random_values(rng, len(matched), value_dtype, num_unique_strings)

    rest = pd.DataFrame(
        {
            col: _random_values(rng, source_size - len(matched), value_dtype, num_unique_strings)
            for col in target.columns
        }
    )
    source = pd.concat([matched, rest])
    source = source[~source.reset_index().duplicated(subset=on_cols, keep="first").values]
    source.index = pd.RangeIndex(len(source))
    return source


def generate_merge_source_by_segments(
    target,
    source_size,
    segment_pct,
    rows_per_segment=ROWS_PER_SEGMENT,
    on=None,
    value_dtype="float",
    num_unique_strings=None,
):
    """Datetime source whose rows all fall within the first k row-slices of ``target``, where
    ``k = round(num_segments * segment_pct / 100)``. Since the datetime merge only reads/rewrites
    the slices whose time range overlaps the source, this makes the merge touch exactly k of the
    target's segments. Half the rows match existing target rows (drive the update path) and half
    are new datetimes inside the same span (drive the insert path), so one source serves every
    strategy while touching the same k segments."""
    on_cols = on or []
    match_cols = on_cols + ["index"]
    num_segments = -(-len(target) // rows_per_segment)
    k = max(1, round(num_segments * segment_pct / 100))
    span = min(k * rows_per_segment, len(target))
    rng = np.random.default_rng(1)

    # Matched rows can only come from the span's existing rows, so cap by span (a small span with a
    # large source_size just means more of the source is new/inserted rows within that time range).
    matched_count = min(source_size // 2, span)
    picks = rng.choice(span, size=matched_count, replace=False)
    matched = target.iloc[picks]
    matched = matched[~matched.reset_index().duplicated(subset=match_cols, keep="first").values].copy()
    for col in target.columns:
        if col not in on_cols:
            matched[col] = _random_values(rng, len(matched), value_dtype, num_unique_strings)

    rest_n = source_size - len(matched)
    rest = pd.DataFrame({col: _random_values(rng, rest_n, value_dtype, num_unique_strings) for col in target.columns})
    lo, hi = target.index[0].value, target.index[span - 1].value
    rest.index = pd.to_datetime(rng.integers(lo, hi + 1, size=rest_n), unit="ns")
    source = pd.concat([matched, rest])
    source = source[~source.reset_index().duplicated(subset=match_cols, keep="first").values]
    return source.sort_index()


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
        self.value_dtype = None
        self.source = None
        self.on = None
        self.work_bucket = None
        # Per-lib_name handles to the source libraries in the (persistent) cache bucket
        self._src_libs = {}

    def lib_name(self, scenario, index_kind, *extra):
        num_rows, num_value_cols = scenario
        return "_".join([str(num_rows), str(num_value_cols), index_kind, *[str(e) for e in extra]])

    # Target and source each get their own known path_prefix so the target subtree can be copied
    # by a literal prefix (no need to infer the "<name><nanos>" leaf) and the source subtree is
    # simply never copied. The inner library name is constant — the scenario lives in the prefix.
    TARGET_PREFIX = "tgt"
    SOURCE_PREFIX = "src"
    INNER_LIB = "data"
    SRC_SYM_PREFIX = "src_"

    def _cache_uri(self, path_prefix):
        return arctic_uri(ARCTICDB_CACHE_BUCKET) + f"&path_prefix={path_prefix}"

    def _target_path_prefix(self, lib_name):
        return f"{self.TARGET_PREFIX}/{lib_name}"

    def _source_path_prefix(self, lib_name):
        return f"{self.SOURCE_PREFIX}/{lib_name}"

    def _setup_cache_base(self, lib_name, target):
        # reset_arcticdb_cache_bucket() has just emptied the bucket, so the libraries cannot exist.
        # Target and source go under separate path_prefixes; the source library is returned so the
        # caller can populate it via _precompute_sources.
        tgt_lib = Arctic(self._cache_uri(self._target_path_prefix(lib_name))).create_library(self.INNER_LIB)
        tgt_lib.write(self.SYM, target)
        return Arctic(self._cache_uri(self._source_path_prefix(lib_name))).create_library(self.INNER_LIB)

    def _source_lib(self, lib_name):
        # The cache bucket is persistent and never mutated by the merge, so one read handle per
        # lib_name is reused across samples.
        if lib_name not in self._src_libs:
            self._src_libs[lib_name] = Arctic(self._cache_uri(self._source_path_prefix(lib_name))).get_library(
                self.INNER_LIB
            )
        return self._src_libs[lib_name]

    def _source_sym(self, index_kind, on_count, source_size, matched_pct=None, num_unique_strings=None):
        # matched_pct is part of the key only for the datetime classes that vary it (a segment %);
        # rowrange and MergeWideDatetime omit it, matching what their setup() passes.
        parts = [index_kind, on_count, source_size]
        if matched_pct is not None:
            parts.append(matched_pct)
        if num_unique_strings is not None:
            parts.append(num_unique_strings)
        return self.SRC_SYM_PREFIX + "_".join(str(p) for p in parts)

    def _precompute_sources(self, src_lib, target, num_value_cols, num_unique_strings=None):
        # The source depends only on (index_kind, on_count, source_size, matched_pct[, num_unique_strings]),
        # never on strategy, so each is generated exactly once here instead of once per (strategy x repeat)
        # sample. setup() then only has to read back the (small) source symbol. Read from param_names by
        # name (positions differ between the datetime and rowrange classes).
        pmap = dict(zip(self.param_names, self.params))
        for on_count in pmap["on_count"]:
            on = None if on_count == 0 else self._select_on(on_count, num_value_cols)
            for source_size in pmap["source_size"]:
                if self.INDEX_KIND == "datetime":
                    # Wide has a single 100k slice, so it drops the segment axis and uses one
                    # full-range (segment_pct=100) source per (on_count, source_size).
                    segment_pcts = pmap["matched_pct"] if "matched_pct" in pmap else [100]
                    for segment_pct in segment_pcts:
                        source = generate_merge_source_by_segments(
                            target,
                            source_size,
                            segment_pct,
                            ROWS_PER_SEGMENT,
                            on=on,
                            value_dtype=self.value_dtype,
                            num_unique_strings=num_unique_strings,
                        )
                        key_pct = segment_pct if "matched_pct" in pmap else None
                        src_lib.write(
                            self._source_sym(self.INDEX_KIND, on_count, source_size, key_pct, num_unique_strings),
                            source,
                        )
                else:
                    # rowrange: matched_pct dropped; hardcode 50% of source rows matching an existing row.
                    source = generate_merge_source(
                        target,
                        source_size,
                        source_size // 2,
                        on=on,
                        value_dtype=self.value_dtype,
                        num_unique_strings=num_unique_strings,
                    )
                    src_lib.write(
                        self._source_sym(self.INDEX_KIND, on_count, source_size, None, num_unique_strings),
                        source,
                    )

    def _prepare_merge(self, lib_name, on_count, num_value_cols, source_sym):
        # Shared body of every class's setup(): isolate the target in a fresh work bucket (copying
        # only its path_prefix subtree) and load the precomputed source directly from the cache
        # bucket. Concrete helper (not overridden) so the 8 classes don't each duplicate it.
        target_prefix = self._target_path_prefix(lib_name)
        # Create a new Arctic instance every sample so nothing cached from the previous sample's
        # (since deleted) bucket survives
        del self.ac
        self.work_bucket = f"{WORK_BUCKET_PREFIX}{uuid.uuid4().hex[:12]}"
        create_bucket(self.work_bucket)
        # The URI-based storage override redirects the copied library config to the new bucket.
        copy_bucket(ARCTICDB_CACHE_BUCKET, self.work_bucket, prefixes=[f"{target_prefix}/"])
        self.ac = Arctic(arctic_uri(self.work_bucket) + f"&path_prefix={target_prefix}")
        self.lib = self.ac.get_library(self.INNER_LIB)
        self.on = None if on_count == 0 else self._select_on(on_count, num_value_cols)
        self.source = self._source_lib(lib_name).read(source_sym).data

    def _teardown(self):
        if self.work_bucket is not None:
            delete_bucket(self.work_bucket)
            self.work_bucket = None

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


class MergeThinDatetime(MergeBase):
    """All merge strategies, long-thin numeric dataframe (10M rows x 2 cols), datetime index.
    matched_pct is the percent of the target's row slices the source touches (100 slices here)."""

    INDEX_KIND = "datetime"

    def __init__(self):
        super().__init__()
        self.value_dtype = "float"
        self.param_names = ["scenario", "strategy", "on_count", "source_size", "matched_pct"]
        self.params = [
            [(10_000_000, 2)],  # scenario: (num_rows, num_value_cols) — long-thin
            ["update", "insert", "update_and_insert"],  # strategy
            [0, 1],  # on_count
            [1_000, 500_000],  # source_size (source row count)
            [20, 80],  # matched_pct (percent of the target's row slices the source touches)
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND)
            src_lib = self._setup_cache_base(self.lib_name(scenario, self.INDEX_KIND), target)
            self._precompute_sources(src_lib, target, num_value_cols)

    def setup(self, scenario, strategy, on_count, source_size, matched_pct):
        num_rows, num_value_cols = scenario
        lib_name = self.lib_name(scenario, self.INDEX_KIND)
        source_sym = self._source_sym(self.INDEX_KIND, on_count, source_size, matched_pct)
        self._prepare_merge(lib_name, on_count, num_value_cols, source_sym)

    def teardown(self, scenario, strategy, on_count, source_size, matched_pct):
        self._teardown()

    def time_merge(self, scenario, strategy, on_count, source_size, matched_pct):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size, matched_pct):
        self.merge(strategy)


class MergeThinRowRange(MergeBase):
    """update only, long-thin numeric dataframe (10M rows x 2 cols), row-range index.
    matched_pct is dropped: row-range merge reads every slice and has no insert path."""

    INDEX_KIND = "rowrange"

    def __init__(self):
        super().__init__()
        self.value_dtype = "float"
        self.param_names = ["scenario", "strategy", "on_count", "source_size"]
        self.params = [
            [(10_000_000, 2)],  # scenario: (num_rows, num_value_cols) — long-thin
            ["update"],  # strategy: only update is implemented for row-range indexes
            [1],  # on_count: row-range indexes cannot be a join key on their own, so on_count >= 1
            [1_000, 500_000],  # source_size (source row count)
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND)
            src_lib = self._setup_cache_base(self.lib_name(scenario, self.INDEX_KIND), target)
            self._precompute_sources(src_lib, target, num_value_cols)

    def setup(self, scenario, strategy, on_count, source_size):
        num_rows, num_value_cols = scenario
        lib_name = self.lib_name(scenario, self.INDEX_KIND)
        source_sym = self._source_sym(self.INDEX_KIND, on_count, source_size)
        self._prepare_merge(lib_name, on_count, num_value_cols, source_sym)

    def teardown(self, scenario, strategy, on_count, source_size):
        self._teardown()

    def time_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)


class MergeThinStringDatetime(MergeBase):
    """All merge strategies, long-thin string dataframe (1M rows x 2 cols), datetime index (10 slices)."""

    INDEX_KIND = "datetime"

    def __init__(self):
        super().__init__()
        self.value_dtype = "string"
        self.param_names = ["scenario", "strategy", "on_count", "source_size", "matched_pct", "num_unique_strings"]
        self.params = [
            [(1_000_000, 2)],  # scenario: (num_rows, num_value_cols)
            ["update", "insert", "update_and_insert"],  # strategy
            [0, 1],  # on_count
            [1_000, 500_000],  # source_size (source row count)
            [20, 80],  # matched_pct (percent of the target's row slices the source touches)
            [100, 100_000],  # num_unique_strings (size of the pool each column is drawn from)
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        # index_kind and num_unique_strings are shared across every strategy, so each
        # (scenario, num_unique_strings) library is built once.
        for scenario in self.params[0]:
            for num_unique_strings in self.params[5]:
                num_rows, num_value_cols = scenario
                target = generate_merge_target(
                    num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND, num_unique_strings=num_unique_strings
                )
                src_lib = self._setup_cache_base(self.lib_name(scenario, self.INDEX_KIND, num_unique_strings), target)
                self._precompute_sources(src_lib, target, num_value_cols, num_unique_strings=num_unique_strings)

    def setup(self, scenario, strategy, on_count, source_size, matched_pct, num_unique_strings):
        num_rows, num_value_cols = scenario
        lib_name = self.lib_name(scenario, self.INDEX_KIND, num_unique_strings)
        source_sym = self._source_sym(self.INDEX_KIND, on_count, source_size, matched_pct, num_unique_strings)
        self._prepare_merge(lib_name, on_count, num_value_cols, source_sym)

    def teardown(self, scenario, strategy, on_count, source_size, matched_pct, num_unique_strings):
        self._teardown()

    def time_merge(self, scenario, strategy, on_count, source_size, matched_pct, num_unique_strings):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size, matched_pct, num_unique_strings):
        self.merge(strategy)


class MergeThinStringRowRange(MergeBase):
    """update only, long-thin string dataframe (1M rows x 2 cols), row-range index (matched_pct dropped)."""

    INDEX_KIND = "rowrange"

    def __init__(self):
        super().__init__()
        self.value_dtype = "string"
        self.param_names = ["scenario", "strategy", "on_count", "source_size", "num_unique_strings"]
        self.params = [
            [(1_000_000, 2)],  # scenario: (num_rows, num_value_cols)
            ["update"],  # strategy: only update is implemented for row-range indexes
            [1],  # on_count: row-range indexes cannot be a join key on their own, so on_count >= 1
            [1_000, 500_000],  # source_size (source row count)
            [100, 100_000],  # num_unique_strings (size of the pool each column is drawn from)
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        for scenario in self.params[0]:
            for num_unique_strings in self.params[4]:
                num_rows, num_value_cols = scenario
                target = generate_merge_target(
                    num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND, num_unique_strings=num_unique_strings
                )
                src_lib = self._setup_cache_base(self.lib_name(scenario, self.INDEX_KIND, num_unique_strings), target)
                self._precompute_sources(src_lib, target, num_value_cols, num_unique_strings=num_unique_strings)

    def setup(self, scenario, strategy, on_count, source_size, num_unique_strings):
        num_rows, num_value_cols = scenario
        lib_name = self.lib_name(scenario, self.INDEX_KIND, num_unique_strings)
        source_sym = self._source_sym(self.INDEX_KIND, on_count, source_size, num_unique_strings=num_unique_strings)
        self._prepare_merge(lib_name, on_count, num_value_cols, source_sym)

    def teardown(self, scenario, strategy, on_count, source_size, num_unique_strings):
        self._teardown()

    def time_merge(self, scenario, strategy, on_count, source_size, num_unique_strings):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size, num_unique_strings):
        self.merge(strategy)


class MergeWideDatetime(MergeBase):
    """All merge strategies, wide numeric dataframe (5k rows x 10k cols), datetime index.
    5k rows = a single 100k row slice, so the segment (matched_pct) axis does not apply here."""

    INDEX_KIND = "datetime"

    def __init__(self):
        super().__init__()
        self.value_dtype = "float"
        self.param_names = ["scenario", "strategy", "on_count", "source_size"]
        self.repeat = _env_repeat(5)
        self.params = [
            [(5_000, 10_000)],  # scenario: (num_rows, num_value_cols)
            ["update", "insert", "update_and_insert"],  # strategy
            [0, 1, 1000],  # on_count
            [100],  # source_size
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND)
            src_lib = self._setup_cache_base(self.lib_name(scenario, self.INDEX_KIND), target)
            self._precompute_sources(src_lib, target, num_value_cols)

    def setup(self, scenario, strategy, on_count, source_size):
        if strategy != "update" and on_count == 1:
            raise SkipNotImplemented  # grid-size cost decision, not unsupported
        num_rows, num_value_cols = scenario
        lib_name = self.lib_name(scenario, self.INDEX_KIND)
        source_sym = self._source_sym(self.INDEX_KIND, on_count, source_size)
        self._prepare_merge(lib_name, on_count, num_value_cols, source_sym)

    def teardown(self, scenario, strategy, on_count, source_size):
        self._teardown()

    def time_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)


class MergeWideRowRange(MergeBase):
    """update only, wide numeric dataframe (5k rows x 10k cols), row-range index (matched_pct dropped)."""

    INDEX_KIND = "rowrange"

    def __init__(self):
        super().__init__()
        self.value_dtype = "float"
        self.param_names = ["scenario", "strategy", "on_count", "source_size"]
        self.repeat = _env_repeat(5)
        self.params = [
            [(5_000, 10_000)],  # scenario: (num_rows, num_value_cols)
            ["update"],  # strategy: only update is implemented for row-range indexes
            [1, 1000],  # on_count: row-range indexes cannot be a join key on their own, so on_count >= 1
            [100],  # source_size
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND)
            src_lib = self._setup_cache_base(self.lib_name(scenario, self.INDEX_KIND), target)
            self._precompute_sources(src_lib, target, num_value_cols)

    def setup(self, scenario, strategy, on_count, source_size):
        num_rows, num_value_cols = scenario
        lib_name = self.lib_name(scenario, self.INDEX_KIND)
        source_sym = self._source_sym(self.INDEX_KIND, on_count, source_size)
        self._prepare_merge(lib_name, on_count, num_value_cols, source_sym)

    def teardown(self, scenario, strategy, on_count, source_size):
        self._teardown()

    def time_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)


class MergeLongWideDatetime(MergeBase):
    """All merge strategies, long-wide numeric dataframe (2M rows x 130 value cols), datetime
    index (20 slices). datetime measures on_count 0 and 50 only (grid-size cost decision)."""

    INDEX_KIND = "datetime"

    def __init__(self):
        super().__init__()
        self.repeat = _env_repeat(5)
        self.value_dtype = "float"
        self.param_names = ["scenario", "strategy", "on_count", "source_size", "matched_pct"]
        self.params = [
            [(2_000_000, 130)],  # scenario: (num_rows, num_value_cols)
            ["update", "insert", "update_and_insert"],  # strategy
            [0, 50],  # on_count: 0 = match on index only
            [1_000, 500_000],  # source_size (source row count)
            [20, 80],  # matched_pct (percent of the target's row slices the source touches)
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND)
            src_lib = self._setup_cache_base(self.lib_name(scenario, self.INDEX_KIND), target)
            self._precompute_sources(src_lib, target, num_value_cols)

    def setup(self, scenario, strategy, on_count, source_size, matched_pct):
        num_rows, num_value_cols = scenario
        lib_name = self.lib_name(scenario, self.INDEX_KIND)
        source_sym = self._source_sym(self.INDEX_KIND, on_count, source_size, matched_pct)
        self._prepare_merge(lib_name, on_count, num_value_cols, source_sym)

    def teardown(self, scenario, strategy, on_count, source_size, matched_pct):
        self._teardown()

    def time_merge(self, scenario, strategy, on_count, source_size, matched_pct):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size, matched_pct):
        self.merge(strategy)


class MergeLongWideRowRange(MergeBase):
    """update only, long-wide numeric dataframe (2M rows x 130 value cols), row-range index
    (matched_pct dropped)."""

    INDEX_KIND = "rowrange"

    def __init__(self):
        super().__init__()
        self.repeat = _env_repeat(5)
        self.value_dtype = "float"
        self.param_names = ["scenario", "strategy", "on_count", "source_size"]
        self.params = [
            [(2_000_000, 130)],  # scenario: (num_rows, num_value_cols)
            ["update"],  # strategy: only update is implemented for row-range indexes
            [1, 50],  # on_count: row-range indexes cannot be a join key on their own, so on_count >= 1
            [1_000, 500_000],  # source_size (source row count)
        ]

    def setup_cache(self):
        self._run_setup_cache()

    def _setup_cache(self):
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND)
            src_lib = self._setup_cache_base(self.lib_name(scenario, self.INDEX_KIND), target)
            self._precompute_sources(src_lib, target, num_value_cols)

    def setup(self, scenario, strategy, on_count, source_size):
        num_rows, num_value_cols = scenario
        lib_name = self.lib_name(scenario, self.INDEX_KIND)
        source_sym = self._source_sym(self.INDEX_KIND, on_count, source_size)
        self._prepare_merge(lib_name, on_count, num_value_cols, source_sym)

    def teardown(self, scenario, strategy, on_count, source_size):
        self._teardown()

    def time_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)
