"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random

import numpy as np
import pandas as pd

from arcticdb import Arctic
from arcticdb.version_store.library import MergeStrategy
from arcticdb.util.test import random_strings_of_length
from asv_runner.benchmarks.mark import SkipNotImplemented

from benchmarks.seaweed_utils import SeaweedClient

# The cache bucket holds every scenario's precomputed target libraries; each measurement
# copies its target prefix into the throwaway work bucket.
CACHE_BUCKET = "arcticdb-merge-update-cache"
WORK_BUCKET = "arcticdb-merge-work"
# Row-slice size the target is chunked into; the datetime source generator touches a number of these.
ROWS_PER_SEGMENT = 100_000

START_DATE = pd.Timestamp("1960-01-01")
random.seed(0)


def generate_merge_target(num_rows, num_value_cols, value_dtype, index_kind, rng, num_unique_strings=None):
    if pd.api.types.is_string_dtype(value_dtype):
        assert num_unique_strings is not None
        strings = random_strings_of_length(num_unique_strings, length=10, unique=True, kind="ascii")
        df = pd.DataFrame({f"val_{j}": rng.choice(strings, num_rows) for j in range(num_value_cols)})
    elif pd.api.types.is_float_dtype(value_dtype):
        assert num_unique_strings is None
        df = pd.DataFrame({f"val_{j}": rng.random(num_rows, dtype=value_dtype) for j in range(num_value_cols)})
    else:
        raise ValueError(f"Unsupported value_dtype: {value_dtype}")
    if index_kind == "datetime":
        df.index = pd.date_range(start=START_DATE, periods=num_rows, freq="s")
    elif index_kind == "rowrange":
        df.index = pd.RangeIndex(len(df))
    return df


def get_random_unique_rows(target, count, rng, unique_on=None):
    """
    Picks `count` amount of rows from `target` deduplicates them so that they don't match on the `on` columns and
    returns them as a dataframe.
    """
    on_cols = unique_on or []
    picks = rng.choice(len(target), size=count, replace=False)
    matched = target.iloc[picks]
    # With no join columns (on_count == 0) there is nothing to deduplicate on, so keep the picks as-is
    # (pandas rejects an empty duplicated() subset).
    if not on_cols:
        return matched.copy()
    return matched[~matched.duplicated(subset=on_cols, keep="first").values].copy()


def generate_source_for_row_range(target, source_size, matched_count, rng, on=None, value_dtype=None):
    """
    Create a dataframe with RowRange index where ~`matched_count` amount of rows match at least one row in `target` on
    all columns in `on`. The columns that are not in `on` get randomised values.
    """
    on = on if on else []
    assert len(on) > 0
    matched = get_random_unique_rows(target, matched_count, rng, on)
    is_string = pd.api.types.is_string_dtype(value_dtype)
    string_pool = pd.unique(target.select_dtypes(include="object").values.ravel()) if is_string else None
    for col in target.columns:
        if col not in on:
            matched[col] = (
                rng.choice(string_pool, len(matched)) if is_string else rng.random(len(matched), dtype=value_dtype)
            )
    rest = pd.DataFrame(
        {
            col: (
                rng.choice(string_pool, source_size - len(matched))
                if is_string
                else rng.random(source_size - len(matched), dtype=value_dtype)
            )
            for col in target.columns
        }
    )
    source = pd.concat([matched, rest])
    source = source[~source.duplicated(subset=on, keep="first").values]
    source.index = pd.RangeIndex(len(source))
    return source.sample(frac=1, random_state=42).reset_index(drop=True)


def generate_source_for_datetime(
    target, source_size, rng, affected_row_slice_idx=None, rows_per_segment=100_000, on=None, value_dtype="float32"
):
    """
    Spread `source_size` rows over the affected target row slices: half of each slice's share matches target rows
    on the index + `on` columns (capped at half the slice), the rest is inserted at fresh in-slice timestamps.
    """
    affected_row_slice_idx = affected_row_slice_idx if affected_row_slice_idx else []
    assert len(affected_row_slice_idx) > 0

    affected_row_slices = []
    for slice_idx in sorted(affected_row_slice_idx):
        start = slice_idx * rows_per_segment
        end = min(start + rows_per_segment, len(target))
        affected_row_slices.append(target[start:end])

    source_rows_per_affected_segment = source_size // len(affected_row_slices)

    on = on if on else []
    is_string = pd.api.types.is_string_dtype(value_dtype)
    string_pool = pd.unique(target.select_dtypes(include="object").values.ravel()) if is_string else None

    source_segments = []
    for row_slice in affected_row_slices:
        # cap matches at half the slice; overflow becomes inserts
        matched_count = min(source_rows_per_affected_segment // 2, len(row_slice) // 2)
        inserted_count = source_rows_per_affected_segment - matched_count
        matched = get_random_unique_rows(row_slice, matched_count, rng, on)
        for col in target.columns:
            if col not in on:
                matched[col] = (
                    rng.choice(string_pool, len(matched)) if is_string else rng.random(len(matched), dtype=value_dtype)
                )
        inserted = pd.DataFrame(
            {
                col: (
                    rng.choice(string_pool, inserted_count)
                    if is_string
                    else rng.random(inserted_count, dtype=value_dtype)
                )
                for col in target.columns
            }
        )
        first_index, last_index = row_slice.index[0], row_slice.index[-1]
        inserted.index = pd.to_datetime(
            rng.integers(first_index.value, last_index.value, size=inserted_count), unit="ns"
        )
        source_segments.append(matched)
        source_segments.append(inserted)

    source = pd.concat(source_segments)
    source_reset = source.reset_index()
    index_col = source_reset.columns[0]
    source = source[~source_reset.duplicated(subset=[index_col] + on, keep="first").values]
    return source.sort_index()


def make_lib_name(scenario, index_kind, *extra):
    num_rows, num_value_cols = scenario
    return "_".join([str(num_rows), str(num_value_cols), index_kind, *[str(e) for e in extra]])


def affected_slices(num_rows_in_target, matched_slices_count):
    num_slices = max(1, (num_rows_in_target // ROWS_PER_SEGMENT))
    count = max(1, min(matched_slices_count, num_slices))
    return np.linspace(0, num_slices - 1, count, dtype=int).tolist()


# ASV keys setup_cache results by source location, so each class defines its own setup_cache.
def reset_cache_bucket(seaweed):
    seaweed.delete_bucket(CACHE_BUCKET)
    seaweed.create_bucket(CACHE_BUCKET)


class MergeBase:

    STRATEGIES = {
        "update": MergeStrategy(matched="update", not_matched_by_target="do_nothing"),
        "insert": MergeStrategy(matched="do_nothing", not_matched_by_target="insert"),
        "update_and_insert": MergeStrategy(matched="update", not_matched_by_target="insert"),
    }

    def __init__(self):
        self.rounds = 1
        # merge_experimental is destructive, so we must restore a fresh base and run once per measurement
        self.number = 1
        self.warmup_time = 0
        self.repeat = 10
        self.timeout = 600
        self.ac = None
        self.lib = None
        self.value_dtype = None
        self.source = None
        self.on = None
        # setup copies the target prefix into the work bucket by literal name.
        self.target_prefix = "target"
        # Each class owns its client. ASV runs setup_cache, setup and teardown on separate instances
        # (only setup_cache's return value is shared between them), so the client is created here in
        # __init__ — the one place every phase can reach it — rather than in setup_cache.
        self.seaweed = SeaweedClient()

    SYM = "sym"

    def _write_cache_target(self, lib_name, target):
        uri = self.seaweed.arctic_uri(CACHE_BUCKET, self.target_prefix)
        Arctic(uri).create_library(lib_name).write(self.SYM, target)

    def _generate_source(self, source_size, matched_slices=None):
        # Params-seeded rng: every setup call for a given combo generates the identical source.
        rng = np.random.default_rng([source_size, matched_slices or 0])
        target = self.lib.read(self.SYM).data
        # Maximal on-set: every on_count (incl. 0) merges the same source; generators need a non-empty on.
        on = [f"val_{j}" for j in range(max(dict(zip(self.param_names, self.params))["on_count"]))]
        if self.INDEX_KIND == "datetime":
            slices = matched_slices or max(1, len(target) // ROWS_PER_SEGMENT)
            return generate_source_for_datetime(
                target,
                source_size,
                rng,
                affected_row_slice_idx=affected_slices(len(target), slices),
                rows_per_segment=ROWS_PER_SEGMENT,
                on=on,
                value_dtype=self.value_dtype,
            )
        return generate_source_for_row_range(
            target, source_size, source_size // 2, rng, on=on, value_dtype=self.value_dtype
        )

    def _prepare_merge(self, lib_name, on_count, source_size, matched_slices=None):
        # Fresh Arctic per sample so nothing cached from the previous sample's bucket survives.
        del self.ac
        self.seaweed.delete_bucket(WORK_BUCKET)  # tolerant: reclaims a bucket leaked by a crashed run
        self.seaweed.create_bucket(WORK_BUCKET)
        # The URI-based storage override redirects the copied library config to the new bucket.
        self.seaweed.copy_bucket(CACHE_BUCKET, WORK_BUCKET, prefixes=[f"{self.target_prefix}/"])
        self.ac = Arctic(self.seaweed.arctic_uri(WORK_BUCKET, self.target_prefix))
        self.lib = self.ac.get_library(lib_name)
        self.on = [f"val_{j}" for j in range(on_count)]
        self.source = self._generate_source(source_size, matched_slices)

    def merge(self, strategy):
        self.lib.merge_experimental(self.SYM, self.source, strategy=self.STRATEGIES[strategy], on=self.on)


class MergeThinDatetime(MergeBase):
    """All merge strategies, long-thin numeric dataframe (10M rows x 2 cols), datetime index.
    matched_slices is the number of the target's row slices the source touches (100 slices here)."""

    INDEX_KIND = "datetime"

    def __init__(self):
        super().__init__()
        self.value_dtype = "float32"
        self.param_names = ["scenario", "strategy", "on_count", "source_size", "matched_slices"]
        self.params = [
            [(10_000_000, 2)],  # scenario: (num_rows, num_value_cols) — long-thin
            ["update", "insert", "update_and_insert"],  # strategy
            [0, 1],  # on_count
            [1_000, 500_000],  # source_size (source row count)
            [20, 80],  # matched_slices (number of the target's 100 row slices the source touches)
        ]

    def setup_cache(self):
        reset_cache_bucket(self.seaweed)
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            rng = np.random.default_rng([num_rows, num_value_cols])
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND, rng)
            self._write_cache_target(make_lib_name(scenario, self.INDEX_KIND), target)

    def setup(self, scenario, strategy, on_count, source_size, matched_slices):
        self._prepare_merge(make_lib_name(scenario, self.INDEX_KIND), on_count, source_size, matched_slices)

    def teardown(self, scenario, strategy, on_count, source_size, matched_slices):
        self.seaweed.delete_bucket(WORK_BUCKET)

    def time_merge(self, scenario, strategy, on_count, source_size, matched_slices):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size, matched_slices):
        self.merge(strategy)


class MergeThinRowRange(MergeBase):
    """update only, long-thin numeric dataframe (10M rows x 2 cols), row-range index.
    matched_slices is dropped: row-range merge reads every slice and has no insert path."""

    INDEX_KIND = "rowrange"

    def __init__(self):
        super().__init__()
        self.value_dtype = "float32"
        self.param_names = ["scenario", "strategy", "on_count", "source_size"]
        self.params = [
            [(10_000_000, 2)],  # scenario: (num_rows, num_value_cols) — long-thin
            ["update"],  # strategy: only update is implemented for row-range indexes
            [1],  # on_count: row-range indexes cannot be a join key on their own, so on_count >= 1
            [1_000, 500_000],  # source_size (source row count)
        ]

    def setup_cache(self):
        reset_cache_bucket(self.seaweed)
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            rng = np.random.default_rng([num_rows, num_value_cols])
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND, rng)
            self._write_cache_target(make_lib_name(scenario, self.INDEX_KIND), target)

    def setup(self, scenario, strategy, on_count, source_size):
        self._prepare_merge(make_lib_name(scenario, self.INDEX_KIND), on_count, source_size)

    def teardown(self, scenario, strategy, on_count, source_size):
        self.seaweed.delete_bucket(WORK_BUCKET)

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
        self.param_names = ["scenario", "strategy", "on_count", "source_size", "matched_slices", "num_unique_strings"]
        self.params = [
            [(1_000_000, 2)],  # scenario: (num_rows, num_value_cols)
            ["update", "insert", "update_and_insert"],  # strategy
            [0, 1],  # on_count
            [1_000, 500_000],  # source_size (source row count)
            [2, 8],  # matched_slices (number of the target's 10 row slices the source touches)
            [100, 100_000],  # num_unique_strings (size of the pool each column is drawn from)
        ]

    def setup_cache(self):
        reset_cache_bucket(self.seaweed)
        pmap = dict(zip(self.param_names, self.params))
        for scenario in pmap["scenario"]:
            for num_unique_strings in pmap["num_unique_strings"]:
                num_rows, num_value_cols = scenario
                rng = np.random.default_rng([num_rows, num_value_cols, num_unique_strings])
                target = generate_merge_target(
                    num_rows,
                    num_value_cols,
                    self.value_dtype,
                    self.INDEX_KIND,
                    rng,
                    num_unique_strings=num_unique_strings,
                )
                # One target per pool size; per-size prefixes let setup copy only the variant being merged.
                self.target_prefix = f"target_{num_unique_strings}"
                self._write_cache_target(make_lib_name(scenario, self.INDEX_KIND), target)

    def setup(self, scenario, strategy, on_count, source_size, matched_slices, num_unique_strings):
        self.target_prefix = f"target_{num_unique_strings}"
        self._prepare_merge(make_lib_name(scenario, self.INDEX_KIND), on_count, source_size, matched_slices)

    def teardown(self, scenario, strategy, on_count, source_size, matched_slices, num_unique_strings):
        self.seaweed.delete_bucket(WORK_BUCKET)

    def time_merge(self, scenario, strategy, on_count, source_size, matched_slices, num_unique_strings):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size, matched_slices, num_unique_strings):
        self.merge(strategy)


class MergeThinStringRowRange(MergeBase):
    """update only, long-thin string dataframe (1M rows x 2 cols), row-range index (matched_slices dropped)."""

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
        reset_cache_bucket(self.seaweed)
        pmap = dict(zip(self.param_names, self.params))
        for scenario in pmap["scenario"]:
            for num_unique_strings in pmap["num_unique_strings"]:
                num_rows, num_value_cols = scenario
                rng = np.random.default_rng([num_rows, num_value_cols, num_unique_strings])
                target = generate_merge_target(
                    num_rows,
                    num_value_cols,
                    self.value_dtype,
                    self.INDEX_KIND,
                    rng,
                    num_unique_strings=num_unique_strings,
                )
                # One target per pool size; per-size prefixes let setup copy only the variant being merged.
                self.target_prefix = f"target_{num_unique_strings}"
                self._write_cache_target(make_lib_name(scenario, self.INDEX_KIND), target)

    def setup(self, scenario, strategy, on_count, source_size, num_unique_strings):
        self.target_prefix = f"target_{num_unique_strings}"
        self._prepare_merge(make_lib_name(scenario, self.INDEX_KIND), on_count, source_size)

    def teardown(self, scenario, strategy, on_count, source_size, num_unique_strings):
        self.seaweed.delete_bucket(WORK_BUCKET)

    def time_merge(self, scenario, strategy, on_count, source_size, num_unique_strings):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size, num_unique_strings):
        self.merge(strategy)


class MergeWideDatetime(MergeBase):
    """All merge strategies, wide numeric dataframe (5k rows x 10k cols), datetime index.
    5k rows = a single 100k row slice, so the segment (matched_slices) axis does not apply here."""

    INDEX_KIND = "datetime"

    def __init__(self):
        super().__init__()
        self.value_dtype = "float32"
        self.param_names = ["scenario", "strategy", "on_count", "source_size"]
        self.repeat = 5
        self.params = [
            [(5_000, 10_000)],  # scenario: (num_rows, num_value_cols)
            ["update", "insert", "update_and_insert"],  # strategy
            [0, 1, 1000],  # on_count
            [100],  # source_size
        ]

    def setup_cache(self):
        reset_cache_bucket(self.seaweed)
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            rng = np.random.default_rng([num_rows, num_value_cols])
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND, rng)
            self._write_cache_target(make_lib_name(scenario, self.INDEX_KIND), target)

    def setup(self, scenario, strategy, on_count, source_size):
        if strategy != "update" and on_count == 1:
            raise SkipNotImplemented  # grid-size cost decision, not unsupported
        self._prepare_merge(make_lib_name(scenario, self.INDEX_KIND), on_count, source_size)

    def teardown(self, scenario, strategy, on_count, source_size):
        self.seaweed.delete_bucket(WORK_BUCKET)

    def time_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)


class MergeWideRowRange(MergeBase):
    """update only, wide numeric dataframe (5k rows x 10k cols), row-range index (matched_slices dropped)."""

    INDEX_KIND = "rowrange"

    def __init__(self):
        super().__init__()
        self.value_dtype = "float32"
        self.param_names = ["scenario", "strategy", "on_count", "source_size"]
        self.repeat = 5
        self.params = [
            [(5_000, 10_000)],  # scenario: (num_rows, num_value_cols)
            ["update"],  # strategy: only update is implemented for row-range indexes
            [1, 1000],  # on_count: row-range indexes cannot be a join key on their own, so on_count >= 1
            [100],  # source_size
        ]

    def setup_cache(self):
        reset_cache_bucket(self.seaweed)
        for scenario in self.params[0]:
            num_rows, num_value_cols = scenario
            rng = np.random.default_rng([num_rows, num_value_cols])
            target = generate_merge_target(num_rows, num_value_cols, self.value_dtype, self.INDEX_KIND, rng)
            self._write_cache_target(make_lib_name(scenario, self.INDEX_KIND), target)

    def setup(self, scenario, strategy, on_count, source_size):
        self._prepare_merge(make_lib_name(scenario, self.INDEX_KIND), on_count, source_size)

    def teardown(self, scenario, strategy, on_count, source_size):
        self.seaweed.delete_bucket(WORK_BUCKET)

    def time_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)

    def peakmem_merge(self, scenario, strategy, on_count, source_size):
        self.merge(strategy)
