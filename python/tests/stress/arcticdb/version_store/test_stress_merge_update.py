import pytest
import pandas as pd
import random
from arcticdb.util.test import assert_frame_equal, assert_vit_equals_except_data, merge, query_stats_operation_count
import arcticdb
from arcticdb.version_store import VersionedItem
import numpy as np
from arcticdb.exceptions import StreamDescriptorMismatch, UserInputException, UnsortedDataException, StorageException
from arcticdb.version_store.library import MergeAction, MergeStrategy
from arcticdb.version_store._store import normalize_merge_action
from typing import List, Optional
import arcticdb.toolbox.query_stats as qs
import math

pytestmark = pytest.mark.merge_update


def make_matching_source(
    target_df: pd.DataFrame,
    segments: List[int],
    rows_per_segment: int,
    on: Optional[List[str]] = None,
    on_segments: Optional[List[int]] = None,
) -> pd.DataFrame:
    indices = []
    assert (on is not None and on_segments is not None) or (on is None and on_segments is None)
    on = on or []
    on_segments = on_segments or []
    matched_columns = pd.DataFrame(columns=on)
    segments = sorted(segments)
    for seg in segments:
        start = seg * rows_per_segment
        end = start + rows_per_segment
        n = np.random.randint(1, high=rows_per_segment)
        rows = sorted(np.random.choice(range(start, end), size=n, replace=False))
        indices.extend(target_df.iloc[rows].index.tolist())

        if on:
            target_vals = target_df.iloc[rows][on]
            if seg not in on_segments:
                # Flip all bits of the underlying uint32 representation to guarantee different float32 values. XOR with
                # 0xFFFFFFFF guarantees source[i] != target[i] for every row because no IEEE 754 float equals its
                # bitwise complement (NaN -> non-NaN, non-NaN may -> NaN, etc.).
                # This is guaranteed to work only if the dates in the index are unique. Otherwise, there's still a
                # chance (even though small) to get a match.
                target_vals = target_vals.copy(deep=True)
                for col in on:
                    raw = target_vals[col].values.view(np.uint32)
                    target_vals[col] = (raw ^ np.uint32(0xFFFFFFFF)).view(np.float32)
            matched_columns = pd.concat([matched_columns, target_vals], ignore_index=True)

    data = {}
    for col in target_df.columns:
        if col in matched_columns.columns:
            data[col] = matched_columns[col].values
        else:
            data[col] = np.random.randint(0, high=2**32, size=len(indices), dtype=np.uint32).view(np.float32)
    return pd.DataFrame(data, index=pd.DatetimeIndex(indices, name=target_df.index.name))


# Merge update stress tests use a lot of memory and can OOM if run in parallel. Add a xdist_group to ensure the tests
# are not running in parallel with each other.
@pytest.mark.xdist_group(name="stress_merge_update")
class TestStressTimeseriesMergeUpdate:

    @classmethod
    def setup_class(cls):
        np.random.seed(102323)
        random.seed(102323)
        cls.rows_per_segment = 100_000
        cls.cols_per_segment = 127
        cls.row_slices = 10
        cls.total_rows = cls.rows_per_segment * cls.row_slices
        cls.total_cols = 150
        cls.col_slices = math.ceil(cls.total_cols / cls.cols_per_segment)
        cls.data = pd.DataFrame(
            {
                f"col_{i}": np.random.randint(0, high=2**32, size=cls.total_rows, dtype=np.uint32).view(np.float32)
                for i in range(cls.total_cols)
            },
            index=pd.date_range(pd.Timestamp(0), periods=cls.total_rows, freq="100ms"),
        )

    @pytest.mark.parametrize(
        "segments_to_update",
        [[0], [9], [5], [8, 9], [0, 1, 2], [2, 3, 4, 5], [0, 2, 4, 6, 8], [1, 3, 5, 7, 9], [1, 4, 5, 6, 9]],
    )
    def test_merge_update(self, in_memory_store_factory, segments_to_update, clear_query_stats):
        lib = in_memory_store_factory()
        lib.write("sym", self.__class__.data)
        source = make_matching_source(self.__class__.data, segments_to_update, self.__class__.rows_per_segment)
        strategy = MergeStrategy(matched="update", not_matched_by_target="do_nothing")
        with qs.query_stats():
            lib.merge_experimental("sym", source, strategy=strategy)
        stats = qs.get_query_stats()
        expected_table_data_read_count = len(segments_to_update) * self.__class__.col_slices
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == expected_table_data_read_count
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == expected_table_data_read_count
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_INDEX") == 1

        result = lib.read("sym").data
        expected = merge(self.__class__.data, source, strategy=strategy)
        assert_frame_equal(result, expected)

    @pytest.mark.parametrize("on_segments", [[], [1, 4, 5, 6, 9], [5, 6], [1, 9], [4, 5, 9]])
    def test_merge_update_on(self, s3_version_store_v1, on_segments):
        segments_to_update = [1, 4, 5, 6, 9]
        on = ["col_1", "col_130"]
        qs.reset_stats()
        lib = s3_version_store_v1
        lib.write("sym", self.__class__.data)
        source = make_matching_source(
            self.__class__.data, segments_to_update, self.__class__.rows_per_segment, on, on_segments
        )
        strategy = MergeStrategy(matched="update", not_matched_by_target="do_nothing")
        with qs.query_stats():
            lib.merge_experimental("sym", source, strategy=strategy, on=on)
        stats = qs.get_query_stats()
        expected_table_data_read_count = len(segments_to_update) * self.__class__.col_slices
        expected_table_data_write_count = (
            len(set(segments_to_update).intersection(set(on_segments))) * self.__class__.col_slices
        )
        assert stats["storage_operations"]["S3_GetObject"]["TABLE_DATA"]["count"] == expected_table_data_read_count
        written_table_data_keys = 0
        if "TABLE_DATA" in stats["storage_operations"]["S3_PutObject"]:
            written_table_data_keys = stats["storage_operations"]["S3_PutObject"]["TABLE_DATA"]["count"]
        assert written_table_data_keys == expected_table_data_write_count
        assert stats["storage_operations"]["S3_GetObject"]["TABLE_INDEX"]["count"] == 1
        assert stats["storage_operations"]["S3_PutObject"]["TABLE_INDEX"]["count"] == 1

        result = lib.read("sym").data
        expected = merge(self.__class__.data, source, strategy=strategy, on=on)
        assert_frame_equal(result, expected)
