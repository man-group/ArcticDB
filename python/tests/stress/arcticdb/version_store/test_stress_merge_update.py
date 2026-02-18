import pytest
import pandas as pd
import random
from arcticdb.util.test import assert_frame_equal, assert_vit_equals_except_data, merge
import arcticdb
from arcticdb.version_store import VersionedItem
import numpy as np
from arcticdb.exceptions import StreamDescriptorMismatch, UserInputException, SortingException, StorageException
from arcticdb.version_store.library import MergeAction, MergeStrategy
from arcticdb.version_store._store import normalize_merge_action
from typing import List
import arcticdb.toolbox.query_stats as qs
import math


def make_matching_source(target_df: pd.DataFrame, segments: List[int], rows_per_segment: int) -> pd.DataFrame:
    indices = []
    for seg in segments:
        start = seg * rows_per_segment
        end = start + rows_per_segment
        segment_indices = target_df.index[start:end]
        n = np.random.randint(1, high=rows_per_segment)
        chosen = np.random.choice(segment_indices, size=n, replace=False)
        indices.extend(chosen)

    res = pd.DataFrame(
        {
            col: np.random.randint(0, high=2**32, size=len(indices), dtype=np.uint32).view(np.float32)
            for col in target_df.columns
        },
        index=pd.DatetimeIndex(indices, name=target_df.index.name),
    )
    res.sort_index(inplace=True)
    return res


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
    def test_merge_update(self, s3_version_store_v1, segments_to_update):
        qs.reset_stats()
        lib = s3_version_store_v1
        lib.write("sym", self.__class__.data)
        source = make_matching_source(self.__class__.data, segments_to_update, self.__class__.rows_per_segment)
        strategy = MergeStrategy(matched="update", not_matched_by_target="do_nothing")
        with qs.query_stats():
            lib.merge_experimental("sym", source, strategy=strategy)
        stats = qs.get_query_stats()
        expected_table_data_read_count = len(segments_to_update) * self.__class__.col_slices
        assert stats["storage_operations"]["S3_GetObject"]["TABLE_DATA"]["count"] == expected_table_data_read_count
        assert stats["storage_operations"]["S3_PutObject"]["TABLE_DATA"]["count"] == expected_table_data_read_count
        assert stats["storage_operations"]["S3_GetObject"]["TABLE_INDEX"]["count"] == 1
        assert stats["storage_operations"]["S3_PutObject"]["TABLE_INDEX"]["count"] == 1

        result = lib.read("sym").data
        expected = merge(self.__class__.data, source, strategy=strategy, inplace=False)
        assert_frame_equal(result, expected)
