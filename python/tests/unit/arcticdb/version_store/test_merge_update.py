"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest
import pandas as pd
from arcticdb.util.test import assert_frame_equal, assert_vit_equals_except_data, merge, query_stats_operation_count
import arcticdb
from arcticdb.version_store import VersionedItem
from arcticdb_ext.exceptions import SchemaException
from arcticdb_ext.storage import KeyType
import numpy as np
from arcticdb.exceptions import (
    UserInputException,
    UnsortedDataException,
    StorageException,
    ArcticException,
)
from arcticdb.version_store.library import MergeAction, MergeStrategy
from arcticdb.version_store._store import normalize_merge_action
from typing import Union, List, Optional
import arcticdb.toolbox.query_stats as qs

pytestmark = pytest.mark.merge_update


def mock_find_keys_for_symbol(key_types):
    keys = {kt: [f"{kt}_{i}" for i in range(key_types[kt])] for kt in key_types}
    return lambda key_type, symbol: keys[key_type]


def generic_merge_test(
    lib,
    sym: str,
    target: Union[List[pd.DataFrame], pd.DataFrame],
    source: pd.DataFrame,
    strategy: MergeStrategy,
    expected: pd.DataFrame,
    on: Optional[List[str]] = None,
):
    if isinstance(target, pd.DataFrame):
        target = [target]
    concat_target = pd.concat(target)
    # Run the actual merge and compare against expected
    lib.write(sym, target[0])
    for df in target[1:]:
        lib.append(sym, df)
    lib.merge_experimental(sym, source, strategy=strategy, on=on)
    read_vit = lib.read(sym)
    assert_frame_equal(read_vit.data, expected)
    oracle_expected = merge(concat_target, source, strategy, on=on)
    assert_frame_equal(oracle_expected, expected)
    return read_vit


@pytest.mark.parametrize(
    "action",
    [
        ("update", MergeAction.UPDATE),
        ("UPDATE", MergeAction.UPDATE),
        ("uPdATe", MergeAction.UPDATE),
        (MergeAction.UPDATE, MergeAction.UPDATE),
        ("insert", MergeAction.INSERT),
        ("INSERT", MergeAction.INSERT),
        ("inSErT", MergeAction.INSERT),
        (MergeAction.INSERT, MergeAction.INSERT),
        ("do_nothing", MergeAction.DO_NOTHING),
        ("DO_NOTHING", MergeAction.DO_NOTHING),
        ("Do_NothING", MergeAction.DO_NOTHING),
        (MergeAction.DO_NOTHING, MergeAction.DO_NOTHING),
    ],
)
def test_normalize_merge_action(action):
    assert normalize_merge_action(action[0]) == action[1]


@pytest.mark.parametrize(
    "strategy",
    (
        MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
        pytest.param(
            MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
            marks=pytest.mark.xfail(reason="Insert is not implemented"),
        ),
        pytest.param(
            MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
            marks=pytest.mark.xfail(reason="Insert is not implemented"),
        ),
    ),
)
class TestMergeTimeseriesCommon:

    def test_merge_matched_update_with_metadata(self, lmdb_library, strategy):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]},
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"]),
        )

        metadata = {"meta": "data"}

        merge_vit = lib.merge_experimental("sym", source, metadata=metadata, strategy=strategy)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.metadata == metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)

        lt = lib._dev_tools.library_tool()

        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize("metadata", ({"meta": "data"}, None))
    def test_merge_writes_new_version_with_empty_source(self, lmdb_library, metadata, strategy):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target, metadata="v0")
        merge_vit = lib.merge_experimental("sym", pd.DataFrame(), metadata=metadata, strategy=strategy)
        assert merge_vit.metadata is None if metadata is None else merge_vit.metadata == metadata
        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2
        read_vit = lib.read("sym")
        assert read_vit.version == 1
        assert merge_vit.metadata is None if metadata is None else merge_vit.metadata == metadata
        assert_frame_equal(read_vit.data, target)

    @pytest.mark.parametrize(
        "source",
        [
            # "a" has different type
            pd.DataFrame(
                {"a": np.array([1, 2, 3], dtype=np.int8), "b": [1.0, 2.0, 3.0]},
                index=pd.DatetimeIndex(
                    [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01 10:00:00"), pd.Timestamp("2024-01-02")]
                ),
            ),
            # "a" is missing, replaced by "c"
            pd.DataFrame(
                {"c": np.array([1, 2, 3], dtype=np.int8), "b": [1.0, 2.0, 3.0]},
                index=pd.DatetimeIndex(
                    [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01 10:00:00"), pd.Timestamp("2024-01-02")]
                ),
            ),
            # "a" is missing
            pd.DataFrame(
                {"b": [1.0, 2.0, 3.0]},
                index=pd.DatetimeIndex(
                    [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01 10:00:00"), pd.Timestamp("2024-01-02")]
                ),
            ),
            # Schemas differ but no data is loaded from the disk
            pd.DataFrame(
                {"b": [1.0, 2.0, 3.0]},
                index=pd.DatetimeIndex(
                    [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-01 10:00:00"), pd.Timestamp("2020-01-02")]
                ),
            ),
        ],
    )
    def test_static_schema_merge_throws_when_schemas_differ(self, lmdb_library, strategy, source):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)
        with pytest.raises(SchemaException):
            lib.merge_experimental("sym", source, strategy=strategy)

    def test_throws_if_source_is_not_sorted(self, lmdb_library, strategy):
        # This requirement can be lifted, however, passing a sorted source will be faster. We can start with it and
        # extend if needed.
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [10, 20, 30], "b": [10.1, 20.1, 30.1]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-01")]
            ),
        )

        with pytest.raises(UnsortedDataException):
            lib.merge_experimental("sym", source, strategy=strategy)


class TestMergeTimeseriesUpdate:

    def setup_method(self):
        self.strategy = MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING)

    @pytest.mark.parametrize(
        "strategy",
        (
            MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
            pytest.param(MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING), marks=pytest.mark.skip),
            pytest.param(MergeStrategy("update", "do_nothing"), marks=pytest.mark.skip),
        ),
    )
    def test_basic(self, lmdb_library, strategy):
        lib = lmdb_library

        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3)
        )
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [4, 5, 6], "b": [7.0, 8.0, 9.0], "c": ["A", "B", "C"]},
            # Only the second row: "2024-01-02" matches
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"]),
        )

        merge_vit = lib.merge_experimental("sym", source, strategy=strategy)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        # Only Jan2 matches → update row 1 with source values
        expected = pd.DataFrame(
            {"a": [1, 5, 3], "b": [1.0, 8.0, 3.0], "c": ["a", "B", "c"]},
            index=pd.date_range("2024-01-01", periods=3),
        )

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)

        lt = lib._dev_tools.library_tool()

        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_write_new_version_even_if_nothing_is_changed(self, lmdb_library):
        # In theory, it's possible to make so that it doesn't write a new version when nothing is matched, but the source
        # is not empty. This has lots of edge cases and will burden the implementation for almost no gain. If nothing is
        #  changed, we'll create a new index key pointing to the existing data keys and write a new version key which is cheap.
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)
        source = pd.DataFrame({"a": [4, 5], "b": [4.0, 5.0]}, index=pd.date_range("2023-01-01", periods=2))
        merge_vit = lib.merge_experimental(
            "sym", source, strategy=MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING)
        )
        assert merge_vit.version == 1

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, target)

        lt = lib._dev_tools.library_tool()

        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize(
        "slicing_policy",
        [{"rows_per_segment": 2}, {"columns_per_segment": 2}, {"rows_per_segment": 2, "columns_per_segment": 2}],
    )
    def test_row_slicing(self, lmdb_library_factory, slicing_policy):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(**slicing_policy))
        target = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5],
                "b": [1.0, 2.0, 3.0, 4.0, 5.0],
                "c": [True, False, True, False, True],
                "d": ["a", "b", "c", "d", "e"],
            },
            index=pd.date_range("2024-01-01", periods=5),
        )

        source = pd.DataFrame(
            {"a": [30, 50], "b": [30.1, 50.1], "c": [False, False], "d": ["C", "E"]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-05")]),
        )
        # Jan3 and Jan5 match → update those rows
        expected = pd.DataFrame(
            {
                "a": [1, 2, 30, 4, 50],
                "b": [1.0, 2.0, 30.1, 4.0, 50.1],
                "c": [True, False, False, False, False],
                "d": ["a", "b", "C", "d", "E"],
            },
            index=pd.date_range("2024-01-01", periods=5),
        )
        read_vit = generic_merge_test(lib, "sym", target, source, self.strategy, expected)

        lt = lib._dev_tools.library_tool()
        if "rows_per_segment" in slicing_policy and "columns_per_segment" in slicing_policy:
            # Start with 3 row slices and 2 column slices = 6 data keys
            # The second row-slice is overwritten with column slicing = 2 data keys
            # The third row-slice is overwritten with column slicing = 2 data keys
            expected_data_keys = 10
        elif "rows_per_segment" in slicing_policy:
            # Start with 3 row slices and no column slices -> 3 data keys
            # The second row-slice is overwritten with column slicing = 1 data key
            # The third row-slice is overwritten with column slicing = 1 data key
            expected_data_keys = 5
        elif "columns_per_segment" in slicing_policy:
            # Start with one row slice and 2 column slices -> 2 data keys
            # The single row-slice is overwritten with column slicing = 2 data keys
            expected_data_keys = 4
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == expected_data_keys
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize(
        "slicing_policy",
        [
            {"columns_per_segment": 2},
            {"rows_per_segment": 2, "columns_per_segment": 2},
        ],
    )
    @pytest.mark.parametrize(
        "on, expected",
        [
            (
                ["a"],
                # a=1,3,4,5 match on index+a; a=99 no match
                pd.DataFrame(
                    {
                        "a": [1, 2, 3, 4, 5],
                        "b": [10.0, 2.0, 30.0, 40.0, 50.0],
                        "c": ["X", "y", "Z", "W", "V"],
                        "d": [10, 20, 99, 40, 50],
                    },
                    index=pd.date_range("2024-01-01", periods=5),
                ),
            ),
            (
                ["d"],
                # d=10,20,40,50 match on index+d; d=99 no match
                pd.DataFrame(
                    {
                        "a": [1, 99, 3, 4, 5],
                        "b": [10.0, 20.0, 3.0, 40.0, 50.0],
                        "c": ["X", "Y", "z", "W", "V"],
                        "d": [10, 20, 30, 40, 50],
                    },
                    index=pd.date_range("2024-01-01", periods=5),
                ),
            ),
            (
                ["a", "d"],
                # only rows where both a AND d match; row 0 (a=1,d=10), row 3 (a=4,d=40), row 4 (a=5,d=50)
                pd.DataFrame(
                    {
                        "a": [1, 2, 3, 4, 5],
                        "b": [10.0, 2.0, 3.0, 40.0, 50.0],
                        "c": ["X", "y", "z", "W", "V"],
                        "d": [10, 20, 30, 40, 50],
                    },
                    index=pd.date_range("2024-01-01", periods=5),
                ),
            ),
        ],
    )
    def test_on_column_with_column_slicing(self, lmdb_library_factory, slicing_policy, on, expected):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(**slicing_policy))
        target = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5],
                "b": [1.0, 2.0, 3.0, 4.0, 5.0],
                "c": ["x", "y", "z", "w", "v"],
                "d": [10, 20, 30, 40, 50],
            },
            index=pd.date_range("2024-01-01", periods=5),
        )
        source = pd.DataFrame(
            {
                "a": [1, 99, 3, 4, 5],
                "b": [10.0, 20.0, 30.0, 40.0, 50.0],
                "c": ["X", "Y", "Z", "W", "V"],
                "d": [10, 20, 99, 40, 50],
            },
            index=pd.date_range("2024-01-01", periods=5),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=on)

    def test_on_empty_list_same_as_none(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame(
            {"a": [10, 20], "b": [10.0, 20.0]},
            index=pd.DatetimeIndex(["2024-01-01", "2024-01-05"]),
        )
        # on=[] same as None: match on index only. Jan1 matches → update; Jan5 no match
        expected = pd.DataFrame({"a": [10, 2, 3], "b": [10.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=[])

    def test_on_index_and_column(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame(
            {"a": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0]},
            index=pd.DatetimeIndex(
                [
                    "2024-01-01",  # Matches index and column
                    "2024-01-02 01:00:00",  # Matches column, but not index
                    "2024-01-03",  # Matches index, but not column
                    "2024-01-04",  # Does not match either
                ]
            ),
        )
        # Only first source row matches (index=Jan1, a=1)
        expected = pd.DataFrame({"a": [1, 2, 3], "b": [10.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    def test_multiple_columns(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {
                "a": [1, 2, 3, 4],
                "b": ["a", "b", "c", "d"],
                "c": ["A", "B", "A", "C"],
                "d": [10.1, 20.2, 30.3, 40.4],
                "e": [100, 200, 300, 400],
            },
            index=pd.date_range("2024-01-01", periods=4),
        )

        source = pd.DataFrame(
            {
                "a": [10, 20, 30, 40, 50],
                "b": ["a", "b", "c", "d", "e"],
                "c": ["A", "D", "A", "B", "C"],
                "d": [10.1, 50.5, 30.3, 40.4, 70.7],
                "e": [100, 500, 600, 400, 800],
            },
            index=pd.DatetimeIndex(
                [
                    "2024-01-01",  # index + b + d + e match (should update)
                    "2024-01-02",  # index + b match, but d + e differ (do nothing)
                    "2024-01-03",  # index + b + d match, e differs (do nothing)
                    "2024-01-05",  # new index, b+d+e match (do nothing)
                    "2024-01-06",  # new index, new b, new d, new e (do nothing)
                ]
            ),
        )
        # Only row 0 matches (index=Jan1, b="a", d=10.1, e=100); rows 1-4 differ on at least one on-column
        expected = pd.DataFrame(
            {
                "a": [10, 2, 3, 4],
                "b": ["a", "b", "c", "d"],
                "c": ["A", "B", "A", "C"],
                "d": [10.1, 20.2, 30.3, 40.4],
                "e": [100, 200, 300, 400],
            },
            index=pd.date_range("2024-01-01", periods=4),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["b", "d", "e"])

    def test_row_from_source_matches_multiple_rows_from_target(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
            ),
        )
        source = pd.DataFrame({"a": [5], "b": [20.0]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]))
        # Source Jan1 matches both target rows at Jan1
        expected = pd.DataFrame(
            {"a": [5, 5, 3], "b": [20.0, 20.0, 3.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
            ),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected)

    def test_row_from_source_matches_multiple_rows_from_target_in_separate_slices(self, lmdb_library_factory):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=2))
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")]
            ),
        )
        source = pd.DataFrame({"a": [5], "b": [20.0], "c": ["B"]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")]))
        # Source Jan2 matches both target rows at Jan2
        expected = pd.DataFrame(
            {"a": [1, 5, 5], "b": [1.0, 20.0, 20.0], "c": ["a", "B", "B"]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")]
            ),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected)

    def test_throws_when_target_row_is_matched_more_than_once(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),  # Matches first row of target
                    pd.Timestamp("2024-01-01"),  # Also matches first row of target
                    pd.Timestamp("2024-01-03"),
                ]
            ),
        )
        with pytest.raises(UserInputException):
            lib.merge_experimental("sym", source, strategy=self.strategy)

    @pytest.mark.parametrize("merge_metadata", (None, "meta"))
    def test_target_is_empty(self, lmdb_library, merge_metadata):
        lib = lmdb_library
        target = pd.DataFrame({"a": np.array([], dtype=np.int64)}, index=pd.DatetimeIndex([]))
        lib.write("sym", target)
        source = pd.DataFrame({"a": np.array([1, 2], dtype=np.int64)}, index=pd.date_range("2024-01-01", periods=2))
        merge_vit = lib.merge_experimental(
            "sym", source, strategy=MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING), metadata=merge_metadata
        )
        assert merge_vit.metadata is None if merge_metadata is None else merge_vit.metadata == merge_metadata
        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 0
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2
        read_vit = lib.read("sym")
        assert read_vit.metadata is None if merge_metadata is None else read_vit.metadata == merge_metadata
        assert_frame_equal(read_vit.data, target)

    @pytest.mark.parametrize(
        "source",
        (
            pd.DataFrame([], index=pd.DatetimeIndex([])),
            pd.DataFrame({"a": []}, index=pd.DatetimeIndex([])),
            pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])),
        ),
    )
    @pytest.mark.parametrize("upsert", (True, False))
    def test_target_symbol_does_not_exist(self, lmdb_library, source, upsert):
        # An update-only strategy never inserts unmatched rows, so upserting into a non-existent symbol could only
        # ever create an empty symbol. That is most likely a user error, thus it throws instead.
        lib = lmdb_library

        expected_exception = UserInputException if upsert else StorageException
        with pytest.raises(expected_exception):
            lib.merge_experimental(
                "sym", source, strategy=MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING), upsert=upsert
            )
        assert not lib.has_symbol("sym")

    def test_upsert_with_existing_symbol(self, lmdb_library):
        # When the symbol exists upsert is irrelevant and a regular merge is performed.
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)
        source = pd.DataFrame(
            {"a": [20, 40]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-04")])
        )
        merge_vit = lib.merge_experimental("sym", source, strategy=self.strategy, upsert=True)
        assert merge_vit.version == 1
        expected = pd.DataFrame({"a": [1, 20, 3]}, index=pd.date_range("2024-01-01", periods=3))
        assert_frame_equal(lib.read("sym").data, expected)

    def test_updates_the_latest_live_version(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1

        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3)
        )
        lib.write("sym", target)
        # Write a second version. Nothing in this version will match the source
        lib.write(
            "sym",
            pd.DataFrame(
                {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]},
                index=pd.date_range("2020-01-01", periods=3),
            ),
        )
        lib.delete_version("sym", 1)

        source = pd.DataFrame(
            {"a": [4, 5, 6], "b": [7.0, 8.0, 9.0], "c": ["A", "B", "C"]},
            # Only the second row: "2024-01-02" matches
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"]),
        )

        # The merge will be performed on the latest undeleted version
        merge_vit = lib.merge_experimental("sym", source, strategy=self.strategy)
        # Only Jan2 matches → update row 1 with source values
        expected = pd.DataFrame(
            {"a": [1, 5, 3], "b": [1.0, 8.0, 3.0], "c": ["a", "B", "c"]},
            index=pd.date_range("2024-01-01", periods=3),
        )
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert merge_vit.version == 2
        assert_frame_equal(read_vit.data, expected)

    def test_throws_if_all_versions_are_deleted(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1

        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3)
        )
        lib.write("sym", target)
        # Write a second version. Nothing in this version will match the source
        lib.write(
            "sym",
            pd.DataFrame(
                {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]},
                index=pd.date_range("2020-01-01", periods=3),
            ),
        )
        lib.delete("sym")

        source = pd.DataFrame(
            {"a": [4, 5, 6], "b": [7.0, 8.0, 9.0], "c": ["A", "B", "C"]},
            # Only the second row: "2024-01-02" matches
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"]),
        )

        # The merge will be performed on the latest undeleted version
        with pytest.raises(StorageException):
            lib.merge_experimental("sym", source, strategy=self.strategy)

    def test_two_segments_with_same_index_value(self, s3_version_store_v1):
        # The merge operation will write two segments with the same index range and the same content in parallel,
        # occasionally both data keys will end up with the same ID. LMDB throws in that case while S3 will overwrite
        # the key.
        lib = s3_version_store_v1
        target = [
            pd.DataFrame({"a": [1]}, index=[pd.Timestamp(0)]),
            pd.DataFrame({"a": [2]}, index=[pd.Timestamp(0)]),
        ]

        source = pd.DataFrame({"a": [3]}, index=[pd.Timestamp(0)])
        for df in target:
            lib.append("sym", df)
        lib.merge_experimental("sym", source, strategy=self.strategy)
        result = lib.read("sym").data
        # Both target rows at Ts0 match source at Ts0 → both updated to a=3
        expected = pd.DataFrame({"a": [3, 3]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0)]))
        assert_frame_equal(result, expected)

    def test_sorted_segments_overlap(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        target_list = [
            pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(0)])),
            pd.DataFrame({"a": [2, 3]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1)])),
        ]
        source = pd.DataFrame({"a": [5, 6]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5)]))
        for tgt in target_list:
            lib.append("test", tgt)
        lib.merge_experimental("test", source, strategy=self.strategy)
        res = lib.read("test").data
        # Ts0 from source matches target rows 0 and 1 → both updated to a=5; Ts5 no match → do_nothing
        expected = pd.DataFrame(
            {"a": [5, 5, 3]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(1)])
        )
        assert_frame_equal(res, expected)

    def test_sorted_segments_overlap_but_source_is_in_first_segment_only(self, lmdb_version_store_v1):
        target1 = pd.DataFrame({"a": [1, 2]}, index=pd.to_datetime([pd.Timestamp(1), pd.Timestamp(2)]))
        target2 = pd.DataFrame({"a": [3]}, index=pd.to_datetime([pd.Timestamp(2)]))
        target_list = [target1, target2]
        source = pd.DataFrame({"a": [4, 5]}, index=pd.to_datetime([pd.Timestamp(0), pd.Timestamp(1)]))
        lib = lmdb_version_store_v1
        # Ts(0) no match, Ts(1) matches target row 0 → a=5
        expected = pd.DataFrame(
            {"a": [5, 2, 3]}, index=pd.to_datetime([pd.Timestamp(1), pd.Timestamp(2), pd.Timestamp(2)])
        )
        generic_merge_test(lib, "sym", target_list, source, self.strategy, expected)

    def test_source_matches_first_value_of_first_segment_and_last_value_of_second_segment(self, lmdb_version_store_v1):
        target1 = pd.DataFrame({"a": [1, 2]}, index=pd.to_datetime([pd.Timestamp(0), pd.Timestamp(1)]))
        target2 = pd.DataFrame({"a": [3]}, index=pd.to_datetime([pd.Timestamp(1)]))
        target_list = [target1, target2]
        source = pd.DataFrame({"a": [5, 6]}, index=pd.to_datetime([pd.Timestamp(0), pd.Timestamp(1)]))
        lib = lmdb_version_store_v1
        # Ts(0) matches target row 0 → a=5; Ts(1) matches target rows 1 and 2 → a=6
        expected = pd.DataFrame(
            {"a": [5, 6, 6]}, index=pd.to_datetime([pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(1)])
        )
        generic_merge_test(lib, "sym", target_list, source, self.strategy, expected)

    @pytest.mark.parametrize(
        "source, expected",
        [
            (
                pd.DataFrame({"a": [1], "b": [99.0]}, index=pd.DatetimeIndex([pd.Timestamp(0)])),
                # a=1 at Ts(0) matches target row 0 → b=99.0
                pd.DataFrame(
                    {"a": [1, 2, 3], "b": [99.0, 20.0, 30.0]},
                    index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(1)]),
                ),
            ),
            (
                pd.DataFrame({"a": [2], "b": [99.0]}, index=pd.DatetimeIndex([pd.Timestamp(0)])),
                # a=2 at Ts(0) matches target row 1 → b=99.0
                pd.DataFrame(
                    {"a": [1, 2, 3], "b": [10.0, 99.0, 30.0]},
                    index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(1)]),
                ),
            ),
        ],
    )
    def test_on_column_with_overlapping_segments(self, lmdb_version_store_v1, source, expected):
        lib = lmdb_version_store_v1
        target_list = [
            pd.DataFrame({"a": [1], "b": [10.0]}, index=pd.DatetimeIndex([pd.Timestamp(0)])),
            pd.DataFrame({"a": [2, 3], "b": [20.0, 30.0]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1)])),
        ]
        generic_merge_test(lib, "sym", target_list, source, self.strategy, expected, on=["a"])

    def test_index_matches_but_on_column_differs(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame(
            {"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]},
            index=pd.date_range("2024-01-01", periods=3),
        )
        # Index matches but a values differ (1≠10, 2≠20, 3≠30) → no updates
        expected = target.copy()
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    def test_all_columns_in_on(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame(
            {"a": [1, 2, 3], "b": [99.0, 99.0, 99.0]},
            index=pd.date_range("2024-01-01", periods=3),
        )
        # b values differ (99≠1, 99≠2, 99≠3) → no matches
        expected = target.copy()
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a", "b"])

    def test_match_on_string_none_nan_indistinguishable(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        target = pd.DataFrame(
            {"a": ["a", np.nan, None, np.nan, None], "b": [1, 2, 3, 4, 5]}, index=pd.date_range("2024-01-01", periods=5)
        )
        source = pd.DataFrame(
            {"a": ["a", np.nan, None, None, np.nan], "b": [10, 20, 30, 40, 50]},
            index=pd.date_range("2024-01-01", periods=5),
        )
        expected = pd.DataFrame(
            {"a": ["a", np.nan, None, np.nan, None], "b": [10, 20, 30, 40, 50]},
            index=pd.date_range("2024-01-01", periods=5),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected)

    def test_match_on_float_nan(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        target = pd.DataFrame({"a": [1.0, np.nan, 3.0], "b": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame({"a": [np.nan], "b": [20]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")]))
        # NaN matches NaN at Jan2 → update b=20
        expected = pd.DataFrame(
            {"a": [1.0, np.nan, 3.0], "b": [1, 20, 3]}, index=pd.date_range("2024-01-01", periods=3)
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    def test_on_column_one_source_row_matches_multiple_target_rows(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 1, 2], "b": [10.0, 20.0, 30.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
            ),
        )
        source = pd.DataFrame({"a": [1], "b": [99.0]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]))
        # a=1 at Jan1 matches both target rows at Jan1 with a=1
        expected = pd.DataFrame(
            {"a": [1, 1, 2], "b": [99.0, 99.0, 30.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
            ),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    def test_on_column_one_source_row_matches_multiple_target_rows_across_segments(self, lmdb_library):
        lib = lmdb_library
        target = [
            pd.DataFrame(
                {"a": [1, 1, 2], "b": [10.0, 20.0, 30.0]},
                index=pd.DatetimeIndex(
                    [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
                ),
            ),
            pd.DataFrame(
                {"a": [2, 3], "b": [40.0, 50.0]},
                index=pd.DatetimeIndex([pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")]),
            ),
        ]
        source = pd.DataFrame({"a": [2], "b": [99.0]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]))
        # a=2 at Jan1: no target at Jan1 has a=2 → no match
        expected = pd.DataFrame(
            {"a": [1, 1, 2, 2, 3], "b": [10.0, 20.0, 30.0, 40.0, 50.0]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-01-03"),
                ]
            ),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    def test_on_nonexistent_column_raises(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2], "b": [1.0, 2.0]}, index=pd.date_range("2024-01-01", periods=2))
        lib.write("sym", target)
        source = pd.DataFrame({"a": [1], "b": [10.0]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]))
        with pytest.raises(UserInputException) as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["nonexistent"])
        assert '"nonexistent"' in str(exc_info.value)

    def test_on_with_repeated_index_values(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5], "b": [1.0, 2.0, 3.0, 4.0, 5.0], "c": ["a", "b", "c", "d", "e"]},
            index=pd.DatetimeIndex(
                [pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(2)]
            ),
        )
        source = pd.DataFrame(
            {"a": [2, 100, 4], "b": [100.0, 101.0, 102.0], "c": ["B", "c", "d"]},
            index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)]),
        )
        # No source (a,b) pair matches any target (a,b) pair at the same timestamp → no updates
        expected = target.copy()
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a", "b"])

    def test_on_list_contains_the_same_column_twice(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        source = pd.DataFrame(
            {"a": [1, 20, 3], "b": [10.0, 20.0, 30.0]},
            index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        # Deduped to on=["a"]. Source a=3 at Ts(2) matches target a=3 at Ts(2) → update b=30.0
        expected = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 30.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a", "a"])

    def test_throws_when_multiple_source_rows_match_same_target_row(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        source = pd.DataFrame(
            {"a": [10, 20], "b": [10.0, 20.0]},
            index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(1)]),
        )
        lib.write("sym", target)
        with pytest.raises(UserInputException, match="Multiple source rows match the same target row"):
            lib.merge_experimental("sym", source, strategy=self.strategy)

    def test_throws_when_multiple_source_rows_match_same_target_row_with_on(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        source = pd.DataFrame(
            {"a": [2, 2], "b": [10.0, 20.0]},
            index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(1)]),
        )
        lib.write("sym", target)
        with pytest.raises(UserInputException, match="Multiple source rows match the same target row"):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["a"])

    def test_throws_when_multiple_source_rows_match_same_target_row_via_nan(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [None, "x", "y"], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        source = pd.DataFrame(
            {"a": np.array([None, np.nan], dtype=object), "b": [10.0, 20.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0)]),
        )
        lib.write("sym", target)
        with pytest.raises(UserInputException, match="Multiple source rows match the same target row"):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["a"])

    def test_two_segments_same_timestamp_repeated_source_values(self, lmdb_library):
        lib = lmdb_library

        t0 = pd.Timestamp("2000-01-01")
        t1 = pd.Timestamp("2000-01-02")

        df1 = pd.DataFrame({"a": [1], "b": [0.0]}, index=pd.DatetimeIndex([t1]))
        df2 = pd.DataFrame({"a": [0], "b": [255.0]}, index=pd.DatetimeIndex([t1]))
        source = pd.DataFrame({"a": [0, 1, 0], "b": [0.0, 0.0, 0.0]}, index=pd.DatetimeIndex([t0, t1, t1]))

        # Source (t0,a=0) no match; (t1,a=1) matches df1 row → b=0.0; (t1,a=0) matches df2 row → b=0.0
        expected = pd.DataFrame({"a": [1, 0], "b": [0.0, 0.0]}, index=pd.DatetimeIndex([t1, t1]))
        generic_merge_test(lib, "sym", [df1, df2], source, self.strategy, expected, on=["a"])

    @pytest.mark.parametrize("on", ([None], ["a", None]))
    def test_match_on_column_named_none(self, lmdb_library, on):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], None: [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        source = pd.DataFrame(
            {"a": [10, 20, 30], None: [10.0, 20.0, 30.0]},
            index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        lib.write("sym", target)
        with pytest.raises(TypeError):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=on)

    def test_match_on_column_named_index_and_unnamed_index(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"index": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame(
            {"index": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0]},
            index=pd.DatetimeIndex(
                [
                    "2024-01-01",  # Matches index and column
                    "2024-01-02 01:00:00",  # Matches column, but not index
                    "2024-01-03",  # Matches index, but not column
                    "2024-01-04",  # Does not match either
                ]
            ),
        )

        # Only first source row matches (index=Jan1, a=1)
        expected = pd.DataFrame(
            {"index": [1, 2, 3], "b": [10.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3)
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["index"])

    def test_match_on_non_existing_column_named_index_and_unnamed_index(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame(
            {"a": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0]},
            index=pd.DatetimeIndex(
                [
                    "2024-01-01",  # Matches index and column
                    "2024-01-02 01:00:00",  # Matches column, but not index
                    "2024-01-03",  # Matches index, but not column
                    "2024-01-04",  # Does not match either
                ]
            ),
        )
        lib.write("sym", target)
        with pytest.raises(UserInputException) as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["index"])
        assert "E_COLUMN_NOT_FOUND" in str(exc_info.value)

    def test_match_on_column_named_index_and_unnamed_index_with_duplicates(self, lmdb_library):
        lib = lmdb_library
        column_names = ["index", "index"]
        target = pd.DataFrame(
            data=[[1, 2], [3, 4], [5, 6]], columns=column_names, index=pd.date_range("2024-01-01", periods=3)
        )
        source = pd.DataFrame(
            data=[[1, 2], [3, 4], [5, 6], [7, 8]],
            columns=column_names,
            index=pd.DatetimeIndex(["2024-01-01", "2024-01-02 01:00:00", "2024-01-03", "2024-01-04"]),
        )
        lib.write("sym", target)
        with pytest.raises(UserInputException, match="E_DUPLICATE_COLUMN") as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["index"])

    @pytest.mark.parametrize("index_name", ("index", "some_name"))
    @pytest.mark.parametrize("column_name", ("index", "some_name"))
    def test_match_on_column_named_as_explicitly_named_index(self, lmdb_library, index_name, column_name):
        lib = lmdb_library
        target = pd.DataFrame(
            {column_name: [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3)
        )
        target.index.name = index_name
        source = pd.DataFrame(
            {column_name: [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0]},
            index=pd.DatetimeIndex(
                [
                    "2024-01-01",  # Matches index and column
                    "2024-01-02 01:00:00",  # Matches column, but not index
                    "2024-01-03",  # Matches index, but not column
                    "2024-01-04",  # Does not match either
                ]
            ),
        )
        source.index.name = index_name
        lib.write("sym", target)
        with pytest.raises(UserInputException) as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=[index_name])
        assert f'"{index_name}"' in str(exc_info.value)
        assert "not contain the datetime index column" in str(exc_info.value)

    def test_on_columns_with_repeated_name(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6]],
            columns=["a", "my_duplicated_column", "my_duplicated_column"],
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1)]),
        )
        source = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6]],
            columns=["a", "my_duplicated_column", "my_duplicated_column"],
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1)]),
        )
        lib.write("sym", target)
        with pytest.raises(UserInputException, match="E_DUPLICATE_COLUMN") as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["my_duplicated_column"])

    def test_on_colum_reorders_matched_rows(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 0, 0, 1, 1], "b": [1, 2, 3, 4, 5], "c": ["a", "b", "c", "d", "e"]},
            index=pd.DatetimeIndex([pd.Timestamp(0)] * 5),
        )
        # The test matches on column "a". Note the order of the columns of a. This means that
        # source[0]: updates target [1, 2]
        # source[1]: updates target []
        # source[2]: updates target [0, 3, 4]
        # Meaning that flatten(rows_to_be_updated) is not a sorted list
        source = pd.DataFrame(
            {"a": [0, 2, 1], "b": [100, 200, 300], "c": ["A", "B", "C"]}, index=pd.DatetimeIndex([pd.Timestamp(0)] * 3)
        )
        expected = pd.DataFrame(
            {"a": [1, 0, 0, 1, 1], "b": [300, 100, 100, 300, 300], "c": ["C", "A", "A", "C", "C"]},
            index=pd.DatetimeIndex([pd.Timestamp(0)] * 5),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])


class TestMergeTimeseriesInsert:

    def setup_method(self):
        self.strategy = MergeStrategy("do_nothing", "insert")

    def test_insert_past_row_slice_boundary_is_not_dropped(self, lmdb_library_factory):
        # Regression test: an unmatched insert row that falls in the gap between two row slices used to be dropped.
        # With rows_per_segment=2 the target [10, 20, 30] is sliced into [10, 20] and [30]; inserting the unmatched
        # rows at 15 and 25 must keep both (25 previously landed past the first slice's end and was lost).
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=2))
        target = pd.DataFrame(
            {"a": [0, 1, 2]},
            index=pd.DatetimeIndex([pd.Timestamp(10), pd.Timestamp(20), pd.Timestamp(30)]),
        )
        lib.write("sym", target)
        source = pd.DataFrame(
            {"a": [100, 101]},
            index=pd.DatetimeIndex([pd.Timestamp(15), pd.Timestamp(25)]),
        )
        lib.merge_experimental("sym", source, strategy=self.strategy)
        expected = pd.DataFrame(
            {"a": [0, 100, 1, 101, 2]},
            index=pd.DatetimeIndex(
                [pd.Timestamp(10), pd.Timestamp(15), pd.Timestamp(20), pd.Timestamp(25), pd.Timestamp(30)]
            ),
        )
        assert_frame_equal(lib.read("sym").data, expected)

    @pytest.mark.parametrize(
        "strategy",
        (MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT), MergeStrategy("do_nothing", "insert")),
    )
    def test_basic(self, lmdb_library, strategy):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, -2, -3], "b": [-1.0, -2.0, -3.0]},
            index=pd.DatetimeIndex(["2023-01-01", "2024-01-01 10:00:00", "2025-01-04"]),
        )

        merge_vit = lib.merge_experimental("sym", source, strategy=strategy)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        expected = pd.concat([target, source]).sort_index()
        assert_frame_equal(lib.read("sym").data, expected)

    @pytest.mark.parametrize(
        "date",
        (
            pd.Timestamp("2023-01-01"),  # Before first value
            pd.Timestamp("2024-01-01 10:00:00"),  # After first value and before second value
            pd.Timestamp("2024-01-02 10:00:00"),  # After second value
        ),
    )
    def test_merge_insert_in_full_segment_creates_new_segment(self, lmdb_library_factory, date, monkeypatch):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=2, columns_per_segment=2))
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3)
        )
        lib.write("sym", target)
        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 4

        source = pd.DataFrame({"a": [10], "b": [20.0], "c": ["A"]}, index=pd.DatetimeIndex([date]))
        monkeypatch.setattr(lib.__class__, "merge_experimental", lambda *args, **kwargs: None, raising=False)
        lib.merge_experimental("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
        expected = pd.concat([target, source]).sort_index()
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

        lt = lib._dev_tools.library_tool()
        # The first segment is changed
        # A new segment is created because after the insert the first segment overflows the rows_per_segment setting
        # Each new segment is column sliced -> 2 * 2 = 4 new data keys
        monkeypatch.setattr(
            lt,
            "find_keys_for_symbol",
            mock_find_keys_for_symbol({KeyType.TABLE_DATA: 8, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}),
        )
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 8
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_writes_new_version_even_if_nothing_is_changed(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)
        source = pd.DataFrame(
            {"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]}, index=pd.date_range("2024-01-01", periods=3)
        )
        merge_vit = lib.merge_experimental(
            "sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT)
        )
        assert merge_vit.version == 1

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(read_vit, merge_vit)
        assert_frame_equal(read_vit.data, target)

        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_index_and_column(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0]},
            index=pd.DatetimeIndex(
                [
                    "2024-01-01",  # Matches index and column
                    "2024-01-02 01:00:00",  # Matches column, but not index
                    "2024-01-03",  # Matches index, but not column
                    "2024-01-04",  # Does not match either
                ]
            ),
        )
        lib.merge_experimental(
            "sym", source, on=["a"], strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT)
        )

        # The first row is matched, but the strategy for matched rows is DO_NOTHING, so no update
        # the rest rows are inserted
        expected = pd.concat([target, source.tail(len(source) - 1)]).sort_index()
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_on_multiple_columns(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {
                "a": [1, 2, 3, 4],
                "b": ["a", "b", "c", "d"],
                "c": ["A", "B", "A", "C"],
                "d": [10.1, 20.2, 30.3, 40.4],
                "e": [100, 200, 300, 400],
            },
            index=pd.date_range("2024-01-01", periods=4),
        )
        lib.write("sym", target)

        source = pd.DataFrame(
            {
                "a": [10, 20, 30, 40, 50],
                "b": ["a", "b", "c", "d", "e"],
                "c": ["A", "D", "A", "B", "C"],
                "d": [10.1, 50.5, 30.3, 40.4, 70.7],
                "e": [100, 500, 600, 400, 800],
            },
            index=pd.DatetimeIndex(
                [
                    "2024-01-01",  # MATCH: index + b + d + e match
                    "2024-01-02",  # NOT_MATCH: index + b match, but d + e differ
                    "2024-01-03",  # NOT_MATCH: index + b + d match, e differs
                    "2024-01-05",  # NOT_MATCH: new index, b+d+e match
                    "2024-01-06",  # NOT_MATCH: new index, new b, new d, new e
                ]
            ),
        )

        lib.merge_experimental(
            "sym", source, on=["b", "d", "e"], strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT)
        )
        expected = pd.concat([target, source.tail(len(source) - 1)]).sort_index()
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_does_not_throw_when_target_row_is_matched_more_than_once_when_matched_is_do_nothing(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 40], "b": [1.0, 2.0, 40.0]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),  # Matches first row of target
                    pd.Timestamp("2024-01-01"),  # Also matches first row of target
                    pd.Timestamp("2024-01-02 00:00:01"),
                ]
            ),
        )

        lib.merge_experimental("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
        expected = pd.DataFrame(
            {
                "a": [
                    1,
                    2,
                    40,
                    3,
                ],
                "b": [1.0, 2.0, 40.0, 3.0],
            },
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-01-02 00:00:01"),
                    pd.Timestamp("2024-01-03"),
                ]
            ),
        )
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_target_is_empty(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": np.array([], dtype=np.int64)}, index=pd.DatetimeIndex([]))
        write_vit = lib.write("sym", target, metadata={"meta": "data"})
        assert write_vit.version == 0
        source = pd.DataFrame({"a": np.array([1, 2], dtype=np.int64)}, index=pd.date_range("2024-01-01", periods=2))
        merge_vit = lib.merge_experimental(
            "sym", source, strategy=MergeStrategy("do_nothing", "insert"), metadata={"new_meta": "new_data"}
        )
        assert merge_vit.version == 1
        expected = source
        read_vit = lib.read("sym")
        assert read_vit.metadata == {"new_meta": "new_data"}
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)

    def test_insert_within_single_segment(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5), pd.Timestamp(10)]),
        )
        lib.write("sym", target)
        source = pd.DataFrame(
            {"a": [100, 200, 300, 400], "b": [100.0, 200.0, 300.0, 400.0]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(2),
                    pd.Timestamp(2),
                    pd.Timestamp(6),
                    pd.Timestamp(9),
                ]
            ),
        )
        lib.merge_experimental("sym", source, self.strategy)
        expected = pd.concat([target, source]).sort_index()
        assert_frame_equal(lib.read("sym").data, expected)

    @pytest.mark.parametrize(
        "source,expected",
        [
            pytest.param(
                pd.DataFrame(
                    {"a": [0, 1, 100], "b": [100.0, 200.0, 300.0]},
                    index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(5)]),
                ),
                pd.DataFrame(
                    {"a": [0, 1, 2, 3, 4, 5, 100, 6], "b": [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 300.0, 6.0]},
                    index=pd.DatetimeIndex(
                        [
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(6),
                        ]
                    ),
                ),
                id="Match_first_equal_run_Insert_in_second_equal_run.",
            ),
            pytest.param(
                pd.DataFrame(
                    {"a": [0, 100, 3], "b": [100.0, 200.0, 300.0]},
                    index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(5)]),
                ),
                pd.DataFrame(
                    {"a": [0, 1, 100, 2, 3, 4, 5, 6], "b": [0.0, 1.0, 200, 2.0, 3.0, 4.0, 5.0, 6.0]},
                    index=pd.DatetimeIndex(
                        [
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(6),
                        ]
                    ),
                ),
                id="Match_in_second_equal_run_Insert_in_first.",
            ),
            pytest.param(
                pd.DataFrame(
                    {"a": [0, 100, 1, 2, 5, 200, 4, 3], "b": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0]},
                    index=pd.DatetimeIndex(
                        [
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                        ]
                    ),
                ),
                pd.DataFrame(
                    {
                        "a": [0, 1, 100, 2, 3, 4, 5, 200, 6],
                        "b": [0.0, 1.0, 200.0, 2.0, 3.0, 4.0, 5.0, 600.0, 6.0],
                    },
                    index=pd.DatetimeIndex(
                        [
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(6),
                        ]
                    ),
                ),
                id="Match_in_both_equal_index_runs_and_insert_in_both.",
            ),
        ],
    )
    def test_within_single_segment_match_on_column(self, lmdb_library, source, expected):
        lib = lmdb_library
        index = pd.DatetimeIndex(
            [
                pd.Timestamp(0),
                pd.Timestamp(0),
                pd.Timestamp(5),
                pd.Timestamp(5),
                pd.Timestamp(5),
                pd.Timestamp(5),
                pd.Timestamp(6),
            ]
        )
        target = pd.DataFrame({"a": range(len(index)), "b": np.linspace(0, len(index) - 1, len(index))}, index=index)
        lib.write("sym", target)
        lib.merge_experimental("sym", source, self.strategy, on=["a"])
        assert_frame_equal(lib.read("sym").data, expected)

    @pytest.mark.parametrize(
        "source",
        [
            pd.DataFrame({"a": [10], "b": [10.0]}, index=pd.DatetimeIndex([pd.Timestamp(0)])),
            pd.DataFrame({"a": [10], "b": [10.0]}, index=pd.DatetimeIndex([pd.Timestamp(5)])),
            pd.DataFrame({"a": [10], "b": [10.0]}, index=pd.DatetimeIndex([pd.Timestamp(10)])),
            pd.DataFrame(
                {"a": [10, 20], "b": [10.0, 20.0]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5)])
            ),
            pd.DataFrame(
                {"a": [10, 20], "b": [10.0, 20.0]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(10)])
            ),
            pd.DataFrame(
                {"a": [10, 20], "b": [10.0, 20.0]}, index=pd.DatetimeIndex([pd.Timestamp(5), pd.Timestamp(10)])
            ),
            pd.DataFrame(
                {"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]},
                index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5), pd.Timestamp(10)]),
            ),
        ],
    )
    def test_within_single_segment_matched_rows_stay_the_same(self, lmdb_library, source):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5), pd.Timestamp(10)]),
        )
        lib.write("sym", target)
        lib.merge_experimental("sym", source, self.strategy)
        assert_frame_equal(lib.read("sym").data, target)

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param(
                pd.DataFrame({"a": [10], "b": [20.0], "c": [True]}, index=pd.DatetimeIndex([pd.Timestamp(2)])),
                id="Insert_single_row_in_first_segment",
            ),
            pytest.param(
                pd.DataFrame({"a": [10], "b": [20.0], "c": [True]}, index=pd.DatetimeIndex([pd.Timestamp(21)])),
                id="Insert_single_row_in_second_segment",
            ),
            pytest.param(
                pd.DataFrame(
                    {"a": [10, 100], "b": [20.0, 200.0], "c": [False, True]},
                    index=pd.DatetimeIndex([pd.Timestamp(6), pd.Timestamp(26)]),
                ),
                id="Insert_single_value_in_both_segments",
            ),
            pytest.param(
                pd.DataFrame(
                    {
                        "a": [10, 20, 30, 40, 50, 1000, 2000],
                        "b": [60.0, 70.0, 80.0, 90.0, 100.0, 3000, 4000],
                        "c": [True, False, False, True, True, True, False],
                    },
                    index=pd.DatetimeIndex(
                        [
                            pd.Timestamp(1),
                            pd.Timestamp(2),
                            pd.Timestamp(3),
                            pd.Timestamp(7),
                            pd.Timestamp(9),
                            pd.Timestamp(21),
                            pd.Timestamp(26),
                        ]
                    ),
                ),
                id="Insert_multiple_values_in_both_segments",
            ),
        ],
    )
    def test_within_two_segments(self, lmdb_library, source):
        lib = lmdb_library
        seg0 = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": [True, False, True]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5), pd.Timestamp(10)]),
        )
        lib.write("sym", seg0)
        seg1 = pd.DataFrame(
            {"a": [4, 5, 6], "b": [4.0, 5.0, 6.0], "c": [False, True, False]},
            index=pd.DatetimeIndex([pd.Timestamp(20), pd.Timestamp(25), pd.Timestamp(30)]),
        )
        lib.append("sym", seg1)
        expected = pd.concat([seg0, seg1, source]).sort_index()
        lib.merge_experimental("sym", source, self.strategy)
        assert_frame_equal(lib.read("sym").data, expected)

    @pytest.mark.parametrize(
        "target",
        [
            [pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(5)]))],
            [
                pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(5)])),
                pd.DataFrame({"a": [10]}, index=pd.DatetimeIndex([pd.Timestamp(10)])),
            ],
        ],
    )
    def test_insert_before_first_segment_start(self, in_memory_store_factory, target):
        lib = in_memory_store_factory()
        for tgt in target:
            lib.append("sym", tgt)
        source = pd.DataFrame(
            {"a": [100, 200, 300]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(1)])
        )
        qs.reset_stats()
        with qs.query_stats():
            lib.merge_experimental("sym", source, self.strategy)
        stats = qs.get_query_stats()
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_INDEX") == 1
        index_key = lib.read_index("sym")
        assert index_key.index[0] == source.index[0]
        assert index_key["end_index"][0] == pd.Timestamp(6)
        assert_frame_equal(lib.read("sym").data, pd.concat(target + [source]).sort_index())

    @pytest.mark.parametrize(
        "target",
        [
            [pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(5)]))],
            [
                pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(5)])),
                pd.DataFrame({"a": [10]}, index=pd.DatetimeIndex([pd.Timestamp(10)])),
            ],
        ],
    )
    def test_insert_after_last_segment_end(self, in_memory_store_factory, target):
        lib = in_memory_store_factory()
        for tgt in target:
            lib.append("sym", tgt)
        source = pd.DataFrame(
            {"a": [100, 200, 300]}, index=pd.DatetimeIndex([pd.Timestamp(15), pd.Timestamp(15), pd.Timestamp(16)])
        )
        qs.reset_stats()
        with qs.query_stats():
            lib.merge_experimental("sym", source, self.strategy)
        stats = qs.get_query_stats()
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_INDEX") == 1
        index_key = lib.read_index("sym")
        assert index_key.index[-1] == target[-1].index[0]
        assert index_key["end_index"][-1] == pd.Timestamp(17)
        assert_frame_equal(lib.read("sym").data, pd.concat(target + [source]).sort_index())

    @pytest.mark.parametrize(
        "target",
        [
            [pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(5)]))],
            [
                pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(5)])),
                pd.DataFrame({"a": [10]}, index=pd.DatetimeIndex([pd.Timestamp(10)])),
            ],
        ],
    )
    def test_insert_before_first_segment_start_and_after_last_segment_end(self, lmdb_library, target):
        lib = lmdb_library
        for tgt in target:
            lib.append("sym", tgt)
        source = pd.DataFrame(
            {"a": [100, 200, 300, 400, 500, 600]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(0),
                    pd.Timestamp(0),
                    pd.Timestamp(1),
                    pd.Timestamp(15),
                    pd.Timestamp(15),
                    pd.Timestamp(16),
                ]
            ),
        )
        lib.merge_experimental("sym", source, self.strategy)
        assert_frame_equal(lib.read("sym").data, pd.concat(target + [source]).sort_index())

    def test_insert_between_two_segments_appends_to_first(self, in_memory_store_factory):
        lib = in_memory_store_factory()
        seg0 = pd.DataFrame({"a": [1, 2]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5)]))
        lib.write("sym", seg0)
        seg1 = pd.DataFrame({"a": [3, 4]}, index=pd.DatetimeIndex([pd.Timestamp(10), pd.Timestamp(15)]))
        lib.append("sym", seg1)
        source = pd.DataFrame({"a": [100, 200]}, index=pd.DatetimeIndex([pd.Timestamp(6), pd.Timestamp(7)]))
        qs.reset_stats()
        with qs.query_stats():
            lib.merge_experimental("sym", source, self.strategy)
        stats = qs.get_query_stats()
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_INDEX") == 1
        assert_frame_equal(lib.read("sym").data, pd.concat([seg0, seg1, source]).sort_index())
        lt = lib.library_tool()
        segments = [lt.read_to_dataframe(key) for key in lt.dataframe_to_keys(lt.read_index("sym"), "sym")]
        segments[0].index.name = None
        segments[1].index.name = None
        assert_frame_equal(segments[0], pd.concat([seg0, source]).sort_index())
        assert_frame_equal(segments[1], seg1)


class TestMergeTimeseriesUpdateAndInsert:

    def setup_method(self):
        self.strategy = MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT)

    @pytest.mark.parametrize(
        "strategy",
        (None, MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT), MergeStrategy("update", "insert")),
    )
    def test_basic(self, lmdb_library, strategy):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3)
        )
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, 20, -2], "b": [-1.1, 20.1, -3.1], "c": ["a", "c", "d"]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01 15:00:00"), pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02 01:00:00")]
            ),
        )

        merge_vit = (
            lib.merge_experimental("sym", source, strategy=strategy)
            if strategy
            else lib.merge_experimental("sym", source)
        )
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        expected = pd.DataFrame(
            {"a": [1, -1, 20, -2, 3], "b": [1.0, -1.1, 20.1, -3.1, 3.0], "c": ["a", "a", "c", "d", "c"]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-01-01 15:00:00"),
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-01-02 01:00:00"),
                    pd.Timestamp("2024-01-03"),
                ]
            ),
        )

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(read_vit, merge_vit)
        assert_frame_equal(read_vit.data, expected)

        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize(
        "slicing_policy",
        [{"rows_per_segment": 2}, {"columns_per_segment": 2}, {"rows_per_segment": 2, "columns_per_segment": 2}],
    )
    def test_slicing(self, lmdb_library_factory, monkeypatch, slicing_policy):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(**slicing_policy))
        target = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5], "b": [1.0, 2.0, 3.0, 4.0, 5.0], "c": ["a", "b", "c", "d", "e"]},
            index=pd.date_range("2024-01-01", periods=5),
        )
        lib.write("sym", target)

        # Insertion pushes the row to be updated into a new segment
        source = pd.DataFrame(
            {"a": [-1, 40], "b": [-1.1, 40.4], "c": ["a", "f"]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-03 15:00:00"), pd.Timestamp("2024-01-04")]),
        )
        monkeypatch.setattr(lib.__class__, "merge_experimental", lambda *args, **kwargs: None, raising=False)
        lib.merge_experimental("sym", source)

        expected = pd.DataFrame(
            {"a": [1, 2, 3, -1, 40, 5], "b": [1.0, 2.0, 3.0, -1.1, 40.4, 5.0], "c": ["a", "b", "c", "a", "f", "e"]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-01-03"),
                    pd.Timestamp("2024-01-03 15:00:00"),
                    pd.Timestamp("2024-01-04"),
                    pd.Timestamp("2024-01-05"),
                ]
            ),
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        assert_frame_equal(lib.read("sym").data, expected)

        lt = lib._dev_tools.library_tool()
        if "rows_per_segment" in slicing_policy and "columns_per_segment" in slicing_policy:
            expected_data_keys = 10
        elif "rows_per_segment" in slicing_policy:
            expected_data_keys = 5
        elif "columns_per_segment" in slicing_policy:
            expected_data_keys = 4
        monkeypatch.setattr(
            lt,
            "find_keys_for_symbol",
            mock_find_keys_for_symbol(
                {KeyType.TABLE_DATA: expected_data_keys, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}
            ),
        )
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == expected_data_keys
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize(
        "source",
        (
            pd.DataFrame([], index=pd.DatetimeIndex([])),
            pd.DataFrame({"a": []}, index=pd.DatetimeIndex([])),
            pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])),
        ),
    )
    @pytest.mark.parametrize("upsert", (True, False))
    @pytest.mark.parametrize("strategy", (MergeStrategy("update", "insert"), MergeStrategy("do_nothing", "insert")))
    @pytest.mark.parametrize("metadata", (None, {"meta": "data"}))
    def test_target_symbol_does_not_exist(self, lmdb_library, source, upsert, strategy, metadata):
        # Model non-existing target after Library.update
        # There is an upsert parameter to control whether to create the index. If upsert=False exception is thrown.
        # Since we're doing a merge on non-existing data, I think it's logical to assume that nothing matches. No
        # updates are performed. The insert operation will insert the data into the newly created target symbol.
        lib = lmdb_library

        if not upsert:
            with pytest.raises(StorageException):
                lib.merge_experimental("sym", source, strategy=strategy, upsert=upsert, metadata=metadata)
            assert not lib.has_symbol("sym")
        else:
            merge_vit = lib.merge_experimental("sym", source, strategy=strategy, upsert=upsert, metadata=metadata)
            assert merge_vit.version == 0
            assert merge_vit.metadata == metadata
            assert lib.list_symbols() == ["sym"]
            read_vit = lib.read("sym")
            assert_vit_equals_except_data(merge_vit, read_vit)
            assert_frame_equal(read_vit.data, source)

    def test_upsert_with_existing_symbol(self, lmdb_library):
        # When the symbol exists upsert is irrelevant and a regular merge is performed.
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)
        source = pd.DataFrame(
            {"a": [20, 40]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-04")])
        )
        merge_vit = lib.merge_experimental("sym", source, strategy=self.strategy, upsert=True)
        assert merge_vit.version == 1
        expected = pd.DataFrame(
            {"a": [1, 20, 3, 40]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-01-03"),
                    pd.Timestamp("2024-01-04"),
                ]
            ),
        )
        assert_frame_equal(lib.read("sym").data, expected)

    def test_upsert_recreates_symbol_after_delete(self, lmdb_library):
        # Recreating a deleted symbol continues the version counter and adds a symbol list key.
        lib = lmdb_library
        lib.write("sym", pd.DataFrame({"a": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3)))
        lib.delete("sym")
        lib_tool = lib._dev_tools.library_tool()
        assert not len(lib.list_symbols())
        num_symbol_list_keys = len(lib_tool.find_keys(KeyType.SYMBOL_LIST))
        source = pd.DataFrame({"a": [10]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-05")]))
        merge_vit = lib.merge_experimental("sym", source, strategy=self.strategy, upsert=True)
        assert merge_vit.version == 1
        assert_frame_equal(lib.read("sym").data, source)
        assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == num_symbol_list_keys + 1
        assert lib.list_symbols() == ["sym"]

    def test_on_index_and_column(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3)
        )
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0], "c": ["A", "B", "A", "C"]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),  # Matches index and column
                    pd.Timestamp("2024-01-02 01:00:00"),  # Matches column, but not index
                    pd.Timestamp("2024-01-03"),  # Matches index, but not column
                    pd.Timestamp("2024-01-04"),  # Does not match either
                ]
            ),
        )
        lib.merge_experimental("sym", source, on=["a"])

        expected = pd.DataFrame(
            {"a": [1, 2, 2, 3, 30, 40], "b": [10.0, 2.0, 20.0, 3.0, 30.0, 40.0], "c": ["A", "b", "B", "c", "A", "C"]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-01-02 01:00:00"),
                    pd.Timestamp("2024-01-03"),
                    pd.Timestamp("2024-01-03"),
                    pd.Timestamp("2024-01-04"),
                ]
            ),
        )
        received = lib.read("sym").data

        assert_frame_equal(received, expected)

    def test_on_multiple_columns(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {
                "a": [1, 2, 3, 4],
                "b": ["a", "b", "c", "d"],
                "c": ["A", "B", "A", "C"],
                "d": [10.1, 20.2, 30.3, 40.4],
                "e": [100, 200, 300, 400],
            },
            index=pd.date_range("2024-01-01", periods=4),
        )
        lib.write("sym", target)

        source = pd.DataFrame(
            {
                "a": [10, 20, 30, 40, 50],
                "b": ["a", "b", "c", "d", "e"],
                "c": ["A", "D", "A", "B", "C"],
                "d": [10.1, 50.5, 30.3, 40.4, 70.7],
                "e": [100, 500, 600, 400, 800],
            },
            index=pd.DatetimeIndex(
                [
                    "2024-01-01",  # index + b + d + e match (update)
                    "2024-01-02",  # index + b match, but d + e differ (insert)
                    "2024-01-03",  # index + b + d match, e differs (insert)
                    "2024-01-05",  # new index, b+d+e match (insert)
                    "2024-01-06",  # new index, new b, new d, new e (insert)
                ]
            ),
        )

        lib.merge_experimental("sym", source, on=["b", "d", "e"])
        expected = pd.DataFrame(
            {
                "a": [10, 2, 20, 3, 30, 4, 40, 50],
                "b": ["a", "b", "b", "c", "c", "d", "d", "e"],
                "c": ["A", "B", "D", "A", "A", "C", "B", "C"],
                "d": [10.1, 20.2, 50.5, 30.3, 30.3, 40.4, 40.4, 70.7],
                "e": [100, 200, 500, 300, 600, 400, 400, 800],
            },
            index=pd.DatetimeIndex(
                [
                    "2024-01-01",  # Updated: matched on b="a", d=10.1, e=100
                    "2024-01-02",  # Original target row
                    "2024-01-02",  # Inserted: no match for b="b", d=50.5, e=500
                    "2024-01-03",  # Original target row
                    "2024-01-03",  # Inserted: no match for b="c", d=30.3, e=600
                    "2024-01-04",  # Original target row
                    "2024-01-05",  # Inserted: no match for b="d", d=40.4, e=400 (different index)
                    "2024-01-06",  # Inserted: completely new b="e", d=70.7, e=800
                ]
            ),
        )
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_target_is_empty(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": np.array([], dtype=np.int64)}, index=pd.DatetimeIndex([]))
        write_vit = lib.write("sym", target)
        source = pd.DataFrame({"a": np.array([1, 2], dtype=np.int64)}, index=pd.date_range("2024-01-01", periods=2))
        merge_vit = lib.merge_experimental("sym", source)
        expected = source
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param(
                pd.DataFrame(
                    {"a": [10, 20], "b": [10.0, 20.0]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0)])
                ),
                id="No_unmatched",
            ),
            pytest.param(
                pd.DataFrame(
                    {"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]},
                    index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(2)]),
                ),
                id="Contains_unmatched",
            ),
        ],
    )
    def test_throws_when_target_row_is_matched_more_than_once(self, lmdb_library, source):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5), pd.Timestamp(10)]),
        )
        lib.write("sym", target)
        with pytest.raises(UserInputException):
            lib.merge_experimental("sym", source, strategy=self.strategy)

    @pytest.mark.parametrize(
        "source,expected",
        [
            pytest.param(
                pd.DataFrame(
                    {"a": [0, 1, 100], "b": [100.0, 200.0, 300.0]},
                    index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(5)]),
                ),
                pd.DataFrame(
                    {"a": [0, 1, 2, 3, 4, 5, 100, 6], "b": [100.0, 200.0, 2.0, 3.0, 4.0, 5.0, 300.0, 6.0]},
                    index=pd.DatetimeIndex(
                        [
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(6),
                        ]
                    ),
                ),
                id="Match_first_equal_run_Insert_in_second_equal_run",
            ),
            pytest.param(
                pd.DataFrame(
                    {"a": [0, 100, 3], "b": [100.0, 200.0, 300.0]},
                    index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(5)]),
                ),
                pd.DataFrame(
                    {"a": [0, 1, 100, 2, 3, 4, 5, 6], "b": [100.0, 1.0, 200, 2.0, 300.0, 4.0, 5.0, 6.0]},
                    index=pd.DatetimeIndex(
                        [
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(6),
                        ]
                    ),
                ),
                id="Match_in_second_equal_run_Insert_in_first",
            ),
            pytest.param(
                pd.DataFrame(
                    {"a": [0, 100, 1, 2, 5, 200, 4, 3], "b": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0]},
                    index=pd.DatetimeIndex(
                        [
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                        ]
                    ),
                ),
                pd.DataFrame(
                    {
                        "a": [0, 1, 100, 2, 3, 4, 5, 200, 6],
                        "b": [100.0, 300.0, 200.0, 400.0, 800.0, 700.0, 500.0, 600.0, 6.0],
                    },
                    index=pd.DatetimeIndex(
                        [
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(0),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(5),
                            pd.Timestamp(6),
                        ]
                    ),
                ),
                id="Match_in_both_equal_index_runs_and_insert_in_both",
            ),
        ],
    )
    def test_within_single_segment_match_on_column(self, lmdb_library, source, expected):
        lib = lmdb_library
        index = pd.DatetimeIndex(
            [
                pd.Timestamp(0),
                pd.Timestamp(0),
                pd.Timestamp(5),
                pd.Timestamp(5),
                pd.Timestamp(5),
                pd.Timestamp(5),
                pd.Timestamp(6),
            ]
        )
        target = pd.DataFrame({"a": range(len(index)), "b": np.linspace(0, len(index) - 1, len(index))}, index=index)
        lib.write("sym", target)
        lib.merge_experimental("sym", source, self.strategy, on=["a"])
        assert_frame_equal(lib.read("sym").data, expected)

    def test_insert_before_first_segment_start_and_update_first_value(self, lmdb_library):
        lib = lmdb_library
        lib.write("sym", pd.DataFrame({"a": [1, 2]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5)])))
        source = pd.DataFrame({"a": [10, 20]}, index=pd.DatetimeIndex([pd.Timestamp(-5), pd.Timestamp(0)]))
        lib.merge_experimental("sym", source, self.strategy)
        expected = pd.DataFrame(
            {"a": [10, 20, 2]}, index=pd.DatetimeIndex([pd.Timestamp(-5), pd.Timestamp(0), pd.Timestamp(5)])
        )
        assert_frame_equal(lib.read("sym").data, expected)

    def test_insert_before_first_segment_start_and_update_first_value_on(self, lmdb_library):
        lib = lmdb_library
        lib.write("sym", pd.DataFrame({"a": [1, 2]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5)])))
        source = pd.DataFrame({"a": [10, 20]}, index=pd.DatetimeIndex([pd.Timestamp(-5), pd.Timestamp(0)]))
        lib.merge_experimental("sym", source, self.strategy, on=["a"])
        expected = pd.DataFrame(
            {"a": [10, 1, 20, 2]},
            index=pd.DatetimeIndex([pd.Timestamp(-5), pd.Timestamp(0), pd.Timestamp(0), pd.Timestamp(5)]),
        )
        assert_frame_equal(lib.read("sym").data, expected)

    def test_insert_after_last_and_match_last(self, lmdb_library):
        lib = lmdb_library
        lib.write("sym", pd.DataFrame({"a": [1, 2]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5)])))
        source = pd.DataFrame({"a": [10, 20]}, index=pd.DatetimeIndex([pd.Timestamp(5), pd.Timestamp(10)]))
        lib.merge_experimental("sym", source, self.strategy)
        expected = pd.DataFrame(
            {"a": [1, 10, 20]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5), pd.Timestamp(10)])
        )
        assert_frame_equal(lib.read("sym").data, expected)

    def test_insert_after_last_and_match_last_on(self, lmdb_library):
        lib = lmdb_library
        lib.write("sym", pd.DataFrame({"a": [1, 2]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5)])))
        source = pd.DataFrame({"a": [10, 20]}, index=pd.DatetimeIndex([pd.Timestamp(5), pd.Timestamp(10)]))
        lib.merge_experimental("sym", source, self.strategy, on=["a"])
        expected = pd.DataFrame(
            {"a": [1, 2, 10, 20]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(5), pd.Timestamp(5), pd.Timestamp(10)]),
        )
        assert_frame_equal(lib.read("sym").data, expected)


class TestMergeUpdateInsertIndexSpansMultipleSegments:
    """End-to-end translation of the C++ fixture ``MergeUpdateInsertIndexSpansMultipleSegments``
    (cpp/arcticdb/processing/test/test_merge_update.cpp): a single index value spans a segment boundary. The C++ tests
    assert on the internal processing-unit output; here we only check the final read result of an update+insert merge.
    The fixture's layout is reproduced with rows_per_segment=5 and columns_per_segment=1 (matching cols_per_segment=1).
    """

    strategy = MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT)

    def _run(self, lmdb_library_factory, target, source, expected, on):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=5, columns_per_segment=1))
        lib.write("sym", target)
        lib.merge_experimental("sym", source, strategy=self.strategy, on=on)
        assert_frame_equal(lib.read("sym").data, expected)

    def test_last_index_value_same_as_next_segment_first_two_overlapping_segments(self, lmdb_library_factory):
        target = pd.DataFrame(
            {"a": np.arange(27, dtype=np.int64), "b": np.arange(27, dtype=np.int32)},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(v)
                    for v in [
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        9,
                        9,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        19,
                        20,
                        21,
                        22,
                        23,
                        24,
                    ]
                ]
            ),
        )
        source = pd.DataFrame(
            {"a": np.array([10, 8, 120], dtype=np.int64), "b": np.array([100, 200, 300], dtype=np.int32)},
            index=pd.DatetimeIndex([pd.Timestamp(v) for v in [9, 9, 9]]),
        )
        expected = pd.DataFrame(
            {
                "a": np.array(
                    [
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        11,
                        120,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        19,
                        20,
                        21,
                        22,
                        23,
                        24,
                        25,
                        26,
                    ],
                    dtype=np.int64,
                ),
                "b": np.array(
                    [
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        200,
                        9,
                        100,
                        11,
                        300,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        19,
                        20,
                        21,
                        22,
                        23,
                        24,
                        25,
                        26,
                    ],
                    dtype=np.int32,
                ),
            },
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(v)
                    for v in [
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        9,
                        9,
                        9,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        19,
                        20,
                        21,
                        22,
                        23,
                        24,
                    ]
                ]
            ),
        )
        self._run(lmdb_library_factory, target, source, expected, on=["a"])

    def test_last_index_value_same_as_next_segment_first_three_overlapping_segments(self, lmdb_library_factory):
        target = pd.DataFrame(
            {"a": np.arange(27, dtype=np.int64), "b": np.arange(27, dtype=np.int32)},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(v)
                    for v in [1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
                ]
            ),
        )
        source = pd.DataFrame(
            {
                "a": np.array([100, 10, 8, 120, 15, 17, 100], dtype=np.int64),
                "b": np.array([100, 200, 300, 400, 500, 600, 700], dtype=np.int32),
            },
            index=pd.DatetimeIndex([pd.Timestamp(v) for v in [8, 9, 9, 9, 9, 10, 11]]),
        )
        expected = pd.DataFrame(
            {
                "a": np.array(
                    [
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        100,
                        8,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        120,
                        17,
                        18,
                        100,
                        19,
                        20,
                        21,
                        22,
                        23,
                        24,
                        25,
                        26,
                    ],
                    dtype=np.int64,
                ),
                "b": np.array(
                    [
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        100,
                        300,
                        9,
                        200,
                        11,
                        12,
                        13,
                        14,
                        500,
                        16,
                        400,
                        600,
                        18,
                        700,
                        19,
                        20,
                        21,
                        22,
                        23,
                        24,
                        25,
                        26,
                    ],
                    dtype=np.int32,
                ),
            },
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(v)
                    for v in [
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        8,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        10,
                        11,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        19,
                    ]
                ]
            ),
        )
        self._run(lmdb_library_factory, target, source, expected, on=["a"])

    def test_two_groups_of_segments_with_matching_last_index_value(self, lmdb_library_factory):
        target = pd.DataFrame(
            {"a": np.arange(27, dtype=np.int64), "b": np.arange(27, dtype=np.int32)},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(v)
                    for v in [1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10, 10, 11, 12, 13, 14]
                ]
            ),
        )
        source = pd.DataFrame(
            {
                "a": np.array([100, 11, 16, 13, 100, 8, 200, 19, 100, 17, 20, 300, 400, 23, 500], dtype=np.int64),
                "b": np.array([i * 100 for i in range(15)], dtype=np.int32),
            },
            index=pd.DatetimeIndex([pd.Timestamp(v) for v in [8, 9, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10, 11, 11, 13]]),
        )
        expected = pd.DataFrame(
            {
                "a": np.array(
                    [
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        100,
                        8,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        100,
                        200,
                        17,
                        18,
                        19,
                        20,
                        21,
                        22,
                        100,
                        300,
                        23,
                        400,
                        24,
                        25,
                        500,
                        26,
                    ],
                    dtype=np.int64,
                ),
                "b": np.array(
                    [
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        0,
                        500,
                        9,
                        10,
                        100,
                        12,
                        300,
                        14,
                        15,
                        200,
                        400,
                        600,
                        900,
                        18,
                        700,
                        1000,
                        21,
                        22,
                        800,
                        1100,
                        1300,
                        1200,
                        24,
                        25,
                        1400,
                        26,
                    ],
                    dtype=np.int32,
                ),
            },
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(v)
                    for v in [
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        8,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        9,
                        10,
                        10,
                        10,
                        10,
                        10,
                        10,
                        10,
                        10,
                        11,
                        11,
                        12,
                        13,
                        13,
                        14,
                    ]
                ]
            ),
        )
        self._run(lmdb_library_factory, target, source, expected, on=["a"])


class TestMergeUpdateInsertIndexSpansMultipleSegmentsChain:
    """End-to-end translation of the C++ fixture ``MergeUpdateInsertIndexSpansMultipleSegmentsChain``
    (cpp/arcticdb/processing/test/test_merge_update.cpp): the same index value chains across several segments. The C++
    tests assert on the internal processing-unit output; here we only check the final read result of an update+insert
    merge. The fixture's layout is reproduced with rows_per_segment=3 and columns_per_segment=1 (matching
    cols_per_segment=1). Shared target index [0,1,2,4,5,6,6,6,6,6,10,11,11,13,15,15,16,17,17,19,20], a=b=0..20.
    """

    strategy = MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT)

    _CHAIN_INDEX = [0, 1, 2, 4, 5, 6, 6, 6, 6, 6, 10, 11, 11, 13, 15, 15, 16, 17, 17, 19, 20]

    def _chain_target(self):
        return pd.DataFrame(
            {"a": np.arange(21, dtype=np.int64), "b": np.arange(21, dtype=np.int32)},
            index=pd.DatetimeIndex([pd.Timestamp(v) for v in self._CHAIN_INDEX]),
        )

    def _run(self, lmdb_library_factory, source, expected, on):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=3, columns_per_segment=1))
        lib.write("sym", self._chain_target())
        lib.merge_experimental("sym", source, strategy=self.strategy, on=on)
        assert_frame_equal(lib.read("sym").data, expected)

    def test_source_in_row_slice_0(self, lmdb_library_factory):
        source = pd.DataFrame(
            {"a": np.array([0, 10], dtype=np.int64), "b": np.array([100, 200], dtype=np.int32)},
            index=pd.DatetimeIndex([pd.Timestamp(v) for v in [0, 2]]),
        )
        expected = pd.DataFrame(
            {
                "a": np.array(
                    [0, 1, 2, 10, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], dtype=np.int64
                ),
                "b": np.array(
                    [100, 1, 2, 200, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], dtype=np.int32
                ),
            },
            index=pd.DatetimeIndex(
                [pd.Timestamp(v) for v in [0, 1, 2, 2, 4, 5, 6, 6, 6, 6, 6, 10, 11, 11, 13, 15, 15, 16, 17, 17, 19, 20]]
            ),
        )
        self._run(lmdb_library_factory, source, expected, on=["a"])

    def test_source_in_row_slice_1_value_is_not_in_multiple_segments(self, lmdb_library_factory):
        source = pd.DataFrame(
            {"a": np.array([4], dtype=np.int64), "b": np.array([100], dtype=np.int32)},
            index=pd.DatetimeIndex([pd.Timestamp(5)]),
        )
        expected = pd.DataFrame(
            {
                "a": np.arange(21, dtype=np.int64),
                "b": np.array(
                    [0, 1, 2, 3, 100, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], dtype=np.int32
                ),
            },
            index=pd.DatetimeIndex([pd.Timestamp(v) for v in self._CHAIN_INDEX]),
        )
        self._run(lmdb_library_factory, source, expected, on=["a"])

    def test_source_in_row_slice_1_value_is_in_multiple_segments(self, lmdb_library_factory):
        source = pd.DataFrame(
            {"a": np.array([9, 5, 6, 100], dtype=np.int64), "b": np.array([100, 200, 300, 400], dtype=np.int32)},
            index=pd.DatetimeIndex([pd.Timestamp(v) for v in [6, 6, 6, 6]]),
        )
        expected = pd.DataFrame(
            {
                "a": np.array(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], dtype=np.int64
                ),
                "b": np.array(
                    [0, 1, 2, 3, 4, 200, 300, 7, 8, 100, 400, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
                    dtype=np.int32,
                ),
            },
            index=pd.DatetimeIndex(
                [pd.Timestamp(v) for v in [0, 1, 2, 4, 5, 6, 6, 6, 6, 6, 6, 10, 11, 11, 13, 15, 15, 16, 17, 17, 19, 20]]
            ),
        )
        self._run(lmdb_library_factory, source, expected, on=["a"])

    def test_source_in_row_slice_3(self, lmdb_library_factory):
        source = pd.DataFrame(
            {"a": np.array([100], dtype=np.int64), "b": np.array([100], dtype=np.int32)},
            index=pd.DatetimeIndex([pd.Timestamp(11)]),
        )
        expected = pd.DataFrame(
            {
                "a": np.array(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 100, 13, 14, 15, 16, 17, 18, 19, 20], dtype=np.int64
                ),
                "b": np.array(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 100, 13, 14, 15, 16, 17, 18, 19, 20], dtype=np.int32
                ),
            },
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(v)
                    for v in [0, 1, 2, 4, 5, 6, 6, 6, 6, 6, 10, 11, 11, 11, 13, 15, 15, 16, 17, 17, 19, 20]
                ]
            ),
        )
        self._run(lmdb_library_factory, source, expected, on=["a"])

    def test_source_in_row_slice_5(self, lmdb_library_factory):
        source = pd.DataFrame(
            {"a": np.array([100], dtype=np.int64), "b": np.array([100], dtype=np.int32)},
            index=pd.DatetimeIndex([pd.Timestamp(15)]),
        )
        expected = pd.DataFrame(
            {
                "a": np.array(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 100, 16, 17, 18, 19, 20], dtype=np.int64
                ),
                "b": np.array(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 100, 16, 17, 18, 19, 20], dtype=np.int32
                ),
            },
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp(v)
                    for v in [0, 1, 2, 4, 5, 6, 6, 6, 6, 6, 10, 11, 11, 13, 15, 15, 15, 16, 17, 17, 19, 20]
                ]
            ),
        )
        self._run(lmdb_library_factory, source, expected, on=["a"])

    def test_insert_in_row_slice_3_without_column_matching(self, lmdb_library_factory):
        # on=None matches on the index only; index 7 is absent from the target so the row is inserted.
        source = pd.DataFrame(
            {"a": np.array([100], dtype=np.int64), "b": np.array([100], dtype=np.int32)},
            index=pd.DatetimeIndex([pd.Timestamp(7)]),
        )
        expected = pd.DataFrame(
            {
                "a": np.array(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], dtype=np.int64
                ),
                "b": np.array(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], dtype=np.int32
                ),
            },
            index=pd.DatetimeIndex(
                [pd.Timestamp(v) for v in [0, 1, 2, 4, 5, 6, 6, 6, 6, 6, 7, 10, 11, 11, 13, 15, 15, 16, 17, 17, 19, 20]]
            ),
        )
        self._run(lmdb_library_factory, source, expected, on=None)


@pytest.mark.parametrize(
    "strategy",
    (
        MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
        pytest.param(
            MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
            marks=pytest.mark.xfail(reason="Insert is not implemented"),
        ),
        pytest.param(
            MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
            marks=pytest.mark.xfail(reason="Insert is not implemented"),
        ),
    ),
)
class TestMergeRowrangeCommon:

    def test_merge_matched_update_with_metadata(self, lmdb_library, strategy):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        write_vit = lib.write("sym", target)

        source = pd.DataFrame({"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]})

        metadata = {"meta": "data"}

        merge_vit = lib.merge_experimental("sym", source, metadata=metadata, strategy=strategy, on=["a"])
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.metadata == metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)

        lt = lib._dev_tools.library_tool()

        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize("metadata", ({"meta": "data"}, None))
    def test_merge_writes_new_version_with_empty_source(self, lmdb_library, metadata, strategy):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        lib.write("sym", target)
        merge_vit = lib.merge_experimental("sym", pd.DataFrame(), metadata=metadata, strategy=strategy, on=["a"])
        assert merge_vit.metadata is None if metadata is None else merge_vit.metadata == metadata
        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

        read_vit = lib.read("sym")
        assert read_vit.version == 1
        assert read_vit.metadata is None if metadata is None else read_vit.metadata == metadata
        assert_frame_equal(read_vit.data, target)

    @pytest.mark.parametrize(
        "source",
        [
            # "a" has different type
            pd.DataFrame({"a": np.array([1, 2, 3], dtype=np.int8), "b": [1.0, 2.0, 3.0]}),
            # "a" is missing, replaced by "c"
            pd.DataFrame({"c": np.array([1, 2, 3], dtype=np.int8), "b": [1.0, 2.0, 3.0]}),
            # "a" is missing
            pd.DataFrame({"b": [1.0, 2.0, 3.0]}),
        ],
    )
    def test_static_schema_merge_throws_when_schemas_differ(self, lmdb_library, strategy, source):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        lib.write("sym", target)
        with pytest.raises(SchemaException):
            lib.merge_experimental("sym", source, strategy=strategy, on=["b"])

    def test_requires_on_column_none(self, lmdb_library, strategy):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2], "b": [1.0, 2.0]})
        lib.write("sym", target)
        source = pd.DataFrame({"a": [1], "b": [10.0]})
        with pytest.raises(UserInputException):
            lib.merge_experimental("sym", source, strategy=strategy, on=None)

    def test_requires_on_column_empty_list(self, lmdb_library, strategy):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2], "b": [1.0, 2.0]})
        lib.write("sym", target)
        source = pd.DataFrame({"a": [1], "b": [10.0]})
        with pytest.raises(UserInputException):
            lib.merge_experimental("sym", source, strategy=strategy, on=[])

    def test_on_nonexistent_column_raises(self, lmdb_library, strategy):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2], "b": [1.0, 2.0]})
        lib.write("sym", target)
        source = pd.DataFrame({"a": [1], "b": [10.0]})
        with pytest.raises(UserInputException):
            lib.merge_experimental("sym", source, strategy=strategy, on=["nonexistent"])


class TestMergeRowrangeUpdate:

    def setup_method(self):
        self.strategy = MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING)

    def test_basic(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["x", "y", "z"]})
        source = pd.DataFrame({"a": [1, 99, 3], "b": [10.0, 20.0, 30.0], "c": ["X", "Y", "Z"]})
        # a=1 matches row 0, a=99 no match, a=3 matches row 2
        expected = pd.DataFrame({"a": [1, 2, 3], "b": [10.0, 2.0, 30.0], "c": ["X", "y", "Z"]})
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    def test_no_matches(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        source = pd.DataFrame({"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]})
        # No a values match → no updates
        expected = target.copy()
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    def test_all_rows_match(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        source = pd.DataFrame({"a": [3, 1, 2], "b": [30.0, 10.0, 20.0]})
        # All a values match: 1→10.0, 2→20.0, 3→30.0
        expected = pd.DataFrame({"a": [1, 2, 3], "b": [10.0, 20.0, 30.0]})
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    @pytest.mark.parametrize(
        "target, source, expected",
        [
            (
                pd.DataFrame({"a": [1, 2, 3, 4], "b": ["x", "y", "z", "w"], "c": [10.0, 20.0, 30.0, 40.0]}),
                pd.DataFrame({"a": [1, 2, 99, 4], "b": ["x", "wrong", "z", "w"], "c": [99.0, 99.0, 99.0, 99.0]}),
                # (a=1,b="x") and (a=4,b="w") match; (a=2,b="wrong") and (a=99,b="z") don't
                pd.DataFrame({"a": [1, 2, 3, 4], "b": ["x", "y", "z", "w"], "c": [99.0, 20.0, 30.0, 99.0]}),
            ),
            (
                pd.DataFrame({"a": [1, 1, 2, 2], "b": ["x", "y", "x", "y"], "c": [10.0, 20.0, 30.0, 40.0]}),
                pd.DataFrame({"a": [1, 2, 1, 2], "b": ["x", "x", "y", "y"], "c": [99.0, 99.0, 99.0, 99.0]}),
                # All four (a,b) combos match → all rows updated
                pd.DataFrame({"a": [1, 1, 2, 2], "b": ["x", "y", "x", "y"], "c": [99.0, 99.0, 99.0, 99.0]}),
            ),
        ],
    )
    def test_multiple_on_columns(self, lmdb_library, target, source, expected):
        lib = lmdb_library
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a", "b"])

    def test_one_source_row_matches_multiple_target_rows(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 1, 2], "b": [10.0, 20.0, 30.0]})
        source = pd.DataFrame({"a": [1], "b": [99.0]})
        # a=1 matches rows 0 and 1
        expected = pd.DataFrame({"a": [1, 1, 2], "b": [99.0, 99.0, 30.0]})
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    @pytest.mark.parametrize(
        "slicing_policy",
        [{"rows_per_segment": 2}, {"columns_per_segment": 2}, {"rows_per_segment": 2, "columns_per_segment": 2}],
    )
    def test_row_and_column_slicing(self, lmdb_library_factory, slicing_policy):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(**slicing_policy))
        target = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5],
                "b": [1.0, 2.0, 3.0, 4.0, 5.0],
                "c": [True, False, True, False, True],
                "d": ["a", "b", "c", "d", "e"],
            }
        )
        source = pd.DataFrame(
            {
                "a": [3, 5],
                "b": [30.1, 50.1],
                "c": [False, False],
                "d": ["C", "E"],
            }
        )
        # a=3 matches row 2, a=5 matches row 4
        expected = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5],
                "b": [1.0, 2.0, 30.1, 4.0, 50.1],
                "c": [True, False, False, False, False],
                "d": ["a", "b", "C", "d", "E"],
            }
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    def test_match_on_float_nan(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        target = pd.DataFrame({"a": [1.0, np.nan, 3.0], "b": [1, 2, 3]})
        source = pd.DataFrame({"a": [np.nan], "b": [20]})
        # NaN matches NaN → update b=20
        expected = pd.DataFrame({"a": [1.0, np.nan, 3.0], "b": [1, 20, 3]})
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a"])

    def test_match_on_string_none_nan_indistinguishable(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        target = pd.DataFrame({"a": ["x", np.nan, None, np.nan, None], "b": [1, 2, 3, 4, 5], "c": [1, 2, 3, 4, 5]})
        source = pd.DataFrame({"a": ["x", np.nan, None, None, np.nan], "b": [4, 5, 3, 2, 1], "c": [10, 20, 30, 40, 50]})
        # Match on (a,b): (NaN,5)→row4, (NaN,3)→row2, (NaN,2)→row1; "x" b mismatch, (NaN,1) no match
        expected = pd.DataFrame({"a": ["x", np.nan, None, np.nan, None], "b": [1, 2, 3, 4, 5], "c": [1, 40, 30, 4, 20]})
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a", "b"])

    def test_all_columns_in_on(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        source = pd.DataFrame({"a": [1, 2, 3], "b": [99.0, 99.0, 99.0]})
        # When all columns are in on, b values differ → no matches
        expected = target.copy()
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["a", "b"])

    @pytest.mark.xfail(
        reason="In pandas 2 empty data frame is written it's index is datetime by default. The current implementation"
        "checks the for schema matching before starting to read the data. At this point we don't know if the"
        "target is empty, so it's not easy to do an early return. Ideally the empty index should be used"
    )
    @pytest.mark.parametrize("merge_metadata", (None, "meta"))
    def test_target_is_empty(self, lmdb_library, merge_metadata):
        lib = lmdb_library
        target = pd.DataFrame({"a": np.array([], dtype=np.int64)})
        lib.write("sym", target)
        source = pd.DataFrame({"a": np.array([1, 2], dtype=np.int64)})
        merge_vit = lib.merge_experimental("sym", source, strategy=self.strategy, metadata=merge_metadata, on=["a"])
        expected = target
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)

    def test_target_symbol_does_not_exist(self, lmdb_library):
        lib = lmdb_library
        source = pd.DataFrame({"a": [1], "b": [10.0]})
        with pytest.raises(StorageException):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["a"])

    def test_writes_new_version_even_if_nothing_is_changed(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        lib.write("sym", target)
        source = pd.DataFrame({"a": [10, 20], "b": [10.0, 20.0]})
        merge_vit = lib.merge_experimental("sym", source, strategy=self.strategy, on=["a"])
        assert merge_vit.version == 1

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, target)

        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize(
        "slicing_policy",
        [
            {"columns_per_segment": 2},
            {"rows_per_segment": 2, "columns_per_segment": 2},
        ],
    )
    @pytest.mark.parametrize(
        "on, expected",
        [
            (
                ["a"],
                # a=1,3,4,5 match; a=99 no match
                pd.DataFrame(
                    {
                        "a": [1, 2, 3, 4, 5],
                        "b": [10.0, 2.0, 30.0, 40.0, 50.0],
                        "c": ["X", "y", "Z", "W", "V"],
                        "d": [100, 20, 300, 400, 500],
                    }
                ),
            ),
            (
                ["d"],
                # Source d=[100,200,300,400,500] vs target d=[10,20,30,40,50] → no matches
                pd.DataFrame(
                    {
                        "a": [1, 2, 3, 4, 5],
                        "b": [1.0, 2.0, 3.0, 4.0, 5.0],
                        "c": ["x", "y", "z", "w", "v"],
                        "d": [10, 20, 30, 40, 50],
                    }
                ),
            ),
            (
                ["a", "d"],
                # must match on both, source d values don't match target d → no matches
                pd.DataFrame(
                    {
                        "a": [1, 2, 3, 4, 5],
                        "b": [1.0, 2.0, 3.0, 4.0, 5.0],
                        "c": ["x", "y", "z", "w", "v"],
                        "d": [10, 20, 30, 40, 50],
                    }
                ),
            ),
        ],
    )
    def test_on_column_with_column_slicing(self, lmdb_library_factory, slicing_policy, on, expected):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(**slicing_policy))
        target = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5],
                "b": [1.0, 2.0, 3.0, 4.0, 5.0],
                "c": ["x", "y", "z", "w", "v"],
                "d": [10, 20, 30, 40, 50],
            }
        )
        source = pd.DataFrame(
            {
                "a": [1, 99, 3, 4, 5],
                "b": [10.0, 20.0, 30.0, 40.0, 50.0],
                "c": ["X", "Y", "Z", "W", "V"],
                "d": [100, 200, 300, 400, 500],
            }
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=on)

    def test_throws_when_multiple_source_rows_match_same_target_row(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        source = pd.DataFrame({"a": [2, 2], "b": [10.0, 20.0]})
        lib.write("sym", target)
        with pytest.raises(UserInputException, match="Multiple source rows match the same target row"):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["a"])

    def test_throws_when_multiple_source_rows_match_same_target_row_via_nan(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [None, "x", "y"], "b": [1.0, 2.0, 3.0]})
        source = pd.DataFrame({"a": np.array([np.nan, None], dtype=object), "b": [10.0, 20.0]})
        lib.write("sym", target)
        with pytest.raises(UserInputException, match="Multiple source rows match the same target row"):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["a"])

    def test_on_column_named_same_as_index(self, lmdb_library):
        """Row-range index name is protobuf-only and does not collide with data columns.
        on=["col"] should match the data column and work without error."""
        lib = lmdb_library
        target = pd.DataFrame({"col": [1, 2, 3], "val": [10.0, 20.0, 30.0]})
        target.index.name = "col"
        source = pd.DataFrame({"col": [2], "val": [99.0]})
        source.index.name = "col"
        lib.write("sym", target)
        lib.merge_experimental("sym", source, strategy=self.strategy, on=["col"])
        result = lib.read("sym").data
        expected = pd.DataFrame({"col": [1, 2, 3], "val": [10.0, 99.0, 30.0]})
        expected.index.name = "col"
        assert_frame_equal(result, expected)

    @pytest.mark.parametrize(
        "index_name",
        ["col", None],
        ids=["row_range_index_name_same_as_duplicated_column_name", "row_range_index_does_not_have_a_name"],
    )
    def test_on_duplicate_data_columns_raises(self, lmdb_library, index_name):
        """Two data columns with the same name: on=["col"] is ambiguous → UserInputException."""
        lib = lmdb_library
        target = pd.DataFrame(
            np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]),
            columns=["col", "col", "other"],
        )
        source = pd.DataFrame(np.array([[10.0, 20.0, 30.0]]), columns=["col", "col", "other"])
        if index_name:
            target.index.name = index_name
            source.index.name = index_name
        lib.write("sym", target)
        with pytest.raises(UserInputException):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["col"])

    @pytest.mark.parametrize("on", ([None], ["a", None]))
    def test_match_on_column_named_none(self, lmdb_library, on):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], None: [1.0, 2.0, 3.0]})
        source = pd.DataFrame({"a": [10, 20, 30], None: [10.0, 20.0, 30.0]})
        lib.write("sym", target)
        with pytest.raises(TypeError):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=on)

    def test_row_range_index_name_is_not_an_actual_column(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.RangeIndex(start=0, stop=3))
        target.index.name = "my_index"
        source = pd.DataFrame({"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]}, index=pd.RangeIndex(start=0, stop=3))
        source.index.name = "my_index"
        lib.write("sym", target)
        with pytest.raises(UserInputException) as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["my_index"])
        assert "E_COLUMN_NOT_FOUND" in str(exc_info.value)
        assert "my_index" in str(exc_info.value)

    def test_row_range_index_name_does_not_count_as_duplicate(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"my_index": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.RangeIndex(start=0, stop=3))
        target.index.name = "my_index"
        source = pd.DataFrame({"my_index": [1, 20, 30], "b": [10.0, 20.0, 30.0]}, index=pd.RangeIndex(start=0, stop=3))
        source.index.name = "my_index"
        lib.write("sym", target)
        expected = pd.DataFrame({"my_index": [1, 2, 3], "b": [10.0, 2.0, 3.0]}, index=pd.RangeIndex(start=0, stop=3))
        expected.index.name = "my_index"
        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["my_index"])


class TestMergeMultiindexUpdate:
    """MultiIndex with datetime first level behaves like datetime-indexed merge.
    MultiIndex without datetime first level behaves like row-range-indexed merge."""

    def setup_method(self):
        self.strategy = MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING)

    ####################################################################################################################
    # On contains column that appears more than once in the dataframe. This is ambiguous and should raise. Multiindex
    # introduces another type of name mangling when a column is part of the multiindex. The primary index remains
    # unchanged, secondary index columns get __idx__ prefix. If the primary index is datetime index and is not named
    # it'll be assigned the default name "index" and in the protobuf we'll store that it's a fake name.
    ####################################################################################################################

    @pytest.mark.parametrize(
        "index_column_names, data_column_names, on",
        [
            pytest.param(
                ["my_duplicate", "my_duplicate", "secondary1"],
                ["data0", "data1"],
                ["my_duplicate"],
                id="match on primary index duplicated in first secondary",
            ),
            pytest.param(
                ["my_duplicate", "secondary0", "my_duplicate"],
                ["data0", "data1"],
                ["my_duplicate"],
                id="match on primary index duplicated in second secondary",
            ),
            pytest.param(
                ["primary", "my_duplicate", "my_duplicate"],
                ["data0", "data1"],
                ["my_duplicate"],
                marks=pytest.mark.xfail(
                    raises=ArcticException,
                    reason="ArcticDB fails on write when there are duplicate column names in the secondary index",
                ),
                id="match on secondary index that is duplicated in the multiindex",
            ),
            pytest.param(
                ["my_duplicate", "my_duplicate", "my_duplicate"],
                ["data0", "data1"],
                ["my_duplicate"],
                marks=pytest.mark.xfail(
                    raises=ArcticException,
                    reason="ArcticDB fails on write when there are duplicate column names in the secondary index",
                ),
                id="match on primary index that is duplicated twice in the multiindex",
            ),
            pytest.param(
                ["my_duplicate", "secondary0", "secondary1"],
                ["my_duplicate", "data1"],
                ["my_duplicate"],
                id="match on primary index that is duplicated in the data",
            ),
            pytest.param(
                ["primary", "secondary0", "my_duplicate"],
                ["my_duplicate", "data1"],
                ["my_duplicate"],
                id="match on a secondary index that is duplicated in the data",
            ),
            pytest.param(
                ["primary", "secondary0", "secondary1"],
                ["my_duplicate", "my_duplicate"],
                ["my_duplicate"],
                id="match on data column that is duplicated but not in the multiindex",
            ),
        ],
    )
    def test_duplicate_on_column_raises_rowrange(self, lmdb_library, index_column_names, data_column_names, on):
        lib = lmdb_library

        target_index_values = [["A", "B"], [1, 2], [10.0, 20.0]]
        target_data_values = [[pd.Timestamp(0), "A"], [pd.Timestamp(0), "B"]]
        target_idx = pd.MultiIndex.from_arrays(target_index_values, names=index_column_names)
        target = pd.DataFrame(target_data_values, columns=data_column_names, index=target_idx)

        source_index_values = [["A", "b"], [10, 20], [100.0, 200.0]]
        source_data_values = [[pd.Timestamp(100), "A"], [pd.Timestamp(200), "B"]]
        source_idx = pd.MultiIndex.from_arrays(source_index_values, names=index_column_names)
        source = pd.DataFrame(source_data_values, columns=data_column_names, index=source_idx)

        lib.write("sym", target)
        with pytest.raises(UserInputException, match="E_DUPLICATE_COLUMN") as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=on)
        assert '"my_duplicate"' in str(exc_info.value)

    @pytest.mark.parametrize(
        "index_column_names, data_column_names, on",
        [
            pytest.param(
                ["index", "index", "secondary1"],
                ["data0", "data1"],
                ["index"],
                id="Primary index explicitly named index not duplicated",
            ),
            pytest.param(
                ["index", "index", "secondary1"],
                ["data0", "data1"],
                ["index"],
                id="Primary index explicitly named index duplicated in secondary index",
            ),
            pytest.param(
                ["index", "index", "index"],
                ["data0", "data1"],
                ["index"],
                id="Primary index explicitly named index duplicated in secondary index twice",
                marks=pytest.mark.xfail(
                    raises=ArcticException,
                    reason="ArcticDB fails on write when there are duplicate column names in the secondary index",
                ),
            ),
            pytest.param(
                ["index", "secondary0", "secondary1"],
                ["index", "data1"],
                ["index"],
                id="Primary index explicitly named index duplicated in data",
            ),
            pytest.param(
                ["my_index", "my_index", "secondary1"],
                ["data0", "data1"],
                ["my_index"],
                id="Primary index explicitly named index not duplicated",
            ),
            pytest.param(
                ["my_index", "my_index", "secondary1"],
                ["data0", "data1"],
                ["my_index"],
                id="Primary index explicitly named index duplicated in secondary index",
            ),
            pytest.param(
                ["my_index", "my_index", "my_index"],
                ["data0", "data1"],
                ["my_index"],
                id="Primary index explicitly named index duplicated in secondary index twice",
                marks=pytest.mark.xfail(
                    raises=ArcticException,
                    reason="ArcticDB fails on write when there are duplicate column names in the secondary index",
                ),
            ),
            pytest.param(
                ["my_index", "secondary0", "secondary1"],
                ["my_index", "data1"],
                ["my_index"],
                id="Primary index explicitly named index duplicated in data",
            ),
        ],
    )
    def test_cannot_contain_index_column_name_when_datetime(
        self, lmdb_library, index_column_names, data_column_names, on
    ):
        lib = lmdb_library

        target_index_values = [pd.date_range("2025-01-01", "2025-01-02"), [1, 2], [10.0, 20.0]]
        target_data_values = [[pd.Timestamp(0), "A"], [pd.Timestamp(0), "B"]]
        target_idx = pd.MultiIndex.from_arrays(target_index_values, names=index_column_names)
        target = pd.DataFrame(target_data_values, columns=data_column_names, index=target_idx)

        source_index_values = [pd.date_range("2025-01-01", "2025-01-02"), [10, 20], [100.0, 200.0]]
        source_data_values = [[pd.Timestamp(100), "A"], [pd.Timestamp(200), "B"]]
        source_idx = pd.MultiIndex.from_arrays(source_index_values, names=index_column_names)
        source = pd.DataFrame(source_data_values, columns=data_column_names, index=source_idx)

        lib.write("sym", target)
        with pytest.raises(UserInputException) as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=on)
        assert "not contain the datetime index column" in str(exc_info.value)

    def test_unnamed_datetime_index_on_contains_column_named_index_not_in_dataframe(self, lmdb_library):
        lib = lmdb_library

        index_column_names = [None, "secondary0", "secondary1"]
        data_column_names = ["data0", "data1"]
        on = ["index"]

        target_index_values = [pd.date_range("2025-01-01", "2025-01-02"), [1, 2], [10.0, 20.0]]
        target_data_values = [[pd.Timestamp(0), "A"], [pd.Timestamp(0), "B"]]
        target_idx = pd.MultiIndex.from_arrays(target_index_values, names=index_column_names)
        target = pd.DataFrame(target_data_values, columns=data_column_names, index=target_idx)

        source_index_values = [pd.date_range("2025-01-01", "2025-01-02"), [10, 20], [100.0, 200.0]]
        source_data_values = [[pd.Timestamp(100), "A"], [pd.Timestamp(200), "B"]]
        source_idx = pd.MultiIndex.from_arrays(source_index_values, names=index_column_names)
        source = pd.DataFrame(source_data_values, columns=data_column_names, index=source_idx)

        lib.write("sym", target)
        with pytest.raises(UserInputException, match="E_COLUMN_NOT_FOUND"):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=on)

    @pytest.mark.parametrize(
        "index_column_names, data_column_names, on",
        [
            pytest.param(
                [None, "index", "index"],
                ["data0", "data1"],
                ["index"],
                id="Unnamed primary index and column named index appears twice in secondary index",
                marks=pytest.mark.xfail(
                    raises=ArcticException,
                    reason="ArcticDB fails on write when there are duplicate column names in the secondary index",
                ),
            ),
            pytest.param(
                [None, "index", "index"],
                ["index", "data1"],
                ["index"],
                id="Unnamed primary index and column named index appears twice in secondary index and once in data",
                marks=pytest.mark.xfail(
                    raises=ArcticException,
                    reason="ArcticDB fails on write when there are duplicate column names in the secondary index",
                ),
            ),
            pytest.param(
                [None, "index", "secondary1"],
                ["index", "data1"],
                ["index"],
                id="Unnamed primary index and column named index appears in secondary index and data",
            ),
            pytest.param(
                [None, "duplicate", "duplicate"],
                ["data0", "data1"],
                ["duplicate"],
                id="Unnamed primary index and column named duplicate appears twice in secondary index",
                marks=pytest.mark.xfail(
                    raises=ArcticException,
                    reason="ArcticDB fails on write when there are duplicate column names in the secondary index",
                ),
            ),
            pytest.param(
                [None, "duplicate", "duplicate"],
                ["duplicate", "data1"],
                ["duplicate"],
                id="Unnamed primary index and column named duplicate appears twice in secondary index and once in data",
                marks=pytest.mark.xfail(
                    raises=ArcticException,
                    reason="ArcticDB fails on write when there are duplicate column names in the secondary index",
                ),
            ),
            pytest.param(
                [None, "duplicate", "secondary1"],
                ["duplicate", "data1"],
                ["duplicate"],
                id="Unnamed primary index and column named duplicate appears in secondary index and in data columns",
            ),
            pytest.param(
                [None, "secondary0", "secondary1"],
                ["duplicate", "duplicate"],
                ["duplicate"],
                id="Unnamed primary index and column named duplicate appears twice in data columns",
            ),
            pytest.param(
                [None, "secondary0", "secondary1"],
                ["index", "index"],
                ["index"],
                id="Unnamed primary index and column named index appears twice in data columns",
            ),
        ],
    )
    def test_unnamed_datetime_index_and_on_contains_duplicate_colum(
        self, lmdb_library, index_column_names, data_column_names, on
    ):
        lib = lmdb_library

        target_index_values = [pd.date_range("2025-01-01", "2025-01-02"), [1, 2], [10.0, 20.0]]
        target_data_values = [[pd.Timestamp(0), "A"], [pd.Timestamp(0), "B"]]
        target_idx = pd.MultiIndex.from_arrays(target_index_values, names=index_column_names)
        target = pd.DataFrame(target_data_values, columns=data_column_names, index=target_idx)

        source_index_values = [pd.date_range("2025-01-01", "2025-01-02"), [10, 20], [100.0, 200.0]]
        source_data_values = [[pd.Timestamp(100), "A"], [pd.Timestamp(200), "B"]]
        source_idx = pd.MultiIndex.from_arrays(source_index_values, names=index_column_names)
        source = pd.DataFrame(source_data_values, columns=data_column_names, index=source_idx)

        lib.write("sym", target)
        with pytest.raises(UserInputException, match="E_DUPLICATE_COLUMN"):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=on)

    def test_default_on_rowrange_raises(self, lmdb_library):
        index_names = ["idx", "a", "b"]
        primary_target_vals = ["P", "Q", "R"]
        primary_source_vals = ["Q", "Z"]
        target_idx = pd.MultiIndex.from_arrays([primary_target_vals, ["A", "B", "C"], [1, 2, 3]], names=index_names)
        source_idx = pd.MultiIndex.from_arrays([primary_source_vals, ["B", "X"], [2, 9]], names=index_names)
        target = pd.DataFrame({"c": [1, 2, 3], "d": [-1.0, -2.0, -3.0]}, index=target_idx)
        source = pd.DataFrame({"c": [1000, 2000], "d": [-1000.0, -2000.0]}, index=source_idx)
        lmdb_library.write("sym", target)
        with pytest.raises(UserInputException):
            lmdb_library.merge_experimental("sym", source, strategy=self.strategy)

    ####################################################################################################################
    # Happy paths
    ####################################################################################################################

    def test_can_match_on_data_column_named_index_when_primary_is_unnamed_datetime_index(self, lmdb_library):
        lib = lmdb_library
        index_names = [None, "a", "b"]
        data_names = ["index", "d"]

        primary_target_vals = pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(4), pd.Timestamp(10)])
        target_idx = pd.MultiIndex.from_arrays([primary_target_vals, ["A", "B", "C"], [1, 2, 3]], names=index_names)
        target_data = [[1.0, 10], [2.0, 20], [3.0, 30]]
        target = pd.DataFrame(target_data, columns=data_names, index=target_idx)

        primary_source_vals = pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(4)])
        source_idx = pd.MultiIndex.from_arrays([primary_source_vals, ["aaa", "b"], [222, 444]], names=index_names)
        source_data = [[1.0, 999], [2.0, 888]]
        source = pd.DataFrame(source_data, columns=data_names, index=source_idx)

        expected_idx = pd.MultiIndex.from_arrays([primary_target_vals, ["A", "b", "C"], [1, 444, 3]], names=index_names)
        expected_data = [[1.0, 10], [2.0, 888], [3.0, 30]]
        expected = pd.DataFrame(expected_data, columns=data_names, index=expected_idx)

        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["index"])

    def test_can_match_on_secondary_index_column_named_index_when_primary_is_unnamed_datetime_index(self, lmdb_library):
        lib = lmdb_library
        index_names = [None, "index", "b"]
        data_names = ["c", "d"]

        primary_target_vals = pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(4), pd.Timestamp(10)])
        target_idx = pd.MultiIndex.from_arrays([primary_target_vals, ["A", "B", "C"], [1, 2, 3]], names=index_names)
        target_data = [[1.0, 10], [2.0, 20], [3.0, 30]]
        target = pd.DataFrame(target_data, columns=data_names, index=target_idx)

        primary_source_vals = pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(4)])
        source_idx = pd.MultiIndex.from_arrays([primary_source_vals, ["aa", "B"], [222, 444]], names=index_names)
        source_data = [[1.0, 999], [2.0, 888]]
        source = pd.DataFrame(source_data, columns=data_names, index=source_idx)

        expected_idx = pd.MultiIndex.from_arrays([primary_target_vals, ["A", "B", "C"], [1, 444, 3]], names=index_names)
        expected_data = [[1.0, 10], [2.0, 888], [3.0, 30]]
        expected = pd.DataFrame(expected_data, columns=data_names, index=expected_idx)

        generic_merge_test(lib, "sym", target, source, self.strategy, expected, on=["index"])

    def test_default_on_datetime(self, lmdb_library):
        index_names = ["idx", "a", "b"]
        primary_target_vals = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
        target_idx = pd.MultiIndex.from_arrays([primary_target_vals, ["A", "B", "C"], [1, 2, 3]], names=index_names)
        target = pd.DataFrame({"c": [1, 2, 3], "d": [-1.0, -2.0, -3.0]}, index=target_idx)

        primary_source_vals = pd.to_datetime(["2024-01-02", "2024-01-05"])
        source_idx = pd.MultiIndex.from_arrays([primary_source_vals, ["a", "b"], [200, 300]], names=index_names)
        source = pd.DataFrame({"c": [1000, 2000], "d": [-1000.0, -2000.0]}, index=source_idx)

        expected_idx = pd.MultiIndex.from_arrays(
            [primary_target_vals, ["A", "a", "C"], [1, 200, 3]], names=["idx", "a", "b"]
        )
        expected = pd.DataFrame({"c": [1, 1000, 3], "d": [-1.0, -1000.0, -3.0]}, index=expected_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, expected)

    def test_on_secondary_index_column_datetime(self, lmdb_library):
        index_names = [None, "a", "b"]
        primary_target_vals = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
        target_idx = pd.MultiIndex.from_arrays([primary_target_vals, ["A", "B", "C"], [1, 2, 3]], names=index_names)
        target = pd.DataFrame({"c": [1, 2, 3], "d": [-1.0, -2.0, -3.0]}, index=target_idx)

        primary_source_vals = pd.to_datetime(["2024-01-02", "2024-01-05"])
        source_idx = pd.MultiIndex.from_arrays([primary_source_vals, ["B", "b"], [200, 300]], names=index_names)
        source = pd.DataFrame({"c": [1000, 2000], "d": [-1000.0, -2000.0]}, index=source_idx)

        expected_idx = pd.MultiIndex.from_arrays(
            [primary_target_vals, ["A", "B", "C"], [1, 200, 3]], names=[None, "a", "b"]
        )
        expected = pd.DataFrame({"c": [1, 1000, 3], "d": [-1.0, -1000.0, -3.0]}, index=expected_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, expected, on=["a"])

    def test_on_secondary_index_column_rowrange(self, lmdb_library):
        index_names = ["idx", "a", "b"]
        target_idx = pd.MultiIndex.from_arrays(
            [np.array([-1, -2, -3], dtype=np.int8), ["A", "B", "C"], [1, 2, 3]], names=index_names
        )
        target = pd.DataFrame({"c": [1, 2, 3], "d": [-1.0, -2.0, -3.0]}, index=target_idx)

        source_idx = pd.MultiIndex.from_arrays(
            [np.array([-10, -20], dtype=np.int8), ["B", "b"], [200, 300]], names=index_names
        )
        source = pd.DataFrame({"c": [1000, 2000], "d": [-1000.0, -2000.0]}, index=source_idx)

        expected_idx = pd.MultiIndex.from_arrays(
            [np.array([-1, -10, -3], dtype=np.int8), ["A", "B", "C"], [1, 200, 3]],
            names=["idx", "a", "b"],
        )
        expected = pd.DataFrame({"c": [1, 1000, 3], "d": [-1.0, -1000.0, -3.0]}, index=expected_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, expected, on=["a"])

    def test_on_data_column_datetime(self, lmdb_library):
        index_names = [None, "a", "b"]
        primary_target_vals = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
        target_idx = pd.MultiIndex.from_arrays([primary_target_vals, ["A", "B", "C"], [1, 2, 3]], names=index_names)
        target = pd.DataFrame({"c": [1, 2, 3], "d": [-1.0, -2.0, -3.0]}, index=target_idx)

        primary_source_vals = pd.to_datetime(["2024-01-02", "2024-01-05"])
        source_idx = pd.MultiIndex.from_arrays([primary_source_vals, ["a", "b"], [200, 300]], names=index_names)
        source = pd.DataFrame({"c": [2, 2000], "d": [-1000.0, -2000.0]}, index=source_idx)

        expected_idx = pd.MultiIndex.from_arrays(
            [primary_target_vals, ["A", "a", "C"], [1, 200, 3]], names=[None, "a", "b"]
        )
        expected = pd.DataFrame({"c": [1, 2, 3], "d": [-1.0, -1000.0, -3.0]}, index=expected_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, expected, on=["c"])

    def test_on_data_column_rowrange(self, lmdb_library):
        index_names = [None, "a", "b"]
        target_idx = pd.MultiIndex.from_arrays(
            [np.array([-1, -2, -3], dtype=np.int8), ["A", "B", "C"], [1, 2, 3]], names=index_names
        )
        target = pd.DataFrame({"c": [1, 2, 3], "d": [-1.0, -2.0, -3.0]}, index=target_idx)

        source_idx = pd.MultiIndex.from_arrays(
            [np.array([-10, -20], dtype=np.int8), ["a", "b"], [200, 300]], names=index_names
        )
        source = pd.DataFrame({"c": [1000, 2], "d": [-1000.0, -2000.0]}, index=source_idx)

        expected_idx = pd.MultiIndex.from_arrays(
            [np.array([-1, -20, -3], dtype=np.int8), ["A", "b", "C"], [1, 300, 3]], names=index_names
        )
        expected = pd.DataFrame({"c": [1, 2, 3], "d": [-1.0, -2000.0, -3.0]}, index=expected_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, expected, on=["c"])

    def test_on_secondary_index_and_data_column_datetime(self, lmdb_library):
        index_names = [None, "a", "b"]
        primary_target_vals = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"])
        target_idx = pd.MultiIndex.from_arrays(
            [primary_target_vals, ["A", "B", "C", "D"], [1, 2, 3, 4]], names=index_names
        )
        target = pd.DataFrame({"c": [10, 20, 30, 40], "d": [-1.0, -2.0, -3.0, -4.0]}, index=target_idx)

        primary_source_vals = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
        source_idx = pd.MultiIndex.from_arrays(
            [primary_source_vals, ["B", "X", "D"], [200, 300, 400]], names=index_names
        )
        source = pd.DataFrame({"c": [20, 30, 99], "d": [-200.0, -300.0, -400.0]}, index=source_idx)
        expected_idx = pd.MultiIndex.from_arrays(
            [primary_target_vals, ["A", "B", "C", "D"], [1, 200, 3, 4]], names=index_names
        )
        expected = pd.DataFrame({"c": [10, 20, 30, 40], "d": [-1.0, -200.0, -3.0, -4.0]}, index=expected_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, expected, on=["a", "c"])

    def test_on_secondary_index_and_data_column_rowrange(self, lmdb_library):
        index_names = ["idx", "a", "b"]
        target_idx = pd.MultiIndex.from_arrays(
            [np.array([-1, -2, -3, -4], dtype=np.int8), ["A", "B", "C", "D"], [1, 2, 3, 4]], names=index_names
        )
        target = pd.DataFrame({"c": [10, 20, 30, 40], "d": [-1.0, -2.0, -3.0, -4.0]}, index=target_idx)

        source_idx = pd.MultiIndex.from_arrays(
            [np.array([-10, -20, -30], dtype=np.int8), ["B", "X", "D"], [200, 300, 400]], names=index_names
        )
        source = pd.DataFrame({"c": [20, 30, 99], "d": [-200.0, -300.0, -400.0]}, index=source_idx)
        expected_idx = pd.MultiIndex.from_arrays(
            [np.array([-1, -10, -3, -4], dtype=np.int8), ["A", "B", "C", "D"], [1, 200, 3, 4]], names=index_names
        )
        expected = pd.DataFrame({"c": [10, 20, 30, 40], "d": [-1.0, -200.0, -3.0, -4.0]}, index=expected_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, expected, on=["a", "c"])

    def test_no_data_columns_datetime(self, lmdb_library):
        dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
        target_idx = pd.MultiIndex.from_arrays([dates, ["A", "B", "C"]], names=["idx", "a"])
        target = pd.DataFrame(index=target_idx)

        source_idx = pd.MultiIndex.from_arrays(
            [pd.to_datetime(["2024-01-02", "2024-01-05"]), ["X", "Z"]], names=["idx", "a"]
        )
        source = pd.DataFrame(index=source_idx)
        # on=None matches by timestamp only. Jan2 matches → __idx__a updated from B to X. Jan5 no match.
        expected_idx = pd.MultiIndex.from_arrays([dates, ["A", "X", "C"]], names=["idx", "a"])
        expected = pd.DataFrame(index=expected_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, expected)

    def test_no_data_columns_rowrange(self, lmdb_library):
        target_idx = pd.MultiIndex.from_arrays([["A", "B", "C"], [1, 2, 3]], names=["a", "b"])
        target = pd.DataFrame(index=target_idx)

        source_idx = pd.MultiIndex.from_arrays([["B", "X"], [2, 9]], names=["a", "b"])
        source = pd.DataFrame(index=source_idx)
        # a=B matches row 1, but b=2 same in source and target; no data columns → unchanged
        expected = target.copy()
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, expected, on=["a"])

    @pytest.mark.parametrize("on", ([None], ["a", None]))
    @pytest.mark.parametrize("is_datetime", [True, False], ids=["datetime", "rowrange"])
    @pytest.mark.parametrize(
        "index_names, data_names", [([None, "a"], ["b"]), (["a", None], ["b"]), (["a", "b"], [None])]
    )
    def test_match_on_column_named_none(self, lmdb_library, on, is_datetime, index_names, data_names):
        lib = lmdb_library
        if is_datetime:
            idx = pd.MultiIndex.from_arrays(
                [[pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)], [1, 2, 3]], names=index_names
            )
            src_idx = pd.MultiIndex.from_arrays([[pd.Timestamp(0)], [1]], names=index_names)
        else:
            idx = pd.MultiIndex.from_arrays([["A", "B", "C"], [1, 2, 3]], names=index_names)
            src_idx = pd.MultiIndex.from_arrays([["A"], [1]], names=index_names)
        target = pd.DataFrame({c: [1.0, 2.0, 3.0] for c in data_names}, index=idx)
        source = pd.DataFrame({c: [99.0] for c in data_names}, index=src_idx)
        lib.write("sym", target)
        with pytest.raises(TypeError):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=on)


@pytest.mark.parametrize(
    "target_segments, source, expected",
    [
        pytest.param(
            [
                pd.DataFrame({"a": [0]}, index=pd.DatetimeIndex([10])),
                pd.DataFrame({"a": [1, 2, 3]}, index=pd.DatetimeIndex([10, 10, 30])),
                pd.DataFrame({"a": [4, 5]}, index=pd.DatetimeIndex([30, 30])),
            ],
            pd.DataFrame({"a": [1000, 1001, 1002, 1003]}, index=pd.DatetimeIndex([30, 40, 50, 60])),
            pd.DataFrame(
                {"a": [0, 1, 2, 3, 4, 5, 1000, 1001, 1002, 1003]},
                index=pd.DatetimeIndex([10, 10, 10, 30, 30, 30, 30, 40, 50, 60]),
            ),
            id="end_trailing",
        ),
        pytest.param(
            [
                pd.DataFrame({"a": [0]}, index=pd.DatetimeIndex([10])),
                pd.DataFrame({"a": [1, 2, 3]}, index=pd.DatetimeIndex([10, 10, 30])),
                pd.DataFrame({"a": [4, 5]}, index=pd.DatetimeIndex([30, 30])),
                pd.DataFrame({"a": [6]}, index=pd.DatetimeIndex([100])),
            ],
            pd.DataFrame({"a": [1000, 1001, 1002]}, index=pd.DatetimeIndex([30, 40, 50])),
            pd.DataFrame(
                {"a": [0, 1, 2, 3, 4, 5, 1000, 1001, 1002, 6]},
                index=pd.DatetimeIndex([10, 10, 10, 30, 30, 30, 30, 40, 50, 100]),
            ),
            id="middle_gap",
        ),
        pytest.param(
            [
                pd.DataFrame({"a": [0]}, index=pd.DatetimeIndex([10])),
                pd.DataFrame({"a": [1, 2, 3]}, index=pd.DatetimeIndex([10, 10, 30])),
                pd.DataFrame({"a": [4, 5]}, index=pd.DatetimeIndex([30, 30])),
            ],
            pd.DataFrame({"a": [1000, 1001, 1002]}, index=pd.DatetimeIndex([1, 2, 30])),
            pd.DataFrame(
                {"a": [1000, 1001, 0, 1, 2, 3, 4, 5, 1002]},
                index=pd.DatetimeIndex([1, 2, 10, 10, 10, 30, 30, 30, 30]),
            ),
            id="beginning_prepend",
        ),
        pytest.param(
            [
                pd.DataFrame({"a": [0]}, index=pd.DatetimeIndex([10])),
                pd.DataFrame({"a": [1, 2, 3]}, index=pd.DatetimeIndex([10, 10, 30])),
                pd.DataFrame({"a": [4, 5]}, index=pd.DatetimeIndex([30, 30])),
            ],
            pd.DataFrame({"a": [1000, 1001]}, index=pd.DatetimeIndex([20, 30])),
            pd.DataFrame(
                {"a": [0, 1, 2, 1000, 3, 4, 5, 1001]},
                index=pd.DatetimeIndex([10, 10, 10, 20, 30, 30, 30, 30]),
            ),
            id="within_span",
        ),
    ],
)
def test_merge_insert_with_repeated_boundary_timestamps(lmdb_library, target_segments, source, expected):
    lib = lmdb_library
    lib.write("sym", target_segments[0])
    for segment in target_segments[1:]:
        lib.append("sym", segment)

    lt = lib._dev_tools.library_tool()
    assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == len(target_segments)

    lib.merge_experimental("sym", source, strategy=MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT), on=["a"])
    assert_frame_equal(lib.read("sym").data, expected)


def test_merge_insert_into_back_shared_middle_group(lmdb_library):
    lib = lmdb_library
    lib.write("sym", pd.DataFrame({"a": [0, 1]}, index=pd.DatetimeIndex([10, 20])))
    lib.append("sym", pd.DataFrame({"a": [2, 3]}, index=pd.DatetimeIndex([20, 30])))
    lib.append("sym", pd.DataFrame({"a": [4, 5]}, index=pd.DatetimeIndex([30, 40])))
    lib.append("sym", pd.DataFrame({"a": [6, 7]}, index=pd.DatetimeIndex([40, 50])))
    assert len(lib._dev_tools.library_tool().find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 4

    source = pd.DataFrame({"a": [1000, 1001, 1002]}, index=pd.DatetimeIndex([15, 25, 35]))
    lib.merge_experimental("sym", source, strategy=MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT), on=["a"])

    expected = pd.DataFrame(
        {"a": [0, 1000, 1, 2, 1001, 3, 4, 1002, 5, 6, 7]},
        index=pd.DatetimeIndex([10, 15, 20, 20, 25, 30, 30, 35, 40, 40, 50]),
    )
    assert_frame_equal(lib.read("sym").data, expected)


def test_merge_insert_only_into_chain_with_shared_boundaries(lmdb_library):
    lib = lmdb_library
    lib.write("sym", pd.DataFrame({"a": [0, 1]}, index=pd.DatetimeIndex([10, 20])))
    lib.append("sym", pd.DataFrame({"a": [2, 3]}, index=pd.DatetimeIndex([20, 30])))
    lib.append("sym", pd.DataFrame({"a": [4, 5]}, index=pd.DatetimeIndex([30, 40])))
    lib.append("sym", pd.DataFrame({"a": [6, 7]}, index=pd.DatetimeIndex([40, 50])))
    assert len(lib._dev_tools.library_tool().find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 4

    source = pd.DataFrame({"a": [1000]}, index=pd.DatetimeIndex([25]))
    lib.merge_experimental("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT), on=["a"])

    expected = pd.DataFrame(
        {"a": [0, 1, 2, 1000, 3, 4, 5, 6, 7]},
        index=pd.DatetimeIndex([10, 20, 20, 25, 30, 30, 40, 40, 50]),
    )
    assert_frame_equal(lib.read("sym").data, expected)
