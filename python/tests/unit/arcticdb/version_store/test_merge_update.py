"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest
import pandas as pd
from arcticdb.util.test import assert_frame_equal, assert_vit_equals_except_data, merge
import arcticdb
from arcticdb.version_store import VersionedItem
from arcticdb_ext.exceptions import SchemaException
from arcticdb_ext.storage import KeyType
import numpy as np
from arcticdb.exceptions import StreamDescriptorMismatch, UserInputException, UnsortedDataException, StorageException
from arcticdb.version_store.library import MergeAction, MergeStrategy
from arcticdb.version_store._store import normalize_merge_action
from typing import Union, List, Optional

pytestmark = pytest.mark.merge_update


def mock_find_keys_for_symbol(key_types):
    keys = {kt: [f"{kt}_{i}" for i in range(key_types[kt])] for kt in key_types}
    return lambda key_type, symbol: keys[key_type]


def raise_wrapper(exception, message=None):
    def _raise(*args, **kwargs):
        raise exception(message)

    return _raise


def generic_merge_test(
    lib,
    sym: str,
    target: Union[List[pd.DataFrame], pd.DataFrame],
    source: pd.DataFrame,
    strategy: MergeStrategy,
    on: Optional[List[str]] = None,
):
    if isinstance(target, pd.DataFrame):
        target = [target]
    lib.write(sym, target[0])
    for df in target[1:]:
        lib.append(sym, df)
    lib.merge_experimental(sym, source, strategy=strategy, on=on)

    concat_target = pd.concat(target)
    # For row range indexes, reset the index after concat so that the expected DataFrame has a contiguous RangeIndex
    # matching what ArcticDB returns on read. DatetimeIndex and MultiIndex are preserved as-is.
    if not isinstance(concat_target.index, (pd.DatetimeIndex, pd.MultiIndex)):
        concat_target = concat_target.reset_index(drop=True)
    expected = merge(concat_target, source, strategy, on=on)
    read_vit = lib.read(sym)
    assert_frame_equal(read_vit.data, expected)


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
    def test_merge_does_not_write_new_version_with_empty_source(self, lmdb_library, metadata, strategy):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)
        merge_vit = lib.merge_experimental("sym", pd.DataFrame(), metadata=metadata, strategy=strategy)
        # There's a bug in append, update, and merge when there's an empty source. All of them return the passed
        # metadata even though it's not used.
        merge_vit.metadata = write_vit.metadata
        assert_vit_equals_except_data(write_vit, merge_vit)
        assert merge_vit.data is None and write_vit.data is None
        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 1

        read_vit = lib.read("sym")
        assert read_vit.version == 0
        assert read_vit.metadata is None
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

        expected = merge(target, source, strategy)

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
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [30, 50], "b": [30.1, 50.1], "c": [False, False], "d": ["C", "E"]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-05")]),
        )
        lib.merge_experimental("sym", source, strategy=self.strategy)
        expected = merge(target, source, self.strategy)

        received = lib.read("sym").data
        assert_frame_equal(received, expected)

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
    @pytest.mark.parametrize("on", [["a"], ["d"], ["a", "d"]])
    def test_on_column_with_column_slicing(self, lmdb_library_factory, slicing_policy, on):
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
        generic_merge_test(lib, "sym", target, source, self.strategy, on=on)

    def test_on_empty_list_same_as_none(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame(
            {"a": [10, 20], "b": [10.0, 20.0]},
            index=pd.DatetimeIndex(["2024-01-01", "2024-01-05"]),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, on=[])

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
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

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
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["b", "d", "e"])

    def test_row_from_source_matches_multiple_rows_from_target(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
            ),
        )
        source = pd.DataFrame({"a": [5], "b": [20.0]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]))
        generic_merge_test(lib, "sym", target, source, self.strategy)

    def test_row_from_source_matches_multiple_rows_from_target_in_separate_slices(self, lmdb_library_factory):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=2))
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")]
            ),
        )
        source = pd.DataFrame({"a": [5], "b": [20.0], "c": ["B"]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")]))
        generic_merge_test(lib, "sym", target, source, self.strategy)

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
        # NOTE: detecting that nothing will be changed is easier when the target is empty. It will be easy to not
        # increase the version number. However, this will create two different behaviors because we currently increase
        # the version if nothing is updated. I think having too many different behaviors will be confusing. IMO this
        # must have the same behavior as test_merge_update_writes_new_version_even_if_nothing_is_changed.
        merge_vit = lib.merge_experimental(
            "sym", source, strategy=MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING), metadata=merge_metadata
        )
        expected = target
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)

    @pytest.mark.parametrize(
        "source",
        (
            pd.DataFrame([], index=pd.DatetimeIndex([])),
            pd.DataFrame({"a": []}, index=pd.DatetimeIndex([])),
            pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])),
        ),
    )
    @pytest.mark.parametrize("upsert", (pytest.param(True, marks=pytest.mark.xfail), False))
    def test_target_symbol_does_not_exist(self, lmdb_library, source, upsert):
        lib = lmdb_library

        if not upsert:
            with pytest.raises(StorageException):
                lib.merge_experimental(
                    "sym", source, strategy=MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING), upsert=upsert
                )
        else:
            merge_vit = lib.merge_experimental(
                "sym", source, strategy=MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING), upsert=upsert
            )
            expected = pd.DataFrame({"a": []}, index=pd.DatetimeIndex([]))
            read_vit = lib.read("sym")
            assert_vit_equals_except_data(merge_vit, read_vit)
            assert_frame_equal(read_vit.data, expected)

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
        expected = merge(target, source, self.strategy)
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
        expected = merge(pd.concat(target), source, strategy=self.strategy)
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
        expected = merge(pd.concat(target_list), source, strategy=self.strategy)
        assert_frame_equal(res, expected)

    def test_sorted_segments_overlap_but_source_is_in_first_segment_only(self, lmdb_version_store_v1):
        target1 = pd.DataFrame({"a": [1, 2]}, index=pd.to_datetime([pd.Timestamp(1), pd.Timestamp(2)]))
        target2 = pd.DataFrame({"a": [3]}, index=pd.to_datetime([pd.Timestamp(2)]))
        target_list = [target1, target2]
        source = pd.DataFrame({"a": [4, 5]}, index=pd.to_datetime([pd.Timestamp(0), pd.Timestamp(1)]))
        lib = lmdb_version_store_v1
        generic_merge_test(lib, "sym", target_list, source, self.strategy)

    def test_source_matches_first_value_of_first_segment_and_last_value_of_second_segment(self, lmdb_version_store_v1):
        target1 = pd.DataFrame({"a": [1, 2]}, index=pd.to_datetime([pd.Timestamp(0), pd.Timestamp(1)]))
        target2 = pd.DataFrame({"a": [3]}, index=pd.to_datetime([pd.Timestamp(1)]))
        target_list = [target1, target2]
        source = pd.DataFrame({"a": [5, 6]}, index=pd.to_datetime([pd.Timestamp(0), pd.Timestamp(1)]))
        lib = lmdb_version_store_v1
        generic_merge_test(lib, "sym", target_list, source, self.strategy)

    @pytest.mark.parametrize(
        "source",
        [
            pd.DataFrame(
                {"a": [1], "b": [99.0]},
                index=pd.DatetimeIndex([pd.Timestamp(0)]),
            ),
            pd.DataFrame(
                {"a": [2], "b": [99.0]},
                index=pd.DatetimeIndex([pd.Timestamp(0)]),
            ),
        ],
    )
    def test_on_column_with_overlapping_segments(self, lmdb_version_store_v1, source):
        lib = lmdb_version_store_v1
        target_list = [
            pd.DataFrame({"a": [1], "b": [10.0]}, index=pd.DatetimeIndex([pd.Timestamp(0)])),
            pd.DataFrame({"a": [2, 3], "b": [20.0, 30.0]}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1)])),
        ]
        generic_merge_test(lib, "sym", target_list, source, self.strategy, on=["a"])

    def test_index_matches_but_on_column_differs(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame(
            {"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]},
            index=pd.date_range("2024-01-01", periods=3),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

    def test_all_columns_in_on(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame(
            {"a": [1, 2, 3], "b": [99.0, 99.0, 99.0]},
            index=pd.date_range("2024-01-01", periods=3),
        )
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a", "b"])

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
        lib.write("sym", target)
        lib.merge_experimental("sym", source, strategy=self.strategy)
        assert_frame_equal(expected, merge(target, source, self.strategy))
        assert_frame_equal(expected, lib.read("sym").data)

    def test_match_on_float_nan(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        target = pd.DataFrame({"a": [1.0, np.nan, 3.0], "b": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3))
        source = pd.DataFrame({"a": [np.nan], "b": [20]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")]))
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

    def test_on_column_one_source_row_matches_multiple_target_rows(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 1, 2], "b": [10.0, 20.0, 30.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
            ),
        )
        source = pd.DataFrame({"a": [1], "b": [99.0]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]))
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

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
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

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
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a", "b"])

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
        with pytest.raises(UserInputException) as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["my_duplicated_column"])
        assert '"my_duplicated_column"' in str(exc_info.value)
        assert "more than once" in str(exc_info.value)
        assert "first repetition at: 1" in str(exc_info.value)

    @pytest.mark.parametrize("index_name", (None, "index_name"))
    def test_on_column_is_named_as_the_index(self, lmdb_library, index_name):
        lib = lmdb_library
        repeated_column = "index" if index_name is None else index_name
        target = pd.DataFrame(
            {repeated_column: [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        source = pd.DataFrame(
            {repeated_column: [1, 20, 3], "b": [10.0, 20.0, 30.0]},
            index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(2)]),
        )
        if index_name is not None:
            target.index.name = index_name
            source.index.name = index_name
        lib.write("sym", target)
        with pytest.raises(UserInputException) as exc_info:
            lib.merge_experimental("sym", source, strategy=self.strategy, on=[repeated_column])
        assert f'"{repeated_column}"' in str(exc_info.value)
        assert "timestamp index column" in str(exc_info.value)

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
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a", "a"])

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

        generic_merge_test(lib, "sym", [df1, df2], source, self.strategy, on=["a"])


class TestMergeTimeseriesInsert:
    @pytest.mark.parametrize(
        "strategy",
        (MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT), MergeStrategy("do_nothing", "insert")),
    )
    def test_basic(self, lmdb_library, monkeypatch, strategy):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, -2, -3], "b": [-1.0, -2.0, -3.0]},
            index=pd.DatetimeIndex(["2023-01-01", "2024-01-01 10:00:00", "2025-01-04"]),
        )
        monkeypatch.setattr(
            lib.__class__,
            "merge_experimental",
            lambda *args, **kwargs: VersionedItem(
                symbol=write_vit.symbol,
                library=write_vit.library,
                data=None,
                version=1,
                metadata=write_vit.metadata,
                host=write_vit.host,
                timestamp=write_vit.timestamp + 1,
            ),
            raising=False,
        )

        merge_vit = lib.merge_experimental("sym", source, strategy=strategy)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        expected = pd.concat([target, source]).sort_index()
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 1))
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

    def test_writes_new_version_even_if_nothing_is_changed(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)
        monkeypatch.setattr(
            lib,
            "merge_experimental",
            lambda *args, **kwargs: VersionedItem(
                symbol=write_vit.symbol,
                library=write_vit.library,
                data=None,
                version=write_vit.version + 1,
                metadata=write_vit.metadata,
                host=write_vit.host,
                timestamp=write_vit.timestamp + 1,
            ),
            raising=False,
        )
        source = pd.DataFrame(
            {"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]}, index=pd.date_range("2024-01-01", periods=3)
        )
        merge_vit = lib.merge_experimental(
            "sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT)
        )
        assert merge_vit.version == 1

        monkeypatch.setattr(
            lib,
            "read",
            lambda *args, **kwargs: VersionedItem(
                symbol=merge_vit.symbol,
                library=merge_vit.library,
                data=target,
                version=merge_vit.version,
                metadata=merge_vit.metadata,
                host=merge_vit.host,
                timestamp=merge_vit.timestamp,
            ),
        )
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(read_vit, merge_vit)
        assert_frame_equal(read_vit.data, target)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(
            lt,
            "find_keys_for_symbol",
            mock_find_keys_for_symbol({KeyType.TABLE_DATA: 1, KeyType.TABLE_INDEX: 1, KeyType.VERSION: 2}),
        )
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_index_and_column(self, lmdb_library, monkeypatch):
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
        monkeypatch.setattr(lib.__class__, "merge_experimental", lambda *args, **kwargs: None, raising=False)
        lib.merge_experimental(
            "sym", source, on=["a"], strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT)
        )

        # The first row is matched, but the strategy for matched rows is DO_NOTHING, so no update
        # the rest rows are inserted
        expected = pd.concat([target, source.tail(len(source) - 1)]).sort_index()
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_on_multiple_columns(self, lmdb_library, monkeypatch):
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

        monkeypatch.setattr(lib.__class__, "merge_experimental", lambda *args, **kwargs: None, raising=False)
        lib.merge_experimental(
            "sym", source, on=["b", "d", "e"], strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT)
        )
        expected = pd.concat([target, source.tail(len(source) - 1)]).sort_index()
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_does_not_throw_when_target_row_is_matched_more_than_once_when_matched_is_do_nothing(
        self, lmdb_library, monkeypatch
    ):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 4], "b": [1.0, 2.0, 4.0]},
            index=pd.DatetimeIndex(
                [
                    pd.Timestamp("2024-01-01"),  # Matches first row of target
                    pd.Timestamp("2024-01-01"),  # Also matches first row of target
                    pd.Timestamp("2024-01-04"),
                ]
            ),
        )

        monkeypatch.setattr(lib.__class__, "merge_experimental", lambda *args, **kwargs: None, raising=False)
        lib.merge_experimental("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
        expected = pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [1.0, 2.0, 3.0, 4.0]}, index=pd.date_range("2024-01-01", periods=4)
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_target_is_empty(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": np.array([], dtype=np.int64)}, index=pd.DatetimeIndex([]))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame({"a": np.array([1, 2], dtype=np.int64)}, index=pd.date_range("2024-01-01", periods=2))
        monkeypatch.setattr(
            lib.__class__,
            "merge_experimental",
            lambda *args, **kwargs: VersionedItem(
                symbol=write_vit.symbol,
                library=write_vit.library,
                data=None,
                version=1,
                metadata=None,
                host=write_vit.host,
                timestamp=write_vit.timestamp + 1,
            ),
            raising=False,
        )
        merge_vit = lib.merge_experimental("sym", source, strategy=MergeStrategy("do_nothing", "insert"))
        expected = source
        monkeypatch.setattr(
            lib,
            "read",
            lambda *args, **kwargs: VersionedItem(
                symbol=merge_vit.symbol,
                library=merge_vit.library,
                data=expected,
                version=merge_vit.version,
                metadata=merge_vit.metadata,
                host=merge_vit.host,
                timestamp=merge_vit.timestamp,
            ),
        )
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)


class TestMergeTimeseriesUpdateAndInsert:

    @pytest.mark.parametrize(
        "strategy",
        (None, MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT), MergeStrategy("update", "insert")),
    )
    def test_basic(self, lmdb_library, monkeypatch, strategy):
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

        monkeypatch.setattr(
            lib.__class__,
            "merge_experimental",
            lambda *args, **kwargs: VersionedItem(
                symbol=write_vit.symbol,
                library=write_vit.library,
                data=None,
                version=1,
                metadata=write_vit.metadata,
                host=write_vit.host,
                timestamp=write_vit.timestamp + 1,
            ),
            raising=False,
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

        monkeypatch.setattr(
            lib,
            "read",
            lambda *args, **kwargs: VersionedItem(
                symbol=merge_vit.symbol,
                library=merge_vit.library,
                data=expected,
                version=merge_vit.version,
                metadata=merge_vit.metadata,
                host=merge_vit.host,
                timestamp=merge_vit.timestamp,
            ),
        )

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(read_vit, merge_vit)
        assert_frame_equal(read_vit.data, expected)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(
            lt,
            "find_keys_for_symbol",
            mock_find_keys_for_symbol({KeyType.TABLE_DATA: 2, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}),
        )
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
    def test_target_symbol_does_not_exist(self, lmdb_library, monkeypatch, source, upsert, strategy):
        # Model non-existing target after Library.update
        # There is an upsert parameter to control whether to create the index. If upsert=False exception is thrown.
        # Since we're doing a merge on non-existing data, I think it's logical to assume that nothing matches. No
        # updates are performed. The insert operation will insert the data into the newly created target symbol.
        lib = lmdb_library

        if not upsert:
            monkeypatch.setattr(lib.__class__, "merge_experimental", raise_wrapper(StorageException), raising=False)
            with pytest.raises(StorageException):
                lib.merge_experimental("sym", source, strategy=strategy, upsert=upsert)
        else:
            import datetime

            monkeypatch.setattr(
                lib.__class__,
                "merge_experimental",
                lambda *args, **kwargs: VersionedItem(
                    symbol="sym",
                    library=lmdb_library.name,
                    data=None,
                    version=0,
                    metadata=None,
                    host=lmdb_library._nvs.env,
                    timestamp=pd.Timestamp(datetime.datetime.now()),
                ),
                raising=False,
            )
            merge_vit = lib.merge_experimental("sym", source, strategy=strategy, upsert=upsert)
            expected = source
            monkeypatch.setattr(
                lib,
                "read",
                lambda *args, **kwargs: VersionedItem(
                    symbol=merge_vit.symbol,
                    library=merge_vit.library,
                    data=expected,
                    version=merge_vit.version,
                    metadata=merge_vit.metadata,
                    host=merge_vit.host,
                    timestamp=merge_vit.timestamp,
                ),
            )
            read_vit = lib.read("sym")
            assert_vit_equals_except_data(merge_vit, read_vit)
            assert_frame_equal(read_vit.data, expected)

    def test_on_index_and_column(self, lmdb_library, monkeypatch):
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
        monkeypatch.setattr(lib.__class__, "merge_experimental", lambda *args, **kwargs: None, raising=False)
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
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data

        assert_frame_equal(received, expected)

    def test_on_multiple_columns(self, lmdb_library, monkeypatch):
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

        monkeypatch.setattr(lib.__class__, "merge_experimental", lambda *args, **kwargs: None, raising=False)
        lib.merge_experimental(
            "sym", source, on=["b", "d", "e"], strategy=MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING)
        )
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

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_target_is_empty(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": np.array([], dtype=np.int64)}, index=pd.DatetimeIndex([]))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame({"a": np.array([1, 2], dtype=np.int64)}, index=pd.date_range("2024-01-01", periods=2))
        monkeypatch.setattr(
            lib.__class__,
            "merge_experimental",
            lambda *args, **kwargs: VersionedItem(
                symbol=write_vit.symbol,
                library=write_vit.library,
                data=None,
                version=1,
                metadata=None,
                host=write_vit.host,
                timestamp=write_vit.timestamp + 1,
            ),
            raising=False,
        )
        merge_vit = lib.merge_experimental("sym", source)
        expected = source
        monkeypatch.setattr(
            lib,
            "read",
            lambda *args, **kwargs: VersionedItem(
                symbol=merge_vit.symbol,
                library=merge_vit.library,
                data=expected,
                version=merge_vit.version,
                metadata=merge_vit.metadata,
                host=merge_vit.host,
                timestamp=merge_vit.timestamp,
            ),
        )
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)

    @pytest.mark.skip(reason="Not implemented yet")
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
        strategy = MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT)
        with pytest.raises(UserInputException):
            lib.merge_experimental("sym", source, strategy=strategy)


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
    def test_merge_does_not_write_new_version_with_empty_source(self, lmdb_library, metadata, strategy):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        write_vit = lib.write("sym", target)
        merge_vit = lib.merge_experimental("sym", pd.DataFrame(), metadata=metadata, strategy=strategy, on=["a"])
        # There's a bug in append, update, and merge when there's an empty source. All of them return the passed
        # metadata even though it's not used.
        merge_vit.metadata = write_vit.metadata
        assert_vit_equals_except_data(write_vit, merge_vit)
        assert merge_vit.data is None and write_vit.data is None
        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 1

        read_vit = lib.read("sym")
        assert read_vit.version == 0
        assert read_vit.metadata is None
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
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

    def test_no_matches(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        source = pd.DataFrame({"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]})
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

    def test_all_rows_match(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        source = pd.DataFrame({"a": [3, 1, 2], "b": [30.0, 10.0, 20.0]})
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

    @pytest.mark.parametrize(
        "target,source",
        [
            (
                pd.DataFrame(
                    {
                        "a": [1, 2, 3, 4],
                        "b": ["x", "y", "z", "w"],
                        "c": [10.0, 20.0, 30.0, 40.0],
                    }
                ),
                pd.DataFrame(
                    {
                        "a": [1, 2, 99, 4],
                        "b": ["x", "wrong", "z", "w"],
                        "c": [99.0, 99.0, 99.0, 99.0],
                    }
                ),
            ),
            (
                pd.DataFrame(
                    {
                        "a": [1, 1, 2, 2],
                        "b": ["x", "y", "x", "y"],
                        "c": [10.0, 20.0, 30.0, 40.0],
                    }
                ),
                pd.DataFrame(
                    {
                        "a": [1, 2, 1, 2],
                        "b": ["x", "x", "y", "y"],
                        "c": [99.0, 99.0, 99.0, 99.0],
                    }
                ),
            ),
        ],
    )
    def test_multiple_on_columns(self, lmdb_library, target, source):
        lib = lmdb_library
        # Must match on both "a" and "b": rows 0 and 3 match, rows 1 and 2 do not
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a", "b"])

    def test_one_source_row_matches_multiple_target_rows(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 1, 2], "b": [10.0, 20.0, 30.0]})
        source = pd.DataFrame({"a": [1], "b": [99.0]})
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

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
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

    def test_match_on_float_nan(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        target = pd.DataFrame({"a": [1.0, np.nan, 3.0], "b": [1, 2, 3]})
        source = pd.DataFrame({"a": [np.nan], "b": [20]})
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a"])

    def test_match_on_string_none_nan_indistinguishable(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        target = pd.DataFrame({"a": ["x", np.nan, None, np.nan, None], "b": [1, 2, 3, 4, 5], "c": [1, 2, 3, 4, 5]})
        source = pd.DataFrame({"a": ["x", np.nan, None, None, np.nan], "b": [4, 5, 3, 2, 1], "c": [10, 20, 30, 40, 50]})
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a", "b"])

    def test_all_columns_in_on(self, lmdb_library):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        source = pd.DataFrame({"a": [1, 2, 3], "b": [99.0, 99.0, 99.0]})
        # When all columns are in on, there are no columns left to update
        generic_merge_test(lib, "sym", target, source, self.strategy, on=["a", "b"])

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
    @pytest.mark.parametrize("on", [["a"], ["d"], ["a", "d"]])
    def test_on_column_with_column_slicing(self, lmdb_library_factory, slicing_policy, on):
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
        generic_merge_test(lib, "sym", target, source, self.strategy, on=on)

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

    def test_on_duplicate_data_columns_raises(self, lmdb_library):
        """Two data columns with the same name: on=["col"] is ambiguous → UserInputException."""
        lib = lmdb_library
        target = pd.DataFrame(
            np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]),
            columns=["col", "col", "other"],
        )
        source = pd.DataFrame(np.array([[10.0, 20.0, 30.0]]), columns=["col", "col", "other"])
        lib.write("sym", target)
        with pytest.raises(UserInputException):
            lib.merge_experimental("sym", source, strategy=self.strategy, on=["col"])


class TestMergeMultiindexUpdate:
    """MultiIndex with datetime first level behaves like datetime-indexed merge.
    MultiIndex without datetime first level behaves like row-range-indexed merge."""

    def setup_method(self):
        self.strategy = MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING)

    # -- Duplicate "col" in on= should raise --------------------------------------------
    #
    # Not tested: cases where two or more non-first MI levels share the same name.
    # ArcticDB stores all MI levels except the first as __idx__<name> internal columns
    # (the first level becomes either the datetime index or the row-range index). When
    # two non-first levels share a name, normalization creates duplicate __idx__<name>
    # columns and write() itself fails with ArcticException. This applies to both
    # datetime and rowrange. The only duplicate-level-name case that survives write is
    # rowrange ["col","col"] (2 levels) — only the second level gets __idx__col.

    @pytest.mark.parametrize(
        "index_names, data_cols",
        [
            (["col", "col"], ["val"]),  # dup in levels
            (["col", "col"], ["col"]),  # dup in levels and data
        ],
        ids=["dup_index", "dup_both"],
    )
    def test_duplicate_col_in_on_rowrange_dup_levels_raises(self, lmdb_library, index_names, data_cols):
        """Rowrange 2-level MI with duplicate level names: write succeeds, merge raises."""
        level_values = [["A", "B", "C"], [1, 2, 3]]
        idx = pd.MultiIndex.from_arrays(level_values, names=index_names)
        src_idx = pd.MultiIndex.from_arrays([[v[0]] for v in level_values], names=index_names)

        target = pd.DataFrame({c: [1.0, 2.0, 3.0] for c in data_cols}, index=idx)
        source = pd.DataFrame({c: [99.0] for c in data_cols}, index=src_idx)

        lmdb_library.write("sym", target)
        with pytest.raises(UserInputException):
            lmdb_library.merge_experimental("sym", source, strategy=self.strategy, on=["col"])

    @pytest.mark.parametrize("is_datetime", [True, False], ids=["datetime", "rowrange"])
    @pytest.mark.parametrize(
        "index_names, data_cols",
        [
            (["col", "other"], ["col"]),  # first level name matches data col
            (["cat", "col"], ["col"]),  # secondary level matches data col
        ],
        ids=["first_level-dup_data", "secondary_level-dup_data"],
    )
    def test_duplicate_col_in_on_level_matches_data_raises(self, lmdb_library, is_datetime, index_names, data_cols):
        # For rowrange, the first MI level becomes the row-range index and C++ does not
        # check its name against data columns. This is a known bug.
        level_values = [["A", "B", "C"], [1, 2, 3], [10, 20, 30]]
        on = []
        if is_datetime:
            on.append("date")
            all_names = ["date"] + index_names
            all_target_vals = [pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])] + level_values
        else:
            on.append("col")
            all_names = index_names
            all_target_vals = level_values
        all_target_vals = all_target_vals[: len(all_names)]
        all_source_vals = [[v[0]] for v in all_target_vals]

        idx = pd.MultiIndex.from_arrays(all_target_vals, names=all_names)
        src_idx = pd.MultiIndex.from_arrays(all_source_vals, names=all_names)

        target = pd.DataFrame({c: [1.0, 2.0, 3.0] for c in data_cols}, index=idx)
        source = pd.DataFrame({c: [99.0] for c in data_cols}, index=src_idx)

        lmdb_library.write("sym", target)
        with pytest.raises(UserInputException):
            assert len(on) > 0
            lmdb_library.merge_experimental("sym", source, strategy=self.strategy, on=on)

    @pytest.mark.parametrize("is_datetime", [True, False], ids=["datetime", "rowrange"])
    def test_duplicate_col_in_on_dup_data_columns_raises(self, lmdb_library, is_datetime):
        """Actual duplicate data column names (not deduped by dict comprehension)."""
        if is_datetime:
            idx = pd.MultiIndex.from_arrays(
                [pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]), ["A", "B", "C"]],
                names=["date", "cat"],
            )
            src_idx = pd.MultiIndex.from_arrays(
                [pd.to_datetime(["2024-01-01"]), ["A"]],
                names=["date", "cat"],
            )
        else:
            idx = pd.MultiIndex.from_arrays(
                [["A", "B", "C"], [1, 2, 3]],
                names=["cat", "other"],
            )
            src_idx = pd.MultiIndex.from_arrays([["A"], [1]], names=["cat", "other"])
        target = pd.DataFrame(
            np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]),
            columns=["col", "col"],
            index=idx,
        )
        source = pd.DataFrame(
            np.array([[99.0, 100.0]]),
            columns=["col", "col"],
            index=src_idx,
        )

        lmdb_library.write("sym", target)
        with pytest.raises(UserInputException):
            lmdb_library.merge_experimental("sym", source, strategy=self.strategy, on=["col"])

    def test_duplicate_col_in_on_data_col_matches_datetime_first_level(self, lmdb_library):
        """MI datetime first level "date" + data col "date": on=["date"] is ambiguous."""
        idx = pd.MultiIndex.from_arrays(
            [pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]), ["A", "B", "C"]],
            names=["date", "cat"],
        )
        src_idx = pd.MultiIndex.from_arrays(
            [pd.to_datetime(["2024-01-01"]), ["A"]],
            names=["date", "cat"],
        )
        target = pd.DataFrame({"date": [1.0, 2.0, 3.0], "val": [10.0, 20.0, 30.0]}, index=idx)
        source = pd.DataFrame({"date": [99.0], "val": [50.0]}, index=src_idx)
        lmdb_library.write("sym", target)
        with pytest.raises(UserInputException):
            lmdb_library.merge_experimental("sym", source, strategy=self.strategy, on=["date"])

    def test_duplicate_col_in_on_nonfirst_level_matches_datetime_first_level(self, lmdb_library):
        """MI datetime first level "date" + non-first level also "date": on=["date"] is ambiguous."""
        idx = pd.MultiIndex.from_arrays(
            [pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]), ["A", "B", "C"]],
            names=["date", "date"],
        )
        src_idx = pd.MultiIndex.from_arrays(
            [pd.to_datetime(["2024-01-01"]), ["A"]],
            names=["date", "date"],
        )
        target = pd.DataFrame({"val": [1.0, 2.0, 3.0]}, index=idx)
        source = pd.DataFrame({"val": [99.0]}, index=src_idx)
        lmdb_library.write("sym", target)
        with pytest.raises(UserInputException):
            lmdb_library.merge_experimental("sym", source, strategy=self.strategy, on=["date"])

    # -- No column name collisions: positive tests ----------------------------------------

    @staticmethod
    def _make_mi_frames(is_datetime):
        """Build target and source DataFrames with a 3-level MultiIndex and 2 data columns.

        index = [idx, a, b], data = [c, d]
        When is_datetime=True, "idx" is a DatetimeIndex; otherwise it is categorical.

        Source overlaps target on one index row to produce an update,
        and has one non-matching row that should be ignored (strategy = do_nothing).
        """
        if is_datetime:
            idx_vals = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
            src_idx_vals = pd.to_datetime(["2024-01-02", "2024-01-05"])
        else:
            idx_vals = ["P", "Q", "R"]
            src_idx_vals = ["Q", "Z"]
        target_idx = pd.MultiIndex.from_arrays(
            [idx_vals, ["A", "B", "C"], [1, 2, 3]], names=["idx", "a", "b"]
        )
        source_idx = pd.MultiIndex.from_arrays(
            [src_idx_vals, ["B", "X"], [2, 9]], names=["idx", "a", "b"]
        )
        target = pd.DataFrame(
            {"c": [1.0, 2.0, 3.0], "d": [10.0, 20.0, 30.0]}, index=target_idx
        )
        source = pd.DataFrame(
            {"c": [99.0, 88.0], "d": [999.0, 888.0]}, index=source_idx
        )
        return target, source

    def test_default_on_datetime(self, lmdb_library):
        """on=None with datetime first level: match on all index levels."""
        target, source = self._make_mi_frames(is_datetime=True)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy)

    def test_default_on_rowrange_raises(self, lmdb_library):
        """on=None with rowrange first level: requires explicit on parameter."""
        target, source = self._make_mi_frames(is_datetime=False)
        lmdb_library.write("sym", target)
        with pytest.raises(UserInputException):
            lmdb_library.merge_experimental("sym", source, strategy=self.strategy)

    @pytest.mark.parametrize("is_datetime", [True, False], ids=["datetime", "rowrange"])
    def test_on_index_column(self, lmdb_library, is_datetime):
        """on contains a column that is part of the multiindex (non-first level)."""
        target, source = self._make_mi_frames(is_datetime)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, on=["a"])

    @pytest.mark.parametrize("is_datetime", [True, False], ids=["datetime", "rowrange"])
    def test_on_data_column(self, lmdb_library, is_datetime):
        """on contains a column that is a data column, not in the multiindex."""
        target, source = self._make_mi_frames(is_datetime)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, on=["c"])

    @pytest.mark.parametrize("is_datetime", [True, False], ids=["datetime", "rowrange"])
    def test_on_mixed_index_and_data_column(self, lmdb_library, is_datetime):
        """on contains one multiindex level and one data column."""
        target, source = self._make_mi_frames(is_datetime)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, on=["a", "c"])

    def test_no_data_columns_datetime(self, lmdb_library):
        """MI datetime with no data columns — only index levels, on=None."""
        dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
        target_idx = pd.MultiIndex.from_arrays([dates, ["A", "B", "C"]], names=["idx", "a"])
        target = pd.DataFrame(index=target_idx)

        source_idx = pd.MultiIndex.from_arrays(
            [pd.to_datetime(["2024-01-02", "2024-01-05"]), ["X", "Z"]], names=["idx", "a"]
        )
        source = pd.DataFrame(index=source_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy)

    def test_no_data_columns_rowrange(self, lmdb_library):
        """MI rowrange with no data columns — on must reference an index level."""
        target_idx = pd.MultiIndex.from_arrays([["A", "B", "C"], [1, 2, 3]], names=["a", "b"])
        target = pd.DataFrame(index=target_idx)

        source_idx = pd.MultiIndex.from_arrays([["B", "X"], [2, 9]], names=["a", "b"])
        source = pd.DataFrame(index=source_idx)
        generic_merge_test(lmdb_library, "sym", target, source, self.strategy, on=["a"])
