"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest
import pandas as pd
from enum import Enum
from arcticdb.util.test import assert_frame_equal, assert_vit_equals_except_data
import arcticdb
from arcticdb.version_store import VersionedItem
from arcticdb_ext.storage import KeyType
import numpy as np
from arcticdb.exceptions import SchemaException, UserInputException, SortingException
from typing import NamedTuple


class MergeAction(Enum):
    UPDATE = 1
    INSERT = 2
    DO_NOTHING = 3


class MergeStrategy(NamedTuple):
    matched: MergeAction = MergeAction.UPDATE
    not_matched_by_target: MergeAction = MergeAction.INSERT


def mock_find_keys_for_symbol(key_types):
    keys = {kt: [f"{kt}_{i}" for i in range(key_types[kt])] for kt in key_types}
    return lambda key_type, symbol: keys[key_type]


def raise_wrapper(exception, message=None):
    def _raise(*args, **kwargs):
        raise exception(message)

    return _raise


class TestMergeTimeseries:

    # ================================================================================================
    # ================================= APPLIES TO ALL STRATEGIES ====================================
    # ================================================================================================
    @pytest.mark.parametrize(
        "strategy",
        (
            MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
            MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
            MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
        ),
    )
    def test_merge_matched_update_with_metadata(self, lmdb_library, strategy, monkeypatch):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]},
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"]),
        )

        metadata = {"meta": "data"}
        monkeypatch.setattr(
            lib,
            "merge",
            lambda *args, **kwargs: VersionedItem(
                symbol=write_vit.symbol,
                library=write_vit.library,
                data=None,
                version=write_vit.version + 1,
                metadata=metadata,
                host=write_vit.host,
                timestamp=write_vit.timestamp + 1,
            ),
            raising=False,
        )

        merge_vit = lib.merge("sym", source, metadata=metadata, strategy=strategy)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
        assert merge_vit.metadata == metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        monkeypatch.setattr(
            lib,
            "read",
            lambda *args, **kwargs: VersionedItem(
                symbol=merge_vit.symbol,
                library=merge_vit.library,
                data=None,  # Not going to check the data, only the metadata
                version=merge_vit.version,
                metadata=merge_vit.metadata,
                host=merge_vit.host,
                timestamp=merge_vit.timestamp,
            ),
        )
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)

        lt = lib._dev_tools.library_tool()

        monkeypatch.setattr(
            lt,
            "find_keys_for_symbol",
            mock_find_keys_for_symbol({KeyType.TABLE_DATA: 2, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}),
        )

        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize("metadata", ({"meta": "data"}, None))
    @pytest.mark.parametrize(
        "strategy",
        (
            MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
            MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
            MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
        ),
    )
    def test_merge_does_not_write_new_version_with_empty_source(self, lmdb_library, metadata, strategy, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: write_vit, raising=False)
        merge_vit = lib.merge("sym", pd.DataFrame(), metadata=metadata, strategy=strategy)
        assert_vit_equals_except_data(write_vit, merge_vit)
        assert merge_vit.data is None and write_vit.data is None
        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 1

    @pytest.mark.parametrize(
        "strategy",
        (
            None,
            MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
            MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
            MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
        ),
    )
    @pytest.mark.parametrize(
        "source",
        [
            pd.DataFrame(
                {"a": np.array([1, 2, 3], dtype=np.int8), "b": [1.0, 2.0, 3.0]},
                index=pd.DatetimeIndex(
                    [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01 10:00:00"), pd.Timestamp("2024-01-02")]
                ),
            ),
            pd.DataFrame(
                {"c": np.array([1, 2, 3], dtype=np.int8), "b": [1.0, 2.0, 3.0]},
                index=pd.DatetimeIndex(
                    [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01 10:00:00"), pd.Timestamp("2024-01-02")]
                ),
            ),
            pd.DataFrame(
                {"b": [1.0, 2.0, 3.0]},
                index=pd.DatetimeIndex(
                    [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01 10:00:00"), pd.Timestamp("2024-01-02")]
                ),
            ),
        ],
    )
    def test_static_schema_merge_throws_when_schemas_differ(self, lmdb_library, strategy, source, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        monkeypatch.setattr(lib.__class__, "merge", raise_wrapper(SchemaException), raising=False)

        with pytest.raises(SchemaException):
            lib.merge("sym", source, strategy=strategy)

    @pytest.mark.parametrize(
        "strategy",
        (
            MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
            MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
            MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
        ),
    )
    def test_throws_if_source_is_not_sorted(self, lmdb_library, strategy, monkeypatch):
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

        monkeypatch.setattr(lib.__class__, "merge", raise_wrapper(SortingException), raising=False)

        with pytest.raises(SortingException):
            lib.merge("sym", source, strategy=strategy)

    # ================================================================================================
    # ==================================== TEST UPDATE ON MATCH ======================================
    # ================================================================================================
    @pytest.mark.parametrize(
        "strategy",
        (
            MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
            MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING),
            MergeStrategy(not_matched_by_target="do_nothing"),
            MergeStrategy("update", "do_nothing"),
            MergeStrategy("UPDATE", "DO_NOTHING"),
            MergeStrategy(MergeAction.UPDATE, "do_nothing"),
            MergeStrategy("update", MergeAction.DO_NOTHING),
        ),
    )
    def test_merge_update(self, lmdb_library, monkeypatch, strategy):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [4, 5, 6], "b": [7.0, 8.0, 9.0]},
            # Only the second row: "2024-01-02" matches
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"]),
        )
        monkeypatch.setattr(
            lib.__class__,
            "merge",
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

        merge_vit = lib.merge("sym", source, strategy=strategy)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        # Only the second row: "2024-01-02" is updated
        expected = pd.DataFrame({"a": [1, 4, 3], "b": [1.0, 8.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))

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

        lt = lib._dev_tools.library_tool()

        monkeypatch.setattr(
            lt,
            "find_keys_for_symbol",
            mock_find_keys_for_symbol({KeyType.TABLE_DATA: 2, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}),
        )

        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_merge_update_writes_new_version_even_if_nothing_is_changed(self, lmdb_library, monkeypatch):
        # In theory, it's possible to make so that it doesn't write a new version when nothing is matched, but the source
        # is not empty. This has lots of edge cases and will burden the implementation for almost no gain. If nothing is
        #  changed, we'll keep the same index and data keys and just write a new version key which is cheap.
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)
        monkeypatch.setattr(
            lib,
            "merge",
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
        source = pd.DataFrame({"a": [4, 5], "b": [4.0, 5.0]}, index=pd.date_range("2023-01-01", periods=2))
        merge_vit = lib.merge("sym", source, strategy=MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING))
        assert merge_vit.version == 1
        assert merge_vit.timestamp > write_vit.timestamp

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
        assert_vit_equals_except_data(merge_vit, read_vit)
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

    @pytest.mark.parametrize(
        "slicing_policy",
        [{"rows_per_segment": 2}, {"columns_per_segment": 2}, {"rows_per_segment": 2, "columns_per_segment": 2}],
    )
    def test_merge_update_row_slicing(self, lmdb_library_factory, slicing_policy, monkeypatch):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(**slicing_policy))
        target = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5], "b": [1.0, 2.0, 3.0, 4.0, 5.0], "c": ["a", "b", "c", "d", "e"]},
            index=pd.date_range("2024-01-01", periods=5),
        )
        lib.write("sym", target)

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)

        source = pd.DataFrame(
            {"a": [30, 50], "b": [30.1, 50.1]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-05")]),
        )
        lib.merge("sym", source, strategy=MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING))

        expected = pd.DataFrame({"a": [1, 2, 30, 4, 50], "b": [1.0, 2.0, 31.1, 4.0, 50.1]})
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

        lt = lib._dev_tools.library_tool()
        if "rows_per_segment" in slicing_policy and "columns_per_segment" in slicing_policy:
            # Start with 3 row slices and 2 column slices = 6 data keys
            # The second segment is overwritten with column slicing = 2 data keys
            # The third segment is overwritten with column slicing = 2 data keys
            expected_data_keys = 10
        elif "rows_per_segment" in slicing_policy:
            # Start with 3 row slices and no column slices -> 3 data keys
            # The second segment is overwritten with column slicing = 1 data key
            # The third segment is overwritten with column slicing = 1 data key
            expected_data_keys = 5
        elif "columns_per_segment" in slicing_policy:
            # Start with one row slice and 2 column slices -> 2 data keys
            # The segment is overwritten with column slicing = 2 data key
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

    def test_merge_update_on_index_and_column(self, lmdb_library, monkeypatch):
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
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["a"], strategy=MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING))

        expected = pd.DataFrame({"a": [1, 2, 3], "b": [10.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data

        assert_frame_equal(received, expected)

    def test_merge_update_multiple_columns(self, lmdb_library, monkeypatch):
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
                    "2024-01-01",  # index + b + d + e match (should update)
                    "2024-01-02",  # index + b match, but d + e differ (do nothing)
                    "2024-01-03",  # index + b + d match, e differs (do nothing)
                    "2024-01-05",  # new index, b+d+e match (do nothing)
                    "2024-01-06",  # new index, new b, new d, new e (do nothing)
                ]
            ),
        )

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge(
            "sym", source, on=["b", "d", "e"], strategy=MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING)
        )

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

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_merge_update_row_from_source_matches_multiple_rows_from_target(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
            ),
        )
        lib.write("sym", target)
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        source = pd.DataFrame({"a": [5], "b": [20.0]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]))
        lib.merge("sym", source, strategy=MergeStrategy(not_matched_by_target=MergeAction.DO_NOTHING))
        expected = pd.DataFrame(
            {"a": [5, 5, 3], "b": [20.0, 20.0, 3.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
            ),
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    @pytest.mark.parametrize(
        "strategy",
        (
            MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
            MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
        ),
    )
    def test_merge_update_throws_when_target_row_is_matched_more_than_once(self, lmdb_library, strategy, monkeypatch):
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

        monkeypatch.setattr(lib.__class__, "merge", raise_wrapper(UserInputException), raising=False)
        with pytest.raises(UserInputException):
            lib.merge("sym", source, strategy=strategy)

    # ================================================================================================
    # ================================= TEST INSERT NOT MATCHED ======================================
    # ================================================================================================

    @pytest.mark.parametrize(
        "strategy",
        (
            MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
            MergeStrategy("do_nothing", "insert"),
            MergeStrategy("DO_NOTHING", "INSERT"),
            MergeStrategy(MergeAction.DO_NOTHING, "insert"),
            MergeStrategy("do_nothing", MergeAction.INSERT),
        ),
    )
    def test_merge_insert_not_matched(self, lmdb_library, monkeypatch, strategy):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, -2, -3], "b": [-1.0, -2.0, -3.0]},
            index=pd.DatetimeIndex(["2023-01-01", "2024-01-01 10:00:00", "2025-01-04"]),
        )
        monkeypatch.setattr(
            lib.__class__,
            "merge",
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

        merge_vit = lib.merge("sym", source, strategy=strategy)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
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
    def test_merge_insert_not_matched_in_full_segment(self, lmdb_library_factory, date, monkeypatch):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=2, columns_per_segment=2))
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3)
        )
        lib.write("sym", target)
        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 4

        source = pd.DataFrame({"a": [10], "b": [20.0], "c": ["A"]}, index=pd.DatetimeIndex([date]))
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
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

    def test_merge_insert_not_matched_writes_new_version_even_if_nothing_is_changed(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)
        monkeypatch.setattr(
            lib,
            "merge",
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
        merge_vit = lib.merge("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
        assert merge_vit.version == 1
        assert merge_vit.timestamp > write_vit.timestamp

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

    def test_merge_insert_not_matched_on_index_and_column(self, lmdb_library, monkeypatch):
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
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["a"], strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))

        # The first row is matched, but the strategy for matched rows is DO_NOTHING, so no update
        # the rest rows are inserted
        expected = pd.concat([target, source.tail(len(source) - 1)]).sort_index()
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_merge_insert_not_matched_on_multiple_columns(self, lmdb_library, monkeypatch):
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

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["b", "d", "e"], strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
        expected = pd.concat([target, source.tail(len(source) - 1)]).sort_index()
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_merge_insert_does_not_throw_when_target_row_is_matched_more_than_once_when_matched_is_do_nothing(
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

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
        expected = pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [1.0, 2.0, 3.0, 4.0]}, index=pd.date_range("2024-01-01", periods=4)
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    # ================================================================================================
    # =================================== TEST UPDATE AND INSERT =====================================
    # ================================================================================================

    @pytest.mark.parametrize(
        "strategy",
        (
            None,
            MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
            MergeStrategy("update", "insert"),
            MergeStrategy("UPDATE", "INSERT"),
            MergeStrategy("update", MergeAction.INSERT),
            MergeStrategy(MergeAction.UPDATE, "insert"),
        ),
    )
    def test_update_and_insert(self, lmdb_library, monkeypatch, strategy):
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
            "merge",
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

        merge_vit = lib.merge("sym", source, strategy=strategy) if strategy else lib.merge("sym", source)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
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
    def test_update_and_insert_with_slicing(self, lmdb_library_factory, monkeypatch, slicing_policy):
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
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source)

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

    def test_update_and_insert_on_index_and_column(self, lmdb_library, monkeypatch):
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
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["a"])

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

    def test_update_and_insert_multiple_columns(self, lmdb_library, monkeypatch):
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

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge(
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

    class TestMergeRowRange:
        """Not implemented yet"""

        def test_merge_not_implemented_with_row_range_yet(self, lmdb_library, monkeypatch):
            lib = lmdb_library
            target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
            lib.write("sym", target)

            source = pd.DataFrame({"a": [1], "b": [2]})
            monkeypatch.setattr(lib.__class__, "merge", raise_wrapper(UserInputException), raising=False)
            with pytest.raises(UserInputException):
                lib.merge("sym", source)

    class TestMergeMultiindex:
        """Not implemented yet"""

        def test_merge_not_implemented_with_multiindex_yet(self, lmdb_library, monkeypatch):
            lib = lmdb_library
            target = pd.DataFrame(
                {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.MultiIndex.from_tuples([("A", 1), ("B", 2), ("C", 3)])
            )
            lib.write("sym", target)
            source = pd.DataFrame({"a": [2], "b": [3.0]}, index=pd.MultiIndex.from_tuples([("A", 1)]))
            monkeypatch.setattr(lib.__class__, "merge", raise_wrapper(UserInputException), raising=False)
            with pytest.raises(UserInputException):
                lib.merge("sym", source)
