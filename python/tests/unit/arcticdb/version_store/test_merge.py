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
from arcticdb.exceptions import SchemaException, UserInputException
from typing import NamedTuple

class MergeAction(Enum):
    UPDATE = 1
    INSERT = 2,
    DO_NOTHING = 3

class MergeStrategy(NamedTuple):
    matched: MergeAction = MergeAction.UPDATE
    not_matched: MergeAction = MergeAction.INSERT


# Note: Maxim had another suggestion for using enum flags instead of a tuple. That would require that the strategies
# for matched and not_matched consist of non-intersecting actions. This is not the case because of INSERT and DO_NOTHING.
# This means that INSERT needs two variants.

# class MergeAction(Flags):
#     UPDATE = 0
#     DO_NOTHING_MATCHED = 1
#     DO_NOTHING_NOT_MATCHED = 2
#     INSERT_MATCHED = 4
#     INSERT_NOT_MATCHED = 8
#
# this can be used like
# lib.merge("sym", source, strategy=MergeAction.DO_NOTHING_MATCHED | MergeAction.INSERT_NOT_MATCHED)
# lib.merge("sym", source, strategy=MergeAction.INSERT_MATCHED | MergeAction.INSERT_NOT_MATCHED)
#
# but it's not shorter than
# lib.merge("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
# lib.merge("sym", source, strategy=MergeStrategy(matched=MergeAction.DO_NOTHING, not_matched=MergeAction.INSERT))
# lib.merge("sym", source, strategy=MergeStrategy(MergeAction.INSERT, MergeAction.INSERT))


def mock_find_keys_for_symbol(key_types):
    keys = {kt: [f"{kt}_{i}" for i in range(key_types[kt])] for kt in key_types}
    return lambda key_type, symbol: keys[key_type]

def raise_wrapper(exception, message=None):
    def _raise(*args, **kwargs):
        raise exception(message)
    return _raise

class TestMergeTimeseries:

    @pytest.mark.parametrize("strategy", (
        MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
        MergeStrategy(MergeAction.INSERT, MergeAction.DO_NOTHING),
        MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
        MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
        MergeStrategy(MergeAction.INSERT, MergeAction.INSERT)
    ))
    def test_merge_matched_update_with_metadata(self, lmdb_library, strategy, monkeypatch):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]},
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"])
        )

        metadata = {"meta": "data"}
        monkeypatch.setattr(lib, "merge", lambda *args, **kwargs: VersionedItem(
            symbol=write_vit.symbol,
            library=write_vit.library,
            data=None,
            version=write_vit.version + 1,
            metadata=metadata,
            host=write_vit.host,
            timestamp=write_vit.timestamp + 1,
        ), raising=False)

        merge_vit = lib.merge("sym", source, metadata=metadata, strategy=strategy)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
        assert merge_vit.metadata == metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem(
            symbol=merge_vit.symbol,
            library=merge_vit.library,
            data=None, # Not going to check the data, only the metadata
            version=merge_vit.version,
            metadata=merge_vit.metadata,
            host=merge_vit.host,
            timestamp=merge_vit.timestamp,
        ))
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)

        lt = lib._dev_tools.library_tool()

        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 2, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))

        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize("metadata", ({"meta": "data"}, None))
    @pytest.mark.parametrize("strategy", (
        MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
        MergeStrategy(MergeAction.INSERT, MergeAction.DO_NOTHING),
        MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
        MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
        MergeStrategy(MergeAction.INSERT, MergeAction.INSERT)
    ))
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

    @pytest.mark.parametrize("strategy", (
        None,
        MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
        MergeStrategy(MergeAction.INSERT, MergeAction.DO_NOTHING),
        MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT),
        MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
        MergeStrategy(MergeAction.INSERT, MergeAction.INSERT)
    ))
    @pytest.mark.parametrize("source", [
        pd.DataFrame(
            {"a": np.array([1, 2, 3], dtype=np.int8),"b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01 10:00:00"),
                pd.Timestamp("2024-01-02")
            ])
        ),
        pd.DataFrame(
            {"c": np.array([1, 2, 3], dtype=np.int8), "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01 10:00:00"),
                pd.Timestamp("2024-01-02")
            ])
        ),
        pd.DataFrame(
            {"b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01 10:00:00"),
                pd.Timestamp("2024-01-02")
            ])
        )
    ])
    def test_static_schema_merge_throws_when_schemas_differ(self, lmdb_library, strategy, source, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        monkeypatch.setattr(lib.__class__, "merge", raise_wrapper(SchemaException), raising=False)

        with pytest.raises(SchemaException):
            lib.merge("sym", source, strategy=strategy)

    # ================================================================================================
    # ==================================== TEST UPDATE ON MATCH ======================================
    # ================================================================================================
    def test_merge_update(self, lmdb_library, monkeypatch):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]},
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"])
        )
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: VersionedItem(
            symbol=write_vit.symbol,
            library=write_vit.library,
            data=None,
            version=1,
            metadata=write_vit.metadata,
            host=write_vit.host,
            timestamp=write_vit.timestamp + 1,
        ), raising=False)

        merge_vit = lib.merge("sym", source, strategy=MergeStrategy(not_matched=MergeAction.DO_NOTHING))
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        expected = pd.DataFrame({"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem(
            symbol=merge_vit.symbol,
            library=merge_vit.library,
            data=expected,
            version=merge_vit.version,
            metadata=merge_vit.metadata,
            host=merge_vit.host,
            timestamp=merge_vit.timestamp,
        ))

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)

        lt = lib._dev_tools.library_tool()

        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 2, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))

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
        monkeypatch.setattr(lib, "merge", lambda *args, **kwargs: VersionedItem(
            symbol=write_vit.symbol,
            library=write_vit.library,
            data=None,
            version=write_vit.version + 1,
            metadata=write_vit.metadata,
            host=write_vit.host,
            timestamp=write_vit.timestamp + 1,
        ), raising=False)
        source = pd.DataFrame({"a": [4, 5], "b": [4.0, 5.0]}, index=pd.date_range("2023-01-01", periods=2))
        merge_vit = lib.merge("sym", source, strategy=MergeStrategy(not_matched=MergeAction.DO_NOTHING))
        assert merge_vit.version == 1
        assert merge_vit.timestamp > write_vit.timestamp

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem(
            symbol=merge_vit.symbol,
            library=merge_vit.library,
            data=target,
            version=merge_vit.version,
            metadata=merge_vit.metadata,
            host=merge_vit.host,
            timestamp=merge_vit.timestamp,
        ))
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, target)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 1, KeyType.TABLE_INDEX: 1, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_merge_update_row_slicing(self, lmdb_library_factory, monkeypatch):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=3))
        target = pd.DataFrame({"a": range(7), "b": np.arange(7, dtype=np.float64)}, index=pd.date_range("2024-01-01", periods=7))
        lib.write("sym", target)

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)

        source = pd.DataFrame({"a": [30, 50], "b": [31.0, 51.0]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-04"), pd.Timestamp("2024-01-06")]))
        lib.merge("sym", source, strategy=MergeStrategy(not_matched=MergeAction.DO_NOTHING))

        expected = pd.DataFrame({"a": [0, 1, 2, 30, 4, 50, 6], "b": [0.0, 1.0, 2.0, 31.0, 4.0, 51.0, 6.0]})
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 4, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 4
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_merge_upgrade_column_slicing(self, lmdb_library_factory, monkeypatch):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(columns_per_segment=2))
        target = pd.DataFrame({f"col_{i}": range(10) for i in range(5)}, index=pd.date_range("2024-01-01", periods=10))
        lib.write("sym", target)

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)

        source = pd.DataFrame(
            {f"col_{i}": [20 + i, 30 + i, 40 + i] for i in range(5)},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04"), pd.Timestamp("2024-01-07")])
        )
        lib.merge("sym", source, strategy=MergeStrategy(not_matched=MergeAction.DO_NOTHING))

        expected = target.copy(deep=True)
        for i in range(len(source)):
            expected.loc[source.index[i]] = source.iloc[i]

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        assert_frame_equal(lib.read("sym").data, expected)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 6, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 6
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_merge_update_column_and_row_slicing(self, lmdb_library_factory, monkeypatch):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(columns_per_segment=2, rows_per_segment=2))
        lt = lib._dev_tools.library_tool()
        target = pd.DataFrame({f"col_{i}": range(10) for i in range(5)}, index=pd.date_range("2024-01-01", periods=10))
        lib.write("sym", target)
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 15

        source = pd.DataFrame({f"col_{i}": [100 + i, 200 + i] for i in range(5)}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-05")]))
        expected = target.copy(deep=True)
        for i in range(len(source)):
            expected.loc[source.index[i]] = source.iloc[i]

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, strategy=MergeStrategy(not_matched=MergeAction.DO_NOTHING))

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 21, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 21
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_merge_update_on_index_and_column(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0]},
            index=pd.DatetimeIndex([
                "2024-01-01", # Matches index and column
                "2024-01-02 01:00:00", # Matches column, but not index
                "2024-01-03", # Matches index, but not column
                "2024-01-04" # Does not match either
        ]))
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["a"], strategy=MergeStrategy(not_matched=MergeAction.DO_NOTHING))

        expected = pd.DataFrame({"a": [1, 2, 3], "b": [10.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data

        assert_frame_equal(received, expected)

    def test_merge_update_multiple_columns(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({
            "a": [1, 2, 3, 4],
            "b": ["a", "b", "c", "d"],
            "c": ["A", "B", "A", "C"],
            "d": [10.1, 20.2, 30.3, 40.4],
            "e": [100, 200, 300, 400]
        }, index=pd.date_range("2024-01-01", periods=4))
        lib.write("sym", target)

        source = pd.DataFrame({
            "a": [10, 20, 30, 40, 50],
            "b": ["a", "b", "c", "d", "e"],
            "c": ["A", "D", "A", "B", "C"],
            "d": [10.1, 50.5, 30.3, 40.4, 70.7],
            "e": [100, 500, 600, 400, 800]
        }, index=pd.DatetimeIndex([
            "2024-01-01",      # index + b + d + e match (should update)
            "2024-01-02",      # index + b match, but d + e differ (do nothing)
            "2024-01-03",      # index + b + d match, e differs (do nothing)
            "2024-01-05",      # new index, b+d+e match (do nothing)
            "2024-01-06"       # new index, new b, new d, new e (do nothing)
        ]))

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["b", "d", "e"], strategy=MergeStrategy(not_matched=MergeAction.DO_NOTHING))

        expected = pd.DataFrame({
            "a": [10, 2, 3, 4],
            "b": ["a", "b", "c", "d"],
            "c": ["A", "B", "A", "C"],
            "d": [10.1, 20.2, 30.3, 40.4],
            "e": [100, 200, 300, 400]
        }, index=pd.date_range("2024-01-01", periods=4))

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_merge_update_row_from_source_matches_multiple_rows_from_target(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
        )
        lib.write("sym", target)
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        source = pd.DataFrame(
            {"a": [5], "b": [20.0]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])
        )
        lib.merge("sym", source, strategy=MergeStrategy(not_matched=MergeAction.DO_NOTHING))
        expected = pd.DataFrame(
            {"a": [5, 5, 3], "b": [20.0, 20.0, 3.0]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    @pytest.mark.parametrize("strategy", (
            MergeStrategy(MergeAction.UPDATE, MergeAction.DO_NOTHING),
            MergeStrategy(MergeAction.UPDATE, MergeAction.INSERT),
    ))
    def test_merge_update_throws_when_target_row_is_matched_more_than_once(self, lmdb_library, strategy, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"), # Matches first row of target
                pd.Timestamp("2024-01-01"), # Also matches first row of target
                pd.Timestamp("2024-01-03")
            ]))

        monkeypatch.setattr(lib.__class__, "merge", raise_wrapper(UserInputException), raising=False)
        with pytest.raises(UserInputException):
            lib.merge("sym", source, strategy=strategy)

    # ================================================================================================
    # ================================= TEST INSERT NOT MATCHED ======================================
    # ================================================================================================

    def test_merge_insert_not_matched(self, lmdb_library, monkeypatch):
        lib = lmdb_library

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, -2, -3], "b": [-1.0, -2.0, -3.0]},
            index=pd.DatetimeIndex(["2023-01-01", "2024-01-01 10:00:00", "2025-01-04"])
        )
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: VersionedItem(
            symbol=write_vit.symbol,
            library=write_vit.library,
            data=None,
            version=1,
            metadata=write_vit.metadata,
            host=write_vit.host,
            timestamp=write_vit.timestamp + 1,
        ), raising=False)

        merge_vit = lib.merge("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
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

    @pytest.mark.parametrize("date", (
            pd.Timestamp("2023-01-01"), # Before first value
            pd.Timestamp("2024-01-01 10:00:00"), # After first value and before second value
            pd.Timestamp("2024-01-02 10:00:00") # After second value
    ))
    def test_merge_insert_not_matched_in_full_segment(self, lmdb_library_factory, date, monkeypatch):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=2, columns_per_segment=2))
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3))
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
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 8, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 8
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_merge_insert_not_matched_writes_new_version_even_if_nothing_is_changed(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)
        monkeypatch.setattr(lib, "merge", lambda *args, **kwargs: VersionedItem(
            symbol=write_vit.symbol,
            library=write_vit.library,
            data=None,
            version=write_vit.version + 1,
            metadata=write_vit.metadata,
            host=write_vit.host,
            timestamp=write_vit.timestamp + 1,
        ), raising=False)
        source = pd.DataFrame({"a": [10, 20, 30], "b": [10.0, 20.0, 30.0]}, index=pd.date_range("2024-01-01", periods=3))
        merge_vit = lib.merge("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
        assert merge_vit.version == 1
        assert merge_vit.timestamp > write_vit.timestamp

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem(
            symbol=merge_vit.symbol,
            library=merge_vit.library,
            data=target,
            version=merge_vit.version,
            metadata=merge_vit.metadata,
            host=merge_vit.host,
            timestamp=merge_vit.timestamp,
        ))
        read_vit = lib.read("sym")
        assert_vit_equals_except_data(read_vit, merge_vit)
        assert_frame_equal(read_vit.data, target)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 1, KeyType.TABLE_INDEX: 1, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_merge_insert_not_matched_on_index_and_column(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0]},
            index=pd.DatetimeIndex([
                "2024-01-01", # Matches index and column
                "2024-01-02 01:00:00", # Matches column, but not index
                "2024-01-03", # Matches index, but not column
                "2024-01-04" # Does not match either
            ]))
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
        target = pd.DataFrame({
            "a": [1, 2, 3, 4],
            "b": ["a", "b", "c", "d"],
            "c": ["A", "B", "A", "C"],
            "d": [10.1, 20.2, 30.3, 40.4],
            "e": [100, 200, 300, 400]
        }, index=pd.date_range("2024-01-01", periods=4))
        lib.write("sym", target)

        source = pd.DataFrame({
            "a": [10, 20, 30, 40, 50],
            "b": ["a", "b", "c", "d", "e"],
            "c": ["A", "D", "A", "B", "C"],
            "d": [10.1, 50.5, 30.3, 40.4, 70.7],
            "e": [100, 500, 600, 400, 800]
        }, index=pd.DatetimeIndex([
            "2024-01-01",      # MATCH: index + b + d + e match
            "2024-01-02",      # NOT_MATCH: index + b match, but d + e differ
            "2024-01-03",      # NOT_MATCH: index + b + d match, e differs
            "2024-01-05",      # NOT_MATCH: new index, b+d+e match
            "2024-01-06"       # NOT_MATCH: new index, new b, new d, new e
        ]))

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["b", "d", "e"], strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))
        expected = pd.concat([target, source.tail(len(source) - 1)]).sort_index()
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_merge_insert_does_not_throw_when_target_row_is_matched_more_than_once_when_matched_is_do_nothing(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"), # Matches first row of target
                pd.Timestamp("2024-01-01"), # Also matches first row of target
                pd.Timestamp("2024-01-04")
            ]))

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, strategy=MergeStrategy(MergeAction.DO_NOTHING, MergeAction.INSERT))

    # ================================================================================================
    # ======================================== TEST UPSERT ===========================================
    # ================================================================================================

    def test_upsert(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, 20, -2], "b": [-1.1, 20.1, -3.1], "c": ["a", "c", "d"]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-01 15:00:00"), pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02 01:00:00")])
        )

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: VersionedItem(
            symbol=write_vit.symbol,
            library=write_vit.library,
            data=None,
            version=1,
            metadata=write_vit.metadata,
            host=write_vit.host,
            timestamp=write_vit.timestamp + 1,
        ), raising=False)

        merge_vit = lib.merge("sym", source)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        expected = pd.DataFrame({
                "a": [1, -1, 20, -2, 3],
                "b": [1.0, -1.1, 20.1, -3.1, 3.0],
                "c": ["a", "a", "c", "d", "c"]
            }, index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01 15:00:00"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02 01:00:00"),
                pd.Timestamp("2024-01-03")
            ])
        )

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem(
            symbol=merge_vit.symbol,
            library=merge_vit.library,
            data=expected,
            version=merge_vit.version,
            metadata=merge_vit.metadata,
            host=merge_vit.host,
            timestamp=merge_vit.timestamp,
        ))

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(read_vit, merge_vit)
        assert_frame_equal(read_vit.data, expected)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 2, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize("slicing_policy", [
        {"rows_per_segment": 2},
        {"columns_per_segment": 2},
        {"rows_per_segment": 2, "columns_per_segment": 2}
    ])
    def test_upsert_with_slicing(self, lmdb_library_factory, monkeypatch, slicing_policy):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(**slicing_policy))
        target = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [1.0, 2.0, 3.0, 4.0, 5.0], "c": ["a", "b", "c", "d", "e"]}, index=pd.date_range("2024-01-01", periods=5))
        lib.write("sym", target)

        # Insertion pushes the row to be updated into a new segment
        source = pd.DataFrame({"a": [-1, 40], "b": [-1.1, 40.4], "c": ["a", "f"]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-03 15:00:00"), pd.Timestamp("2024-01-04")]))
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source)

        expected = pd.DataFrame(
            {"a": [1, 2, 3, -1, 40, 5], "b": [1.0, 2.0, 3.0, -1.1, 40.4, 5.0], "c": ["a", "b", "c", "a", "f", "e"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-03 15:00:00"),
                pd.Timestamp("2024-01-04"),
                pd.Timestamp("2024-01-05")
            ])
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
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: expected_data_keys, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == expected_data_keys
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_upsert_on_index_and_column(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]},
            index=pd.date_range("2024-01-01", periods=3)
        )
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0], "c": ["A", "B", "A", "C"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"), # Matches index and column
                pd.Timestamp("2024-01-02 01:00:00"), # Matches column, but not index
                pd.Timestamp("2024-01-03"), # Matches index, but not column
                pd.Timestamp("2024-01-04") # Does not match either
            ])
        )
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["a"])

        expected = pd.DataFrame(
            {"a": [1, 2, 2, 3, 30, 40], "b": [10.0, 2.0, 20.0, 3.0, 30.0, 40.0], "c": ["A", "b", "B", "c", "A", "C"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02 01:00:00"),
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-04")
            ])
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data

        assert_frame_equal(received, expected)

    def test_upsert_multiple_columns(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({
            "a": [1, 2, 3, 4],
            "b": ["a", "b", "c", "d"],
            "c": ["A", "B", "A", "C"],
            "d": [10.1, 20.2, 30.3, 40.4],
            "e": [100, 200, 300, 400]
        }, index=pd.date_range("2024-01-01", periods=4))
        lib.write("sym", target)

        source = pd.DataFrame({
            "a": [10, 20, 30, 40, 50],
            "b": ["a", "b", "c", "d", "e"],
            "c": ["A", "D", "A", "B", "C"],
            "d": [10.1, 50.5, 30.3, 40.4, 70.7],
            "e": [100, 500, 600, 400, 800]
        }, index=pd.DatetimeIndex([
            "2024-01-01",      # index + b + d + e match (update)
            "2024-01-02",      # index + b match, but d + e differ (insert)
            "2024-01-03",      # index + b + d match, e differs (insert)
            "2024-01-05",      # new index, b+d+e match (insert)
            "2024-01-06"       # new index, new b, new d, new e (insert)
        ]))

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["b", "d", "e"], strategy=MergeStrategy(not_matched=MergeAction.DO_NOTHING))
        expected = pd.DataFrame({
                "a": [10, 2, 20, 3, 30, 4, 40, 50],
                "b": ["a", "b", "b", "c", "c", "d", "d", "e"],
                "c": ["A", "B", "D", "A", "A", "C", "B", "C"],
                "d": [10.1, 20.2, 50.5, 30.3, 30.3, 40.4, 40.4, 70.7],
                "e": [100, 200, 500, 300, 600, 400, 400, 800]
            }, index=pd.DatetimeIndex([
                "2024-01-01",      # Updated: matched on b="a", d=10.1, e=100
                "2024-01-02",      # Original target row
                "2024-01-02",      # Inserted: no match for b="b", d=50.5, e=500
                "2024-01-03",      # Original target row
                "2024-01-03",      # Inserted: no match for b="c", d=30.3, e=600
                "2024-01-04",      # Original target row
                "2024-01-05",      # Inserted: no match for b="d", d=40.4, e=400 (different index)
                "2024-01-06"       # Inserted: completely new b="e", d=70.7, e=800
        ]))

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    # The tests below are testing INSERT on matched. The more time I spend with this functionality, the less convinced I
    # am that we need to implement it.
    # Maxim had a good example for why it might be necessary:
    # Imagine the user wants to have a table containing data from multiple data providers. Then one of the providers
    # has an outage. The user must be able to insert rows from that provider. The data might look like this:
    # timestamp   ticker   ask   bid ....
    # 01-01-2024  AAPL     100   99
    # 01-01-2024  MSFT     200   199
    # 01-02-2024  AAPL     101   100

    # Then some provider might have outage on 01-02-2024 and provide update. Then the user can call
    # lib.merge(sym, source, strategy=MergeStrategy(MergeAction.INSERT, MergeAction.DO_NOTHING)) to insert the data.

    # I think the example is flawed:
    # 1) If a table contains data from multiple providers, then there would be multiple rows for the same stock at the
    #    same timestamp. Without a provider column, how would you distinguish between them?
    # 2) If you have a provider column, then you can perform upsert lib.merge(sym, source, on=["provider"])
    # 3) What if nothing matches the index, then insert on matched will not do anything.
    # 4) What if the provider issues a dataframe with rows to be updated and inserted? It will create a "duplicate" of
    #    some rows which doesn't seem useful.

    # The main issue, in my opinion, is that we're trying to come up with non-standard behavior based on a prediction of
    # what the data might look like without having actual production data or client requests to support this.

    # I think it's better to have a separate insert call rather than overloading the merge semantics.

    # ================================================================================================
    # ==================================== TEST INSERT ON MATCH ======================================
    # ================================================================================================

    def test_matched_insert(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, 20, -2], "b": [-1.1, 20.1, -3.1], "c": ["a", "c", "d"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01 15:00:00"),
                pd.Timestamp("2024-01-02"), # Matched -> insert
                pd.Timestamp("2024-01-02 01:00:00")
            ])
        )

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: VersionedItem(
            symbol=write_vit.symbol,
            library=write_vit.library,
            data=None,
            version=1,
            metadata=write_vit.metadata,
            host=write_vit.host,
            timestamp=write_vit.timestamp + 1,
        ), raising=False)

        merge_vit = lib.merge("sym", source, strategy=MergeStrategy(MergeAction.INSERT, MergeAction.DO_NOTHING))
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        expected = pd.DataFrame({"a": [1, 2, 20, 3], "b": [1.0, 2.0, 20.1, 3.0], "c": ["a", "b", "c", "c"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"), # New row
                pd.Timestamp("2024-01-03")
            ])
        )

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem(
            symbol=merge_vit.symbol,
            library=merge_vit.library,
            data=expected,
            version=merge_vit.version,
            metadata=merge_vit.metadata,
            host=merge_vit.host,
            timestamp=merge_vit.timestamp,
        ))

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(read_vit, merge_vit)
        assert_frame_equal(read_vit.data, expected)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 2, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize("slicing_policy", [
        {"rows_per_segment": 2},
        {"columns_per_segment": 2},
        {"rows_per_segment": 2, "columns_per_segment": 2}
    ])
    def test_matched_insert_with_slicing(self, lmdb_library_factory, monkeypatch, slicing_policy):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(**slicing_policy))
        target = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5], "b": [1.0, 2.0, 3.0, 4.0, 5.0], "c": ["a", "b", "c", "d", "e"]},
            index=pd.date_range("2024-01-01", periods=5)
        )
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-3, -5], "b": [-3.3, -5.5], "c": ["C", "F"]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-05")])
        )
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, strategy=MergeStrategy(MergeAction.INSERT, MergeAction.DO_NOTHING))

        expected = pd.DataFrame(
            {"a": [1, 2, 3, -3, 4, 5, -5], "b": [1.0, 2.0, 3.0, -3.3, 4.0, 5.0, -5.5], "c": ["a", "b", "c", "C", "d", "e", "F"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-04"),
                pd.Timestamp("2024-01-05"),
                pd.Timestamp("2024-01-05")
            ])
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        assert_frame_equal(lib.read("sym").data, expected)

        lt = lib._dev_tools.library_tool()
        if "rows_per_segment" in slicing_policy and "columns_per_segment" in slicing_policy:
            # Start with 3 row slices and 2 column slices = 6 data keys
            # A value is inserted into the second segment with 2 column slices -> 2 data keys
            # Inserting the value spills into a new segment with 2 column slices -> 2 data keys
            # Inserting into a segment (does not spill) with 2 column slices -> 2 data keys
            expected_data_keys = 12
        elif "rows_per_segment" in slicing_policy:
            # Start with 3 row slices and no column slices -> 3 data keys
            # A value is inserted into the second segment -> 1 data key
            # Inserting the value spills into a new segment -> 1 data key
            # Inserting into a segment (does not spill) -> 1 data key
            expected_data_keys = 6
        elif "columns_per_segment" in slicing_policy:
            # Start with one row slice and 2 column slices -> 2 data keys
            # Two values get inserted into the segment -> 2 data keys
            expected_data_keys = 4
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: expected_data_keys, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == expected_data_keys
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_matched_insert_target_can_be_matched_by_multiple_source_rows(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, -10], "b": [-1.1, -10.1], "c": ["A", "AA"]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")])
        )
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, strategy=MergeStrategy(MergeAction.INSERT, MergeAction.DO_NOTHING))

        expected = pd.DataFrame(
            {"a": [1, -1, -10, 2, 3], "b": [1.0, -1.1, -10.1, 2.0, 3.0], "c": ["a", "A", "AA", "b", "c"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-03")
            ])
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        assert_frame_equal(lib.read("sym").data, expected)

    def test_matched_insert_on_index_and_columns(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]},
            index=pd.date_range("2024-01-01", periods=3)
        )
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0], "c": ["A", "B", "A", "C"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"), # Matches index and column -> insert
                pd.Timestamp("2024-01-02 01:00:00"), # Matches column, but not index -> nothing
                pd.Timestamp("2024-01-03"), # Matches index, but not column -> nothing
                pd.Timestamp("2024-01-04") # Does not match either -> nothing
            ])
        )
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["a"], strategy=MergeStrategy(MergeAction.INSERT, MergeAction.DO_NOTHING))

        expected = pd.DataFrame(
            {"a": [1, 1, 2, 3], "b": [1.0, 10.0, 2.0, 3.0], "c": ["a", "A", "b", "c"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"), # Inserted
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-03")
            ])
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data

        assert_frame_equal(received, expected)

    def test_matched_insert_on_multiple_columns(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({
            "a": [1, 2, 3, 4],
            "b": ["a", "b", "c", "d"],
            "c": ["A", "B", "A", "C"],
            "d": [10.1, 20.2, 30.3, 40.4],
            "e": [100, 200, 300, 400]
        }, index=pd.date_range("2024-01-01", periods=4))
        lib.write("sym", target)

        source = pd.DataFrame({
            "a": [10, 20, 30, 40, 50],
            "b": ["a", "b", "c", "d", "e"],
            "c": ["A", "D", "A", "B", "C"],
            "d": [10.1, 50.5, 30.3, 40.4, 70.7],
            "e": [100, 500, 600, 400, 800]
        }, index=pd.DatetimeIndex([
            "2024-01-01",      # MATCH: index + b + d + e match
            "2024-01-02",      # NOT_MATCH: index + b match, but d + e differ
            "2024-01-03",      # NOT_MATCH: index + b + d match, e differs
            "2024-01-05",      # NOT_MATCH: new index, b+d+e match
            "2024-01-06"       # NOT_MATCH: new index, new b, new d, new e
        ]))

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["b", "d", "e"], strategy=MergeStrategy(MergeAction.INSERT, MergeAction.DO_NOTHING))
        expected = pd.DataFrame({
                "a": [1, 10, 2, 3, 4],
                "b": ["a", "a", "b", "c", "d"],
                "c": ["A", "A", "B", "A", "C"],
                "d": [10.1, 10.1, 20.2, 30.3, 40.4],
                "e": [100, 100, 200, 300, 400]
            },
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-04"),
            ])
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    # ================================================================================================
    # ============================= TEST INSERT ON MATCH AND NOT MATCH ===============================
    # ================================================================================================

    def test_insert_matched_insert_not_matched(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, 20, -2], "b": [-1.1, 20.1, -3.1], "c": ["A", "C", "D"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01 15:00:00"), # Not matched -> insert
                pd.Timestamp("2024-01-02"), # Matched -> insert
                pd.Timestamp("2024-01-02 01:00:00") # Not matched -> insert
            ])
        )

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: VersionedItem(
            symbol=write_vit.symbol,
            library=write_vit.library,
            data=None,
            version=1,
            metadata=write_vit.metadata,
            host=write_vit.host,
            timestamp=write_vit.timestamp + 1,
        ), raising=False)

        merge_vit = lib.merge("sym", source, strategy=MergeStrategy(MergeAction.INSERT, MergeAction.INSERT))
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        expected = pd.DataFrame(
            {"a": [1, -1, 2, 20, -2, 3],"b": [1.0, -1.1, 2.0, 20.1, -3.1, 3.0], "c": ["a", "A", "b", "C", "D", "c"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01 15:00:00"), # Not matched -> insert
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"), # Matched -> insert
                pd.Timestamp("2024-01-02 01:00:00"), # Not matched -> insert
                pd.Timestamp("2024-01-03")
            ])
        )

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem(
            symbol=merge_vit.symbol,
            library=merge_vit.library,
            data=expected,
            version=merge_vit.version,
            metadata=merge_vit.metadata,
            host=merge_vit.host,
            timestamp=merge_vit.timestamp,
        ))

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(read_vit, merge_vit)
        assert_frame_equal(read_vit.data, expected)

        lt = lib._dev_tools.library_tool()
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: 2, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize("slicing_policy", [
        {"rows_per_segment": 2},
        {"columns_per_segment": 2},
        {"rows_per_segment": 2, "columns_per_segment": 2}
    ])
    def test_insert_matched_insert_not_matched_with_slicing(self, lmdb_library_factory, monkeypatch, slicing_policy):
        lib = lmdb_library_factory(arcticdb.LibraryOptions(**slicing_policy))
        target = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5], "b": [1.0, 2.0, 3.0, 4.0, 5.0], "c": ["a", "b", "c", "d", "e"]},
            index=pd.date_range("2024-01-01", periods=5)
        )
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-3, -30, -6], "b": [-3.3, -30.3, -6.6], "c": ["C", "D", "F"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-03 10:00:00"),
                pd.Timestamp("2024-01-06")
            ])
        )
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, strategy=MergeStrategy(MergeAction.INSERT, MergeAction.INSERT))

        expected = pd.DataFrame(
            {"a": [1, 2, 3, -3, -30, 4, 5, -5], "b": [1.0, 2.0, 3.0, -3.3, -30.3, 4.0, 5.0, -5.5], "c": ["a", "b", "c", "C", "D", "d", "e", "F"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-03"), # Original target row
                pd.Timestamp("2024-01-03"), # Matched -> insert,
                pd.Timestamp("2024-01-03 10:00:00"), # Not matched -> insert
                pd.Timestamp("2024-01-04"),
                pd.Timestamp("2024-01-05"), # Original target row
                pd.Timestamp("2024-01-06")  # Not matched -> insert
            ])
        )

        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        assert_frame_equal(lib.read("sym").data, expected)

        lt = lib._dev_tools.library_tool()
        if "rows_per_segment" in slicing_policy and "columns_per_segment" in slicing_policy:
            # Start with 3 row slices and 2 column slices = 6 data keys
            # A value is inserted into the second segment with 2 column slices -> 2 data keys
            # Inserting the value spills into a new segment with 2 column slices -> 2 data keys
            # Inserting into a segment (does not spill) with 2 column slices -> 2 data keys
            expected_data_keys = 12
        elif "rows_per_segment" in slicing_policy:
            # Start with 3 row slices and no column slices -> 3 data keys
            # A value is inserted into the second segment -> 1 data key
            # Inserting the value spills into a new segment -> 1 data key
            # Inserting into a segment (does not spill) -> 1 data key
            expected_data_keys = 6
        elif "columns_per_segment" in slicing_policy:
            # Start with one row slice and 2 column slices -> 2 data keys
            # Two values get inserted into the segment -> 2 data keys
            expected_data_keys = 4
        monkeypatch.setattr(lt, "find_keys_for_symbol", mock_find_keys_for_symbol({KeyType.TABLE_DATA: expected_data_keys, KeyType.TABLE_INDEX: 2, KeyType.VERSION: 2}))
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == expected_data_keys
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_insert_matched_insert_not_matched_target_can_be_matched_by_multiple_source_rows(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]}, index=pd.date_range("2024-01-01", periods=3))
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [-1, -10, -100], "b": [-1.1, -10.1, -100.1], "c": ["A", "AA", "AAA"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02 10:00:00")
            ])
        )
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, strategy=MergeStrategy(MergeAction.INSERT, MergeAction.INSERT))

        expected = pd.DataFrame(
            {"a": [1, -1, -10, 2, -100, 3], "b": [1.0, -1.1, -10.1, 2.0, -100.1, 3.0], "c": ["a", "A", "AA", "b", "AAA", "c"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02 10:00:00"),
                pd.Timestamp("2024-01-03")
            ])
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        assert_frame_equal(lib.read("sym").data, expected)

    def test_insert_matched_insert_not_matched_on_index_and_column(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame(
            {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]},
            index=pd.date_range("2024-01-01", periods=3)
        )
        lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 2, 30, 40], "b": [10.0, 20.0, 30.0, 40.0], "c": ["A", "B", "A", "C"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"), # Matches index and column -> insert
                pd.Timestamp("2024-01-02 01:00:00"), # Matches column, but not index -> insert
                pd.Timestamp("2024-01-03"), # Matches index, but not column -> insert
                pd.Timestamp("2024-01-04") # Does not match either -> insert
            ])
        )
        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["a"], strategy=MergeStrategy(MergeAction.INSERT, MergeAction.INSERT))

        expected = pd.DataFrame(
            {"a": [1, 1, 2, 2, 3, 30, 40], "b": [1.0, 10.0, 2.0, 20.0, 3.0, 30.0, 40.0], "c": ["a", "A", "b", "B", "c", "A", "C"]},
            index=pd.DatetimeIndex([
                pd.Timestamp("2024-01-01"),     # Original target row
                pd.Timestamp("2024-01-01"),     # Matched on column 'a'=1 -> insert
                pd.Timestamp("2024-01-02"),     # Original target row
                pd.Timestamp("2024-01-02 01:00:00"), # Matched on column 'a'=2 but not on index-> insert
                pd.Timestamp("2024-01-03"),     # Original target row
                pd.Timestamp("2024-01-03"),     # Matches index but, no match on 'a'=30 -> insert
                pd.Timestamp("2024-01-04")      # Does not match either -> insert
            ])
        )
        monkeypatch.setattr(lib, "read", lambda *args, **kwargs: VersionedItem("sym", "lib", expected, 2))
        received = lib.read("sym").data
        assert_frame_equal(received, expected)

    def test_insert_matched_insert_not_matched_on_multiple_columns(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        target = pd.DataFrame({
            "a": [1, 2, 3, 4],
            "b": ["a", "b", "c", "d"],
            "c": ["A", "B", "A", "C"],
            "d": [10.1, 20.2, 30.3, 40.4],
            "e": [100, 200, 300, 400]
        }, index=pd.date_range("2024-01-01", periods=4))
        lib.write("sym", target)

        source = pd.DataFrame({
            "a": [10, 20, 30, 40, 50],
            "b": ["a", "b", "c", "d", "e"],
            "c": ["A", "D", "A", "B", "C"],
            "d": [10.1, 50.5, 30.3, 40.4, 70.7],
            "e": [100, 500, 600, 400, 800]
        }, index=pd.DatetimeIndex([
            "2024-01-01", # MATCH: index + b + d + e match
            "2024-01-02", # NOT_MATCH: index + b match, but d + e differ
            "2024-01-03", # NOT_MATCH: index + b + d match, e differs
            "2024-01-05", # NOT_MATCH: new index, b+d+e match
            "2024-01-06"  # NOT_MATCH: new index, new b, new d, new e
        ]))

        monkeypatch.setattr(lib.__class__, "merge", lambda *args, **kwargs: None, raising=False)
        lib.merge("sym", source, on=["b", "d", "e"], strategy=MergeStrategy(MergeAction.INSERT, MergeAction.INSERT))

        expected = pd.DataFrame({
            "a": [1, 10, 2, 20, 3, 30, 4, 40, 50],
            "b": ["a", "a", "b", "b", "c", "c", "d", "d", "e"],
            "c": ["A", "A", "B", "D", "A", "A", "C", "B", "C"],
            "d": [10.1, 10.1, 20.2, 50.5, 30.3, 30.3, 40.4, 40.4, 70.7],
            "e": [100, 100, 200, 500, 300, 600, 400, 400, 800]
        }, index=pd.DatetimeIndex([
            pd.Timestamp("2024-01-01"),  # Original target row
            pd.Timestamp("2024-01-01"),  # Matched (b,d,e) -> insert
            pd.Timestamp("2024-01-02"),  # Original target row
            pd.Timestamp("2024-01-02"),  # Not matched -> insert
            pd.Timestamp("2024-01-03"),  # Original target row
            pd.Timestamp("2024-01-03"),  # Not matched -> insert
            pd.Timestamp("2024-01-04"),  # Original target row
            pd.Timestamp("2024-01-05"),  # Matched (b,d,e) diff index -> insert
            pd.Timestamp("2024-01-06"),  # Nothing matched -> insert
        ]))

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
                {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]},
                index=pd.MultiIndex.from_tuples([("A", 1), ("B", 2), ("C", 3)])
            )
            lib.write("sym", target)
            source = pd.DataFrame(
                {"a": [2], "b": [3.0]},
                index=pd.MultiIndex.from_tuples([("A", 1)])
            )
            monkeypatch.setattr(lib.__class__, "merge", raise_wrapper(UserInputException), raising=False)
            with pytest.raises(UserInputException):
                lib.merge("sym", source)


