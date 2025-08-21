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
from arcticdb_ext.storage import KeyType


class MergeAction(Enum):
    UPDATE = 1
    INSERT = 2,
    DO_NOTHING = 3

def append_tuple_row(target, row):
    columns = target.columns
    row_as_df = pd.DataFrame([row._asdict()], columns=columns, index=[row.Index])
    row_as_df.index.name = target.index.name
    return pd.concat([target, row_as_df])


def merge(self, sym, source, metadata=None, on=None, action=(MergeAction.UPDATE, MergeAction.DO_NOTHING)):
    on = on or []
    target_vit = self.read(sym)
    if len(source) == 0:
        return target_vit
    target = target_vit.data
    result = pd.DataFrame(columns=target.columns, index=pd.Index([], dtype=target.index.dtype)).astype(target.dtypes)
    result.index.name = target.index.name

    for target_row in target.itertuples():
        matching_source_row = None
        for source_row in source.itertuples():
            if target_row[0] == source_row[0] and all([getattr(source_row, col) == getattr(target_row, col) for col in on]):
                matching_source_row = source_row
                break
        if matching_source_row is not None:
            # On matched
            if action[0] == MergeAction.UPDATE:
                result = append_tuple_row(result, matching_source_row)
            elif action[0] == MergeAction.INSERT:
                result = append_tuple_row(result, target_row)
                result = append_tuple_row(result, matching_source_row)
        else:
            result = append_tuple_row(result, target_row)
    return self.write(sym, result, metadata=metadata)
class TestMergeTimeseries:

    def test_merge_update_on_index(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        monkeypatch.setattr(lib.__class__, "merge", merge, raising=False)

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]},
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"])
        )

        merge_vit = lib.merge("sym", source)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
        assert merge_vit.metadata == write_vit.metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        expected = pd.DataFrame({"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)

        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    def test_merge_update_on_index_with_metadata(self, lmdb_library, monkeypatch):
        lib = lmdb_library
        monkeypatch.setattr(lib.__class__, "merge", merge, raising=False)

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)

        source = pd.DataFrame(
            {"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]},
            index=pd.DatetimeIndex(["2024-01-01 10:00:00", "2024-01-02", "2024-01-04"])
        )

        metadata = {"meta": "data"}
        merge_vit = lib.merge("sym", source, metadata=metadata)
        assert merge_vit.version == 1
        assert merge_vit.symbol == write_vit.symbol
        assert merge_vit.timestamp > write_vit.timestamp
        assert merge_vit.metadata == metadata
        assert merge_vit.library == write_vit.library
        assert merge_vit.host == write_vit.host
        assert merge_vit.data is None

        expected = pd.DataFrame({"a": [1, 20, 3], "b": [1.0, 20.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))

        read_vit = lib.read("sym")
        assert_vit_equals_except_data(merge_vit, read_vit)
        assert_frame_equal(read_vit.data, expected)

        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 2
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 2

    @pytest.mark.parametrize("metadata", ({"meta": "data"}, None))
    def test_merge_update_does_not_write_new_version_with_empty_source(self, lmdb_library, metadata, monkeypatch):
        lib = lmdb_library
        monkeypatch.setattr(lib.__class__, "merge", merge, raising=False)

        target = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3))
        write_vit = lib.write("sym", target)
        merge_vit = lib.merge("sym", pd.DataFrame(), metadata=metadata)
        assert_vit_equals_except_data(write_vit, merge_vit)
        assert merge_vit.data is None and write_vit.data is None
        lt = lib._dev_tools.library_tool()
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.TABLE_INDEX, "sym")) == 1
        assert len(lt.find_keys_for_symbol(KeyType.VERSION, "sym")) == 1

