"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.version_store import StreamDescriptorMismatch
from arcticdb_ext.storage import KeyType
from arcticdb.util.test import assert_frame_equal
from arcticdb_ext.types import DataType



@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_changing_numeric_type(version_store_factory, dynamic_schema):
    lib = version_store_factory(dynamic_schema=dynamic_schema)
    sym_append = "test_changing_numeric_type_append"
    sym_update = "test_changing_numeric_type_update"
    df_write = pd.DataFrame({"col": np.arange(3, dtype=np.uint32)}, index=pd.date_range("2024-01-01", periods=3))
    df_append = pd.DataFrame({"col": np.arange(1, dtype=np.int32)}, index=pd.date_range("2024-01-04", periods=1))
    df_update = pd.DataFrame({"col": np.arange(1, dtype=np.int32)}, index=pd.date_range("2024-01-02", periods=1))

    lib.write(sym_append, df_write)
    lib.write(sym_update, df_write)

    if not dynamic_schema:
        with pytest.raises(StreamDescriptorMismatch):
            lib.append(sym_append, df_append)
        with pytest.raises(StreamDescriptorMismatch):
            lib.update(sym_update, df_update)
    else:
        lib.append(sym_append, df_append)
        lib.update(sym_update, df_update)

        expected_append = pd.concat([df_write, df_append])
        received_append = lib.read(sym_append).data
        assert_frame_equal(expected_append, received_append)

        expected_update = pd.DataFrame({"col": np.array([0, 0, 2], dtype=np.int64)}, index=pd.date_range("2024-01-01", periods=3))
        received_update = lib.read(sym_update).data
        assert_frame_equal(expected_update, received_update)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("dynamic_strings_first", [True, False])
def test_changing_string_type(version_store_factory, dynamic_schema, dynamic_strings_first):
    lib = version_store_factory(dynamic_strings=True, dynamic_schema=dynamic_schema)
    sym_append = "test_changing_string_type_append"
    sym_update = "test_changing_string_type_update"
    df_write = pd.DataFrame({"col": ["a", "bb", "ccc"]}, index=pd.date_range("2024-01-01", periods=3))
    df_append = pd.DataFrame({"col": ["dddd"]}, index=pd.date_range("2024-01-04", periods=1))
    df_update = pd.DataFrame({"col": ["dddd"]}, index=pd.date_range("2024-01-02", periods=1))

    lib.write(sym_append, df_write, dynamic_strings=dynamic_strings_first)
    lib.write(sym_update, df_write, dynamic_strings=dynamic_strings_first)

    lib.append(sym_append, df_append, dynamic_strings=not dynamic_strings_first)
    lib.update(sym_update, df_update, dynamic_strings=not dynamic_strings_first)

    expected_append = pd.concat([df_write, df_append])
    received_append = lib.read(sym_append).data
    assert_frame_equal(expected_append, received_append)

    expected_update = pd.DataFrame({"col": ["a", "dddd", "ccc"]}, index=pd.date_range("2024-01-01", periods=3))
    received_update = lib.read(sym_update).data
    assert_frame_equal(expected_update, received_update)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("wider_strings_first", [True, False])
def test_changing_fixed_string_width(version_store_factory, dynamic_schema, wider_strings_first):
    lib = version_store_factory(dynamic_strings=False, dynamic_schema=dynamic_schema)
    sym_append = "test_changing_fixed_string_width_append"
    sym_update = "test_changing_fixed_string_width_update"
    df_write = pd.DataFrame({"col": ["aa", "bb", "cc"]}, index=pd.date_range("2024-01-01", periods=3))
    df_append = pd.DataFrame({"col": ["d" * (1 if wider_strings_first else 3)]}, index=pd.date_range("2024-01-04", periods=1))
    df_update = pd.DataFrame({"col": ["d" * (1 if wider_strings_first else 3)]}, index=pd.date_range("2024-01-02", periods=1))

    lib.write(sym_append, df_write)
    lib.write(sym_update, df_write)

    lib.append(sym_append, df_append)
    lib.update(sym_update, df_update)

    expected_append = pd.concat([df_write, df_append])
    received_append = lib.read(sym_append).data
    assert_frame_equal(expected_append, received_append)

    expected_update = pd.DataFrame({"col": ["aa", "d" * (1 if wider_strings_first else 3), "cc"]}, index=pd.date_range("2024-01-01", periods=3))
    received_update = lib.read(sym_update).data
    assert_frame_equal(expected_update, received_update)


def test_type_promotion_stored_in_index_key(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    lib_tool = lib.library_tool()
    sym = "symbol"
    col = "column"

    def get_type_of_column():
        index_key = lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, sym)[-1]
        tsd = lib_tool.read_timeseries_descriptor(index_key)
        type_desc = [field.type() for field in tsd.fields() if field.name() == col][0]
        return type_desc.data_type()

    df_write = pd.DataFrame({col: [1, 2]}, dtype="int8", index=pd.date_range("2024-01-01", periods=2))
    lib.write(sym, df_write)
    assert get_type_of_column() == DataType.INT8

    df_append_1 = pd.DataFrame({col: [3, 4]}, dtype="int32", index=pd.date_range("2024-01-03", periods=2))
    lib.append(sym, df_append_1)
    assert get_type_of_column() == DataType.INT32

    df_append_2 = pd.DataFrame({col: [5, 6]}, dtype="float64", index=pd.date_range("2024-01-05", periods=2))
    lib.append(sym, df_append_2)
    assert get_type_of_column() == DataType.FLOAT64

    result_df = lib.read(sym).data
    expected_df = pd.concat([df_write, df_append_1, df_append_2])
    assert_frame_equal(result_df, expected_df)