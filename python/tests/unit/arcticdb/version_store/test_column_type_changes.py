"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.version_store import StreamDescriptorMismatch
from arcticdb.util.test import assert_frame_equal


@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_changing_numeric_type(version_store_factory, dynamic_schema):
    lib = version_store_factory(dynamic_schema=dynamic_schema)
    sym_append = "test_changing_numeric_type_append"
    sym_update = "test_changing_numeric_type_update"
    df_write = pd.DataFrame({"col": np.arange(3, dtype=np.uint8)}, index=pd.date_range("2024-01-01", periods=3))
    df_append = pd.DataFrame({"col": np.arange(1, dtype=np.uint16)}, index=pd.date_range("2024-01-04", periods=1))
    df_update = pd.DataFrame({"col": np.arange(1, dtype=np.uint16)}, index=pd.date_range("2024-01-02", periods=1))

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

        expected_update = pd.DataFrame({"col": np.array([0, 0, 2], dtype=np.uint16)}, index=pd.date_range("2024-01-01", periods=3))
        received_update = lib.read(sym_update).data
        assert_frame_equal(expected_update, received_update)


@pytest.mark.parametrize("dynamic_schema, dynamic_strings_first", [
    (True, True),
    (True, False),
    pytest.param(False,
                 True,
                 marks=pytest.mark.xfail(
                     reason="""Issue with appending/updating a dynamic string column with fixed-width strings
                     https://github.com/man-group/ArcticDB/issues/1204"""
                 )
                 ),
    (False, False),
])
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