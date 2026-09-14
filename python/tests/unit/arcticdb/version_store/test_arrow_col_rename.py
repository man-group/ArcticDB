"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.exceptions import SchemaException, UserInputException
from arcticdb.util.test import assert_frame_equal, assert_series_equal


@pytest.skip(reason="Monday 12844033169: Not implemented yet", allow_module_level=True)
def assert_norm_meta_arrow_compatible(lib, sym):
    tsd = lib.version_store.read_descriptor(sym, lib._get_version_query(None)).timeseries_descriptor
    col_names = [field.name for field in tsd.as_stream_descriptor.fields()]
    assert len(col_names) == len(set(col_names))
    norm_meta = tsd.normalization
    input_type = norm_meta.WhichOneof("input_type")
    if input_type == "df":
        assert not norm_meta.df.has_synthetic_columns
        common = norm_meta.df.common
        assert not common.has_name
    elif input_type == "series":
        assert not norm_meta.series.has_synthetic_columns
        common = norm_meta.series.common
        assert common.has_name
        assert common.name == col_names[-1]
    else:
        assert False
    assert not len(common.col_names)
    if common.WhichOneof("index_type") == "index":
        index = common.index
        assert not index.fake_name
        assert not index.is_int
        if index.is_physically_stored:
            assert index.name == col_names[0]
    else:  # multi_index
        index = common.multi_index
        assert not index.is_int
        assert not len(index.fake_field_pos)
        assert index.name == col_names[0]


@pytest.mark.parametrize("method_arg", [5, [], [5, "hello"]])
def test_bad_arguments(in_memory_version_store, method_arg):
    lib = in_memory_version_store
    sym = "test_bad_arguments"
    with pytest.raises(UserInputException):
        lib.rename_columns_arrow_compat(sym, method_arg)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
# Empty string names have special norm metadata in ArcticDB, but are allowed by Arrow without modification
# Current sparrow version doesn't support empty string column names though, see test_write_empty_column_name_fails
@pytest.mark.parametrize("col_name", [None, "", 10])
def test_arrow_col_rename_basic(in_memory_version_store, object_type, col_name):
    lib = in_memory_version_store
    sym = "test_arrow_col_rename_basic"
    input = pd.DataFrame({col_name: [0]}) if object_type == "DataFrame" else pd.Series([0], name=col_name)
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected = pd.DataFrame({str(col_name): [0]}) if object_type == "DataFrame" else pd.Series([0], name=str(col_name))
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


def test_arrow_col_rename_duplicates(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_arrow_col_rename_duplicates"
    input = pd.DataFrame(np.zeros((1, 12)), columns=["col", "col", "col", "", "", "", None, None, "None", 10, "10", 10])
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    # Note that None and int column names are stringified prior to deduplication from left-to-right, hence the order
    # "None", "_None_", "__None__", "10", "_10_", "__10__" in the output, even though some columns were strings already
    expected = pd.DataFrame(
        np.zeros((1, 12)),
        columns=["col", "_col_", "__col__", "", "__", "____", "None", "_None_", "__None__", "10", "_10_", "__10__"],
    )
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_arrow_col_rename_synthetic_columns(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_arrow_col_rename_synthetic_columns"
    input = pd.DataFrame(np.zeros((1, 10))) if object_type == "DataFrame" else pd.Series([0])
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected = (
        pd.DataFrame(
            np.zeros((1, 10)),
            columns=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
        )
        if object_type == "DataFrame"
        else pd.Series([0], name="0")
    )
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_single_index_auto_rename_int_no_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_int_no_clash"
    input = (
        pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)])
    )
    input.index.name = 10
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "10"
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_single_index_auto_rename_int_one_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_int_one_clash"
    input = (
        pd.DataFrame({"10": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name="10")
    )
    input.index.name = 10
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "_10_"
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


def test_single_index_auto_rename_int_multiple_clashes(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_int_multiple_clashes"
    input = pd.DataFrame({"10": [0], 10: [1]}, index=[pd.Timestamp(0)])
    input.index.name = 10
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected = pd.DataFrame({"10": [0], "_10_": [1]}, index=[pd.Timestamp(0)])
    expected.index.name = "__10__"
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_single_index_auto_rename_nameless_no_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_nameless_no_clash"
    input = (
        pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)])
    )
    assert input.index.name is None
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "index"
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("col_name", [None, "", 10, "index", "hello"])
def test_single_index_auto_rename_index_and_col_name_same(in_memory_version_store, object_type, col_name):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_index_and_col_name_same"
    input = (
        pd.DataFrame({col_name: [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name=col_name)
    )
    input.index.name = col_name
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected = (
        pd.DataFrame({str(col_name): [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name=str(col_name))
    )
    expected.index.name = f"_{col_name}_"
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_single_index_auto_rename_nameless_one_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_nameless_one_clash"
    input = (
        pd.DataFrame({"index": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name="index")
    )
    assert input.index.name is None
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "_index_"
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


def test_single_index_auto_rename_nameless_multiple_clashes(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_nameless_multiple_clashes"
    input = pd.DataFrame(np.zeros((1, 2)), columns=["index", "index"], index=[pd.Timestamp(0)])
    assert input.index.name is None
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected = pd.DataFrame(np.zeros((1, 2)), columns=["index", "_index_"], index=[pd.Timestamp(0)])
    expected.index.name = "__index__"
    assert_frame_equal(received, expected)
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("input_index_name", [None, "my_index"])
# Single-element lists allowed for timeseries index
@pytest.mark.parametrize("method_arg", ["ts", ["ts"]])
def test_single_index_explicit_rename_nameless_no_clash(
    in_memory_version_store, object_type, input_index_name, method_arg
):
    lib = in_memory_version_store
    sym = "test_single_index_explicit_rename_nameless_no_clash"
    input = (
        pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)])
    )
    input.index.name = input_index_name
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym, method_arg)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "ts"
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_single_index_explicit_rename_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_single_index_explicit_rename_clash"
    input = (
        pd.DataFrame({"ts": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name="ts")
    )
    lib.write(sym, input)
    with pytest.raises(SchemaException):
        lib.rename_columns_arrow_compat(sym, "ts")


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_single_index_too_many_index_names(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_single_index_too_many_index_names"
    input = (
        pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)])
    )
    lib.write(sym, input)
    with pytest.raises(UserInputException):
        lib.rename_columns_arrow_compat(sym, ["level0", "level1"])


# These need to be compat tests, as is_int is always false in PandasMultiIndex since July 2026
@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_multi_index_auto_rename_int_no_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_int_no_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=[10, "level1"])
    input = pd.DataFrame({"col": [0]}, index=index) if object_type == "DataFrame" else pd.Series([0], index=index)
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["10", "level1"])
    expected = (
        pd.DataFrame({"col": [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index)
    )
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_multi_index_auto_rename_int_one_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_int_one_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=[10, "level1"])
    input = (
        pd.DataFrame({"10": [0]}, index=index) if object_type == "DataFrame" else pd.Series([0], index=index, name="10")
    )
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["_10_", "level1"])
    expected = (
        pd.DataFrame({"col": [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index)
    )
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


def test_multi_index_auto_rename_int_multiple_clashes(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_int_multiple_clashes"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=[10, 10])
    input = pd.DataFrame({"10": [0], 10: [1]}, index=index)
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["__10__", "___10___"])
    expected = pd.DataFrame({"10": [0], "_10_": [1]}, index=expected_index)
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_multi_index_auto_rename_nameless_no_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_nameless_no_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]])
    input = pd.DataFrame({"col": [0]}, index=index) if object_type == "DataFrame" else pd.Series([0], index=index)
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["index_level_0", "index_level_1"])
    expected = (
        pd.DataFrame({"col": [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index)
    )
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize(
    "input_names,output_names",
    [
        pytest.param([None, None], ["_index_level_0_", "_index_level_1_"]),
        pytest.param([None, "index_level_0"], ["__index_level_0__", "_index_level_0_"]),
        pytest.param(["index_level_1", None], ["index_level_1", "_index_level_0_"]),
    ],
)
def test_multi_index_auto_rename_nameless_clashes(in_memory_version_store, object_type, input_names, output_names):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_nameless_clashes"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=input_names)
    input = (
        pd.DataFrame({"index_level_0": [0]}, index=index)
        if object_type == "DataFrame"
        else pd.Series([0], index=index, name="index_level_0")
    )
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=output_names)
    expected = (
        pd.DataFrame({"index_level_0": [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index, name="index_level_0")
    )
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("input_names", [[None, None], ["level0", "level1"]])
def test_multi_index_explicit_rename_no_clash(in_memory_version_store, object_type, input_names):
    lib = in_memory_version_store
    sym = "test_multi_index_explicit_rename_nameless_no_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=input_names)
    input = pd.DataFrame({"col": [0]}, index=index) if object_type == "DataFrame" else pd.Series([0], index=index)
    lib.write(sym, input)
    lib.rename_columns_arrow_compat(sym, ["my_level_0", "my_level_1"])
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["my_level_0", "my_level_1"])
    expected = (
        pd.DataFrame({"col": [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index)
    )
    (
        assert_frame_equal(received, expected)
        if isinstance(expected, pd.DataFrame)
        else assert_series_equal(received, expected)
    )
    assert_norm_meta_arrow_compatible(lib, sym)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("method_arg", [["col", "level1"], ["level0", "col"]])
def test_multi_index_explicit_rename_clash(in_memory_version_store, object_type, method_arg):
    lib = in_memory_version_store
    sym = "test_multi_index_explicit_rename_no_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]])
    input = pd.DataFrame({"col": [0]}, index=index) if object_type == "DataFrame" else pd.Series([0], index=index)
    lib.write(sym, input)
    with pytest.raises(SchemaException):
        lib.rename_columns_arrow_compat(sym, method_arg)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("method_arg", [["level0"], ["level0", "level1", "level2"]])
def test_multi_index_incorrect_index_name_count(in_memory_version_store, object_type, method_arg):
    lib = in_memory_version_store
    sym = "test_multi_index_incorrect_index_name_count"
    index = pd.MultiIndex.from_arrays([[0], [1]])
    input = pd.DataFrame({"col": [0]}, index=index) if object_type == "DataFrame" else pd.Series([0], index=index)
    lib.write(sym, input)
    with pytest.raises(UserInputException):
        lib.rename_columns_arrow_compat(sym, method_arg)
