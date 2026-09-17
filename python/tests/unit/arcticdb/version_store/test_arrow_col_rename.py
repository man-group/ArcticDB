"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb.exceptions import NoSuchVersionException, SchemaException, UserInputException
from arcticdb.options import OutputFormat
from arcticdb.util.test import assert_frame_equal, assert_pandas_equal


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
        # The on-disk field name can differ from the declared pandas-level name for values like "" that get a
        # special on-disk placeholder key for disambiguation purposes (see col_names below)
        name_col_entry = common.col_names.get(col_names[-1])
        if name_col_entry.is_empty:
            assert common.name == ""
        else:
            assert common.name == col_names[-1]
    empty_col_count = 0
    for col_name, col_meta in common.col_names.items():
        assert not col_meta.is_int
        assert not col_meta.is_none
        # Ideally we would store empty string column names as "" with col_meta.is_empty == False. However, there are
        # C++ level checks (e.g. SegmentInMemoryImpl::column_index) that would prevent this data being read by older
        # clients, which is not a problem with 5 -> "5" or None -> "None". Additionally, sparrow does not support
        # empty strings as column names (checked 16/9/26) due to an assertion in record_batch::check_consistency().
        if col_meta.is_empty:
            empty_col_count += 1
            assert col_name.startswith("__empty__")
        else:
            assert col_name == col_meta.original_name
    assert empty_col_count <= 1
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
        for idx in range(1, index.field_count + 1):
            assert col_names[idx].startswith("__idx__")


def generic_rename_columns_arrow_compat_test(lib, sym, index_columns=None):
    before = lib.read(sym, output_format=OutputFormat.PYARROW)
    before_data, before_metadata, before_version = before.data, before.metadata, before.version
    vit = lib.rename_columns_arrow_compat(sym, index_columns)
    assert_norm_meta_arrow_compatible(lib, sym)
    after = lib.read(sym, output_format=OutputFormat.PYARROW).data
    assert vit.version == before_version + 1
    assert before_metadata == vit.metadata
    if isinstance(index_columns, str):
        before_data = before_data.set_column(0, index_columns, before_data.column(0))
    elif isinstance(index_columns, list):
        for idx, index_name in enumerate(index_columns):
            before_data = before_data.set_column(idx, index_name, before_data.column(idx))
    assert before_data.equals(after)
    # Idempotent
    after_pandas = lib.read(sym, output_format=OutputFormat.PANDAS).data
    vit_idempotent = lib.rename_columns_arrow_compat(sym, index_columns)
    # TODO: Uncomment this once implemented
    # assert vit_idempotent == vit
    after_after_pandas = lib.read(sym, output_format=OutputFormat.PANDAS).data
    assert_pandas_equal(after_after_pandas, after_pandas)


@pytest.mark.parametrize("index_columns", [5, [], [5, "hello"]])
def test_bad_arguments(in_memory_version_store, index_columns):
    lib = in_memory_version_store
    sym = "test_bad_arguments"
    with pytest.raises(UserInputException):
        lib.rename_columns_arrow_compat(sym, index_columns)


# Dynamic schema uses different name-mangling (appends _0 instead of _n where n is the column index) as column index is
# not stable on append/update
@pytest.mark.parametrize("dynamic_schema", [False, True])
@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("col_name", [None, "", 10])
def test_arrow_col_rename_basic(in_memory_store_factory, dynamic_schema, object_type, col_name):
    lib = in_memory_store_factory(dynamic_schema=dynamic_schema)
    sym = "test_arrow_col_rename_basic"
    input = pd.DataFrame({col_name: [0]}) if object_type == "DataFrame" else pd.Series([0], name=col_name)
    lib.write(sym, input, metadata="hello")
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    if object_type == "DataFrame":
        expected = pd.DataFrame({str(col_name): [0]})
    else:
        expected = pd.Series([0], name="" if col_name is None else str(col_name))
    assert_pandas_equal(received, expected)


def test_arrow_col_rename_previous_version_unchanged(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_arrow_col_rename_previous_version_unchanged"
    df_0 = pd.DataFrame({10: [0]})
    lib.write(sym, df_0)
    lib.rename_columns_arrow_compat(sym, prune_previous_version=False)
    assert len(lib.list_versions(sym)) == 2
    previous_df = lib.read(sym, as_of=0).data
    assert_frame_equal(previous_df, df_0)
    assert previous_df.columns[0] == 10


@pytest.mark.xfail(reason="Not yet implemented", strict=True)
def test_arrow_col_rename_valid_schema_is_noop(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_arrow_col_rename_valid_schema_is_noop"
    # This dataframe has schema that is already valid with Arrow
    df = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    df.index.name = "ts"
    lib.write(sym, df)
    assert lib.rename_columns_arrow_compat(sym).version == 0
    assert lib.read_metadata(sym).version == 0


def test_arrow_col_rename_duplicates(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_arrow_col_rename_duplicates"
    input = pd.DataFrame(np.zeros((1, 12)), columns=["col", "col", "col", "", "", "", None, None, "None", 10, "10", 10])
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    # Note that None and int column names are stringified prior to deduplication from left-to-right, hence the order
    # "None", "_None_", "__None__", "10", "_10_", "__10__" in the output, even though some columns were strings already
    expected = pd.DataFrame(
        np.zeros((1, 12)),
        columns=["col", "_col_", "__col__", "", "__", "____", "None", "_None_", "__None__", "10", "_10_", "__10__"],
    )
    assert_pandas_equal(received, expected)


def test_arrow_col_rename_synthetic_columns(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_arrow_col_rename_synthetic_columns"
    input = pd.DataFrame(np.zeros((1, 10)))
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = pd.DataFrame(np.zeros((1, 10)), columns=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"])
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_single_index_auto_rename_int_no_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_int_no_clash"
    input = (
        pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name="col")
    )
    input.index.name = 10
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "10"
    assert_pandas_equal(received, expected)


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
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    # The index's real name takes the unwrapped slot unconditionally, since it is processed before the data
    # columns; the data column is processed afterwards and wrapped to avoid the resulting clash
    expected = (
        pd.DataFrame({"_10_": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name="_10_")
    )
    expected.index.name = "10"
    assert_pandas_equal(received, expected)


def test_single_index_auto_rename_int_multiple_clashes(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_int_multiple_clashes"
    input = pd.DataFrame({"10": [0], 10: [1]}, index=[pd.Timestamp(0)])
    input.index.name = 10
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    # The index's real name takes the unwrapped slot "10" unconditionally; both data columns then wrap to avoid it
    expected = pd.DataFrame({"_10_": [0], "__10__": [1]}, index=[pd.Timestamp(0)])
    expected.index.name = "10"
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_single_index_auto_rename_nameless_no_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_nameless_no_clash"
    input = (
        pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name="col")
    )
    assert input.index.name is None
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "__index__"
    assert_pandas_equal(received, expected)


# Empty DatetimeIndex frames are not marked as having a physically stored index, but still have an index column
@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("index_name", [None, "ts"])
def test_single_index_auto_rename_empty_frame(in_memory_version_store, object_type, index_name):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_empty_frame"
    index = pd.DatetimeIndex([], name=index_name)
    values = np.array([], dtype=np.int64)
    input = (
        pd.DataFrame({"col": values}, index=index)
        if object_type == "DataFrame"
        else pd.Series(values, index=index, name="col")
    )
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "__index__" if index_name is None else index_name
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("col_name", [None, "", 10, "__index__", "hello"])
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
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    if col_name is None:
        index_name = "__index__"
        # An unnamed Series is renamed to "" (the polars convention for unnamed Series) rather than "None"
        col_display_name = "" if object_type == "Series" else "None"
    else:
        # The index's real name takes the unwrapped slot unconditionally, since it is processed before the data
        # columns; the data column is processed afterwards and wrapped to avoid the resulting clash
        index_name = str(col_name)
        col_display_name = f"_{col_name}_"
    expected = (
        pd.DataFrame({col_display_name: [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name=col_display_name)
    )
    expected.index.name = index_name
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_single_index_auto_rename_nameless_one_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_nameless_one_clash"
    input = (
        pd.DataFrame({"__index__": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name="__index__")
    )
    assert input.index.name is None
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "___index___"
    assert_pandas_equal(received, expected)


def test_single_index_auto_rename_nameless_multiple_clashes(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_nameless_multiple_clashes"
    input = pd.DataFrame(np.zeros((1, 2)), columns=["__index__", "__index__"], index=[pd.Timestamp(0)])
    assert input.index.name is None
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = pd.DataFrame(np.zeros((1, 2)), columns=["__index__", "____index____"], index=[pd.Timestamp(0)])
    expected.index.name = "___index___"
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("input_index_name", [None, "my_index"])
# Single-element lists allowed for timeseries index
@pytest.mark.parametrize("index_columns", ["ts", ["ts"]])
def test_single_index_explicit_rename_nameless_no_clash(
    in_memory_version_store, object_type, input_index_name, index_columns
):
    lib = in_memory_version_store
    sym = "test_single_index_explicit_rename_nameless_no_clash"
    input = (
        pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
        if object_type == "DataFrame"
        else pd.Series([0], index=[pd.Timestamp(0)], name="col")
    )
    input.index.name = input_index_name
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym, index_columns)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "ts"
    assert_pandas_equal(received, expected)


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
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["10", "level1"])
    expected = (
        pd.DataFrame({"col": [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index, name="")
    )
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_multi_index_auto_rename_int_one_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_int_one_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=[10, "level1"])
    input = (
        pd.DataFrame({"10": [0]}, index=index) if object_type == "DataFrame" else pd.Series([0], index=index, name="10")
    )
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["10", "level1"])
    expected = (
        pd.DataFrame({"_10_": [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index, name="_10_")
    )
    assert_pandas_equal(received, expected)


def test_multi_index_auto_rename_int_multiple_clashes(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_int_multiple_clashes"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=[10, 10])
    input = pd.DataFrame({"10": [0], 10: [1]}, index=index)
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["10", "_10_"])
    expected = pd.DataFrame({"__10__": [0], "___10___": [1]}, index=expected_index)
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
def test_multi_index_auto_rename_nameless_no_clash(in_memory_version_store, object_type):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_nameless_no_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]])
    input = (
        pd.DataFrame({"col": [0]}, index=index)
        if object_type == "DataFrame"
        else pd.Series([0], index=index, name="col")
    )
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["__index_level_0__", "__index_level_1__"])
    expected = (
        pd.DataFrame({"col": [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index, name="col")
    )
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize(
    "input_names,output_names,output_col_name",
    [
        pytest.param([None, None], ["___index_level_0___", "__index_level_1__"], "__index_level_0__"),
        pytest.param(
            [None, "__index_level_0__"], ["___index_level_0___", "__index_level_0__"], "____index_level_0____"
        ),
        pytest.param(["__index_level_1__", None], ["__index_level_1__", "___index_level_1___"], "__index_level_0__"),
    ],
)
def test_multi_index_auto_rename_nameless_clashes(
    in_memory_version_store, object_type, input_names, output_names, output_col_name
):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_nameless_clashes"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=input_names)
    input = (
        pd.DataFrame({"__index_level_0__": [0]}, index=index)
        if object_type == "DataFrame"
        else pd.Series([0], index=index, name="__index_level_0__")
    )
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=output_names)
    expected = (
        pd.DataFrame({output_col_name: [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index, name=output_col_name)
    )
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("input_names", [[None, None], ["level0", "level1"]])
def test_multi_index_explicit_rename_no_clash(in_memory_version_store, object_type, input_names):
    lib = in_memory_version_store
    sym = "test_multi_index_explicit_rename_no_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=input_names)
    input = (
        pd.DataFrame({"col": [0]}, index=index)
        if object_type == "DataFrame"
        else pd.Series([0], index=index, name="col")
    )
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym, ["my_level_0", "my_level_1"])
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["my_level_0", "my_level_1"])
    expected = (
        pd.DataFrame({"col": [0]}, index=expected_index)
        if object_type == "DataFrame"
        else pd.Series([0], index=expected_index, name="col")
    )
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("index_columns", [["col", "level1"], ["level0", "col"]])
def test_multi_index_explicit_rename_clash(in_memory_version_store, object_type, index_columns):
    lib = in_memory_version_store
    sym = "test_multi_index_explicit_rename_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]])
    input = (
        pd.DataFrame({"col": [0]}, index=index)
        if object_type == "DataFrame"
        else pd.Series([0], index=index, name="col")
    )
    lib.write(sym, input)
    with pytest.raises(SchemaException):
        lib.rename_columns_arrow_compat(sym, index_columns)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("index_columns", [["level0"], ["level0", "level1", "level2"]])
def test_multi_index_incorrect_index_name_count(in_memory_version_store, object_type, index_columns):
    lib = in_memory_version_store
    sym = "test_multi_index_incorrect_index_name_count"
    index = pd.MultiIndex.from_arrays([[0], [1]])
    input = (
        pd.DataFrame({"col": [0]}, index=index)
        if object_type == "DataFrame"
        else pd.Series([0], index=index, name="col")
    )
    lib.write(sym, input)
    with pytest.raises(UserInputException):
        lib.rename_columns_arrow_compat(sym, index_columns)


def test_noop_with_arrow_written_data(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    sym = "test_noop_with_arrow_written_data"
    table = pa.table({"col": pa.array([0], pa.int64())})
    lib.write(sym, table)
    # TODO: Uncomment when implemented
    # assert lib.rename_columns_arrow_compat(sym).version == 0
    assert lib.read_metadata(sym).version == 0


def test_exception_with_pickled_data(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_exception_with_pickled_data"
    lib.write(sym, "hello")
    assert lib.is_symbol_pickled(sym)
    with pytest.raises(UserInputException):
        lib.rename_columns_arrow_compat(sym)


def test_exception_with_numpy_array(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_exception_with_numpy_array"
    lib.write(sym, np.arange(1))
    assert not lib.is_symbol_pickled(sym)
    with pytest.raises(UserInputException):
        lib.rename_columns_arrow_compat(sym)


def test_exception_with_non_existent_symbol(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_exception_with_numpy_array"
    with pytest.raises(NoSuchVersionException):
        lib.rename_columns_arrow_compat(sym)
