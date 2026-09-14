"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb import QueryBuilder
from arcticdb.exceptions import InternalException, NoSuchVersionException, SchemaException, UserInputException
from arcticdb.options import OutputFormat
from arcticdb.util.test import assert_frame_equal, assert_pandas_equal
from arcticdb_ext.storage import KeyType


def make_df_or_series(object_type, name, values, index=None):
    if object_type == "DataFrame":
        return pd.DataFrame({name: values}, index=index)
    return pd.Series(values, index=index, name=name)


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
    for col_name, col_meta in common.col_names.items():
        assert not col_meta.is_int
        assert not col_meta.is_none
        assert not col_meta.is_empty
        assert col_name == col_meta.original_name
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


def assert_data_key_col_names_expected(lib, sym, version, df):
    lib_tool = lib.library_tool()
    data_keys = [
        data_key for data_key in lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym) if data_key.version_id == version
    ]
    column_set = set(df.columns.to_list()) if isinstance(df, pd.DataFrame) else set([df.name])
    if isinstance(df.index, pd.DatetimeIndex):
        column_set.add(df.index.name)
    elif isinstance(df.index, pd.MultiIndex):
        column_set.add(df.index.names[0])
        for idx in range(1, df.index.nlevels):
            column_set.add(f"__idx__{df.index.names[idx]}")
    for data_key in data_keys:
        desc = lib_tool.read_descriptor(data_key)
        for field in desc.fields():
            assert field.name in column_set


def generic_rename_columns_arrow_compat_test(lib, sym, index_columns=None):
    before = lib.read(sym, output_format=OutputFormat.PYARROW)
    before_data, before_metadata, before_version = before.data, before.metadata, before.version
    vit = lib.rename_columns_arrow_compat(sym, index_columns)
    assert_norm_meta_arrow_compatible(lib, sym)
    after_data = lib.read(sym, output_format=OutputFormat.PYARROW).data
    assert vit.version == before_version + 1
    assert before_metadata == lib.read_metadata(sym).metadata
    if isinstance(index_columns, str):
        before_data = before_data.set_column(0, index_columns, before_data.column(0))
    elif isinstance(index_columns, list):
        for idx, index_name in enumerate(index_columns):
            before_data = before_data.set_column(idx, index_name, before_data.column(idx))
    assert before_data.equals(after_data)
    # Idempotent
    after_pandas = lib.read(sym, output_format=OutputFormat.PANDAS).data
    vit_idempotent = lib.rename_columns_arrow_compat(sym, index_columns)
    assert vit_idempotent.version == vit.version
    assert_data_key_col_names_expected(lib, sym, vit.version, after_pandas)


@pytest.mark.parametrize("index_columns", [5, [], [5, "hello"]])
def test_bad_arguments(in_memory_version_store, index_columns):
    lib = in_memory_version_store
    sym = "test_bad_arguments"
    with pytest.raises(UserInputException):
        lib.rename_columns_arrow_compat(sym, index_columns)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("col_name", [None, "", 10])
def test_arrow_col_rename_basic(in_memory_version_store, object_type, col_name):
    lib = in_memory_version_store
    sym = "test_arrow_col_rename_basic"
    input = make_df_or_series(object_type, col_name, [0])
    lib.write(sym, input, metadata="hello")
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    col_name = str(col_name) if col_name != "" else "__empty__"
    expected = make_df_or_series(object_type, col_name, [0])
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


@pytest.mark.parametrize(
    "input",
    [
        pd.DataFrame({"col": [0]}, index=[pd.date_range("2026-01-01", periods=1, name="ts")]),
        pa.table({"col": pa.array([0], pa.int64())}),
    ],
)
def test_arrow_col_rename_valid_schema_is_noop(in_memory_version_store_arrow, input):
    lib = in_memory_version_store_arrow
    sym = "test_arrow_col_rename_valid_schema_is_noop"
    lib.write(sym, input)
    assert lib.rename_columns_arrow_compat(sym).version == 0
    assert lib.read_metadata(sym).version == 0


def test_arrow_col_rename_norm_meta_only_change_doesnt_rewrite_data_keys(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_arrow_col_rename_norm_meta_only_change_doesnt_rewrite_data_keys"
    # This dataframe has a StreamDescriptor with the field "10" that is already correct. Only the norm metadata needs
    # to change
    df = pd.DataFrame({10: [0]})
    lib.write(sym, df)
    assert lib.rename_columns_arrow_compat(sym).version == 1
    assert lib.read_index(sym)["version_id"].iloc[0] == 0


@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("cols_per_segment", [2, 127])
def test_arrow_col_rename_duplicates(in_memory_store_factory, rows_per_segment, cols_per_segment):
    lib = in_memory_store_factory(segment_row_size=rows_per_segment, column_group_size=cols_per_segment)
    sym = "test_arrow_col_rename_duplicates"
    input = pd.DataFrame(
        np.zeros((10, 12)), columns=["col", "col", "col", "", "", "", None, None, "None", 10, "10", 10]
    )
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    # Note that None and int column names are stringified prior to deduplication from left-to-right, hence the order
    # "None", "_None_", "__None__", "10", "_10_", "__10__" in the output, even though some columns were strings already
    expected = pd.DataFrame(
        np.zeros((10, 12)),
        columns=[
            "col",
            "_col_",
            "__col__",
            "__empty__",
            "___empty___",
            "____empty____",
            "None",
            "_None_",
            "__None__",
            "10",
            "_10_",
            "__10__",
        ],
    )
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("cols_per_segment", [2, 127])
def test_arrow_col_rename_synthetic_columns(in_memory_store_factory, cols_per_segment):
    lib = in_memory_store_factory(column_group_size=cols_per_segment)
    sym = "test_arrow_col_rename_synthetic_columns"
    input = pd.DataFrame(np.zeros((1, 10)))
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = pd.DataFrame(np.zeros((1, 10)), columns=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"])
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("cols_per_segment", [2, 127])
def test_single_index_auto_rename_int_no_clash(
    in_memory_store_factory, object_type, rows_per_segment, cols_per_segment
):
    lib = in_memory_store_factory(segment_row_size=rows_per_segment, column_group_size=cols_per_segment)
    sym = "test_single_index_auto_rename_int_no_clash"
    input = (
        pd.DataFrame(
            {"col0": np.arange(10), "col1": np.arange(10, 20), "col2": np.arange(20, 30)},
            index=pd.date_range("2026-01-01", periods=10),
        )
        if object_type == "DataFrame"
        else pd.Series(np.arange(10), index=pd.date_range("2026-01-01", periods=10), name="col")
    )
    input.index.name = 10
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "10"
    assert_pandas_equal(received, expected)


# Empty DatetimeIndex frames are not marked as having a physically stored index, but still have an index column
@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("index_name", [None, "ts"])
def test_single_index_auto_rename_empty_frame(in_memory_version_store, object_type, index_name):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename_empty_frame"
    index = pd.DatetimeIndex([], name=index_name)
    values = np.array([], dtype=np.int64)
    input = make_df_or_series(object_type, "col", values, index=index)
    lib.write(sym, input)
    if index_name == "ts":
        assert lib.rename_columns_arrow_compat(sym).version == 0
    else:
        generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "__index__" if index_name is None else index_name
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize(
    "col_name, index_name, expected_col_name, expected_index_name",
    [
        # The index's real name takes the unwrapped slot unconditionally, since it is processed before the data
        # columns; the data column is processed afterwards and wrapped to avoid the resulting clash
        pytest.param("10", 10, "_10_", "10", id="int_one_clash"),
        pytest.param("col", None, "col", "__index__", id="nameless_no_clash"),
        # Here the index has no real name, so its auto-generated candidate name clashes with the data column
        # instead, and it is the index name that ends up wrapped
        pytest.param("__index__", None, "__index__", "___index___", id="nameless_one_clash"),
        pytest.param(None, None, "None", "__index__", id="index_and_col_name_both_none"),
        pytest.param("", "", "___empty___", "__empty__", id="index_and_col_name_both_empty"),
        pytest.param("__index__", "__index__", "___index___", "__index__", id="index_and_col_name_both_dunder_index"),
        pytest.param("hello", "hello", "_hello_", "hello", id="index_and_col_name_both_hello"),
    ],
)
def test_single_index_auto_rename(
    in_memory_version_store, object_type, col_name, index_name, expected_col_name, expected_index_name
):
    lib = in_memory_version_store
    sym = "test_single_index_auto_rename"
    input = make_df_or_series(object_type, col_name, [0], index=[pd.Timestamp(0)])
    input.index.name = index_name
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = make_df_or_series(object_type, expected_col_name, [0], index=[pd.Timestamp(0)])
    expected.index.name = expected_index_name
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
@pytest.mark.parametrize("input_index_name", [None, "my_index", "ts"])
# Single-element lists allowed for timeseries index
@pytest.mark.parametrize("index_columns", ["ts", ["ts"]])
def test_single_index_explicit_rename_no_clash(in_memory_version_store, object_type, input_index_name, index_columns):
    lib = in_memory_version_store
    sym = "test_single_index_explicit_rename_no_clash"
    input = make_df_or_series(object_type, "col", [0], index=[pd.Timestamp(0)])
    input.index.name = input_index_name
    lib.write(sym, input)
    if input_index_name == "ts":
        assert lib.rename_columns_arrow_compat(sym, index_columns).version == 0
    else:
        generic_rename_columns_arrow_compat_test(lib, sym, index_columns)
    received = lib.read(sym).data
    expected = input
    expected.index.name = "ts"
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize(
    "index, col_name, index_columns",
    [
        pytest.param([pd.Timestamp(0)], "ts", "ts", id="single_index_explicit_rename_clash"),
        pytest.param([pd.Timestamp(0)], "col", ["level0", "level1"], id="single_index_too_many_index_names"),
        pytest.param(
            pd.MultiIndex.from_arrays([[0], [1]]),
            "col",
            ["col", "level1"],
            id="multi_index_rename_clash_level0",
        ),
        pytest.param(
            pd.MultiIndex.from_arrays([[0], [1]]),
            "col",
            ["level0", "col"],
            id="multi_index_rename_clash_level1",
        ),
        pytest.param(pd.MultiIndex.from_arrays([[0], [1]]), "col", ["level0"], id="multi_index_too_few_index_names"),
        pytest.param(
            pd.MultiIndex.from_arrays([[0], [1]]),
            "col",
            ["level0", "level1", "level2"],
            id="multi_index_too_many_index_names",
        ),
    ],
)
def test_explicit_rename_raises(in_memory_version_store, object_type, index, col_name, index_columns):
    lib = in_memory_version_store
    sym = "test_explicit_rename_raises"
    input = make_df_or_series(object_type, col_name, [0], index=index)
    lib.write(sym, input)
    with pytest.raises(UserInputException):
        lib.rename_columns_arrow_compat(sym, index_columns)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("cols_per_segment", [2, 127])
def test_multi_index_auto_rename_nameless_no_clash(
    in_memory_store_factory, object_type, rows_per_segment, cols_per_segment
):
    lib = in_memory_store_factory(segment_row_size=rows_per_segment, column_group_size=cols_per_segment)
    sym = "test_multi_index_auto_rename_nameless_no_clash"
    index = pd.MultiIndex.from_arrays([pd.date_range("2026-01-01", periods=10), np.arange(10)])
    input = (
        pd.DataFrame({"col0": np.arange(10, 20), "col1": np.arange(20, 30), "col2": np.arange(30, 40)}, index=index)
        if object_type == "DataFrame"
        else pd.Series(np.arange(10, 20), index=index, name="col")
    )
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays(
        [pd.date_range("2026-01-01", periods=10), np.arange(10)], names=["__index_level_0__", "__index_level_1__"]
    )
    expected = (
        pd.DataFrame(
            {"col0": np.arange(10, 20), "col1": np.arange(20, 30), "col2": np.arange(30, 40)}, index=expected_index
        )
        if object_type == "DataFrame"
        else pd.Series(np.arange(10, 20), index=expected_index, name="col")
    )
    assert_pandas_equal(received, expected)


# See test_compatability.py::test_arrow_col_rename_multi_index_auto_rename_clashes for int top-level index, as this has
# always been False since July 2026
@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize(
    "col_name,input_names,output_names,output_col_name",
    [
        pytest.param(
            "__index_level_0__",
            [None, None],
            ["___index_level_0___", "__index_level_1__"],
            "__index_level_0__",
            id="nameless_no_clash",
        ),
        pytest.param(
            "__index_level_0__",
            [None, "__index_level_0__"],
            ["___index_level_0___", "__index_level_0__"],
            "____index_level_0____",
            id="nameless_one_clash",
        ),
        pytest.param(
            "__index_level_0__",
            ["__index_level_1__", None],
            ["__index_level_1__", "___index_level_1___"],
            "__index_level_0__",
            id="nameless_one_clash_second_level",
        ),
    ],
)
def test_multi_index_auto_rename_clashes(
    in_memory_version_store, object_type, col_name, input_names, output_names, output_col_name
):
    lib = in_memory_version_store
    sym = "test_multi_index_auto_rename_clashes"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=input_names)
    input = make_df_or_series(object_type, col_name, [0], index=index)
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=output_names)
    expected = make_df_or_series(object_type, output_col_name, [0], index=expected_index)
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("object_type", ["DataFrame", "Series"])
@pytest.mark.parametrize("input_names", [[None, None], ["level0", "level1"]])
def test_multi_index_explicit_rename_no_clash(in_memory_version_store, object_type, input_names):
    lib = in_memory_version_store
    sym = "test_multi_index_explicit_rename_no_clash"
    index = pd.MultiIndex.from_arrays([[0], [1]], names=input_names)
    input = make_df_or_series(object_type, "col", [0], index=index)
    lib.write(sym, input)
    generic_rename_columns_arrow_compat_test(lib, sym, ["my_level_0", "my_level_1"])
    received = lib.read(sym).data
    expected_index = pd.MultiIndex.from_arrays([[0], [1]], names=["my_level_0", "my_level_1"])
    expected = make_df_or_series(object_type, "col", [0], index=expected_index)
    assert_pandas_equal(received, expected)


# Dynamic schema uses different name mangling to static schema as duplicate column names are not supported, and column
# index used in static schema is not stable on append/update
@pytest.mark.parametrize("col_name", [None, "", 10])
def test_arrow_col_rename_dynamic_schema_basic(in_memory_version_store_dynamic_schema, col_name):
    lib = in_memory_version_store_dynamic_schema
    sym = "test_arrow_col_rename_dynamic_schema_basic"
    input = pd.DataFrame({"col0": [0], "col1": [1], col_name: [2]})
    lib.write(sym, input, metadata="hello")
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    col_name = str(col_name) if col_name != "" else "__empty__"
    expected = pd.DataFrame({"col0": [0], "col1": [1], col_name: [2]})
    assert_pandas_equal(received, expected)


def test_arrow_col_rename_dynamic_schema_multiple_row_slices(in_memory_store_factory):
    lib = in_memory_store_factory(dynamic_schema=True, dynamic_strings=True)
    sym = "test_arrow_col_rename_dynamic_schema_multiple_row_slices"
    df_0 = pd.DataFrame({"col0": np.arange(1, dtype=np.uint8), None: ["hello"], "": np.arange(1, dtype=np.float64)})
    df_1 = pd.DataFrame({"": np.arange(1, 2, dtype=np.float32), 10: [True], "col0": np.arange(1, 2, dtype=np.uint16)})
    lib.write(sym, df_0)
    lib.append(sym, df_1)
    generic_rename_columns_arrow_compat_test(lib, sym)
    received = lib.read(sym).data
    expected = pd.DataFrame(
        {
            "col0": np.arange(2, dtype=np.uint16),
            "None": ["hello", None],
            "__empty__": np.arange(2, dtype=np.float64),
            "10": [False, True],
        }
    )
    assert_pandas_equal(received, expected)


@pytest.mark.parametrize("input", [{"a": pd.DataFrame({"col": [0]})}, "hello", np.arange(1)])
def test_exception_with_unsupported_data(in_memory_version_store, input):
    lib = in_memory_version_store
    sym = "test_exception_with_unsupported_data"
    lib.write(sym, input, recursive_normalizers=isinstance(input, dict))
    is_symbol_pickled = lib.is_symbol_pickled(sym)
    assert is_symbol_pickled == isinstance(input, str)
    with pytest.raises(SchemaException):
        lib.rename_columns_arrow_compat(sym)


def test_exception_with_non_existent_symbol(in_memory_version_store):
    lib = in_memory_version_store
    sym = "test_exception_with_non_existent_symbol"
    with pytest.raises(NoSuchVersionException):
        lib.rename_columns_arrow_compat(sym)


# It is impossible to handle this type of data as it contains duplicated field names in the StreamDescriptor, and so
# cannot even be correctly decoded (the SegmentInMemory column_map contains fewer entries than there are columns)
# See Monday issues 9715738171 and 12909663080
@pytest.mark.parametrize("index", [[pd.Timestamp(0)], pd.MultiIndex.from_arrays([[0], [1]], names=[10, 10])])
def test_auto_rename_int_multiple_clashes(in_memory_version_store, index):
    lib = in_memory_version_store
    sym = "test_auto_rename_int_multiple_clashes"
    input = pd.DataFrame({"10": [0], 10: [1]}, index=index)
    input.index.name = 10
    lib.write(sym, input)
    with pytest.raises(InternalException):
        lib.rename_columns_arrow_compat(sym)
