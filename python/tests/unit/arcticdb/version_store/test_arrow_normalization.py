import copy

from arcticdb.options import OutputFormat
import pandas as pd
import numpy as np
import pyarrow as pa
import pytest

from arcticdb.util.test import assert_frame_equal, assert_frame_equal_with_arrow
from arcticdb.exceptions import ArcticDbNotYetImplemented


def generic_arrow_norm_test(lib, sym, pandas_object, expected_columns, expected_types=None):
    lib.write(sym, pandas_object)
    table = lib.read(sym).data
    assert table.column_names == expected_columns
    if expected_types is not None:
        for i, expected_type in enumerate(expected_types):
            assert table.schema.field(i).type == expected_type
    # If pandas_object is a Series, convert it to a dataframe
    df = pd.DataFrame(pandas_object)
    # df.columns.dtype is int64 if all column names are integers, we always set it to mixed
    df.columns = df.columns.astype("object")
    assert_frame_equal_with_arrow(df, table)
    roundtripped_df = table.to_pandas()
    assert_frame_equal(df, roundtripped_df)


@pytest.mark.parametrize("index", [None, pd.RangeIndex(start=0, step=2, stop=4)])
@pytest.mark.parametrize("index_name", [None, "my index"])
@pytest.mark.parametrize("col_name", ["col", "my index"])
def test_range_index(lmdb_version_store_arrow, index, index_name, col_name):
    df = pd.DataFrame({col_name: [0, 1]}, index=index)
    df.index.name = index_name
    generic_arrow_norm_test(lmdb_version_store_arrow, "test_range_index", df, [col_name])


@pytest.mark.parametrize("col_name", ["col", None, 5, ""])
@pytest.mark.parametrize("duplicate", [True, False])
def test_duplicate_and_special_col_names(lmdb_version_store_arrow, col_name, duplicate):
    columns = [col_name, "y"]
    expected_columns = [f"{col_name}", "y"]
    if duplicate:
        columns.append(col_name)
        expected_columns.append(f"_{col_name}_")
    df = pd.DataFrame(np.zeros((1, len(columns))), columns=columns)
    generic_arrow_norm_test(lmdb_version_store_arrow, "test_duplicate_and_special_col_names", df, expected_columns)


@pytest.mark.parametrize("col_name", [None, 5])
def test_special_col_names_clash_with_string_col_name(lmdb_version_store_arrow, col_name):
    columns = [col_name, str(col_name), col_name]
    df = pd.DataFrame(np.zeros((1, len(columns))), columns=columns)
    generic_arrow_norm_test(
        lmdb_version_store_arrow,
        "test_special_col_names_clash_with_string_col_name",
        df,
        [f"{col_name}", f"_{col_name}_", f"__{col_name}__"],
    )


@pytest.mark.parametrize("columns", [["col"], ["index"], ["index", "index"], ["__index__"], ["__index__", "__index__"]])
def test_unnamed_timeseries_index(lmdb_version_store_arrow, columns):
    df = pd.DataFrame(np.zeros((1, len(columns))), columns=columns, index=[pd.Timestamp(0)])
    index_column_name = "__index__" if "__index__" not in columns else "___index___"
    expected_columns = [index_column_name]
    taken_column_names = set(expected_columns)
    for column in columns:
        while column in taken_column_names:
            column = f"_{column}_"
        taken_column_names.add(column)
        expected_columns.append(column)
    generic_arrow_norm_test(lmdb_version_store_arrow, "test_unnamed_timeseries_index", df, expected_columns)


@pytest.mark.parametrize("index_name", ["index", "__index__", "ts"])
def test_named_timeseries_index_no_clash(lmdb_version_store_arrow, index_name):
    columns = ["col"]
    df = pd.DataFrame(np.zeros((1, len(columns))), columns=columns, index=[pd.Timestamp(0)])
    df.index.name = index_name
    expected_columns = [index_name] + columns
    generic_arrow_norm_test(lmdb_version_store_arrow, "test_named_timeseries_index_no_clash", df, expected_columns)


@pytest.mark.parametrize("index_name", ["index", "__index__", "ts"])
def test_named_timeseries_index_clash(lmdb_version_store_arrow, index_name):
    columns = [index_name, index_name]
    df = pd.DataFrame(np.zeros((1, len(columns))), columns=columns, index=[pd.Timestamp(0)])
    df.index.name = index_name
    expected_columns = [index_name, f"_{columns[0]}_", f"__{columns[1]}__"]
    generic_arrow_norm_test(lmdb_version_store_arrow, "test_named_timeseries_index_clash", df, expected_columns)


@pytest.mark.parametrize("col_name", ["", None, 5])
def test_named_timeseries_index_clash_special_col_names(lmdb_version_store_arrow, col_name):
    index_name = str(col_name)
    columns = [col_name, col_name]
    df = pd.DataFrame(np.zeros((1, len(columns))), columns=columns, index=[pd.Timestamp(0)])
    df.index.name = index_name
    expected_columns = [index_name, f"_{columns[0]}_", f"__{columns[1]}__"]
    generic_arrow_norm_test(
        lmdb_version_store_arrow, "test_named_timeseries_index_clash_special_col_names", df, expected_columns
    )


@pytest.mark.parametrize(
    "columns",
    [
        ["col"],
        ["index"],
        ["__index_level_0__"],
        ["__index_level_0__", "__index_level_0__"],
        ["__index_level_0__", "__index_level_1__"],
    ],
)
def test_unnamed_multiindex(lmdb_version_store_arrow, columns):
    df = pd.DataFrame(
        np.zeros((1, len(columns))), columns=columns, index=pd.MultiIndex.from_product([[pd.Timestamp(0)], ["id"]])
    )
    index_column_names = ["__index_level_0__", "__index_level_1__"]
    if index_column_names[0] in columns:
        index_column_names[0] = f"_{index_column_names[0]}_"
    if index_column_names[1] in columns:
        index_column_names[1] = f"_{index_column_names[1]}_"
    if columns == ["__index_level_0__", "__index_level_0__"]:
        columns[-1] = f"__{columns[-1]}__"
    expected_columns = index_column_names + columns
    generic_arrow_norm_test(lmdb_version_store_arrow, "test_unnamed_multiindex", df, expected_columns)


@pytest.mark.parametrize(
    "index_column_names",
    [
        ["my name", None],
        [None, "my name"],
    ],
)
@pytest.mark.parametrize(
    "columns",
    [
        ["col"],
        ["index"],
        ["__index_level_0__"],
        ["__index_level_0__", "__index_level_0__"],
        ["__index_level_0__", "__index_level_1__"],
    ],
)
def test_partially_named_multiindex(lmdb_version_store_arrow, index_column_names, columns):
    df = pd.DataFrame(
        np.zeros((1, len(columns))),
        columns=columns,
        index=pd.MultiIndex.from_product([[pd.Timestamp(0)], ["id"]], names=index_column_names),
    )
    expected_columns = copy.deepcopy(index_column_names)
    for i in range(len(index_column_names)):
        expected_columns[i] = f"__index_level_{i}__" if index_column_names[i] is None else index_column_names[i]
    if expected_columns[0] in columns:
        expected_columns[0] = f"_{expected_columns[0]}_"
    if expected_columns[1] in columns:
        expected_columns[1] = f"_{expected_columns[1]}_"
    expected_columns += copy.deepcopy(columns)
    if columns == ["__index_level_0__", "__index_level_0__"]:
        expected_columns[-1] = f"_{expected_columns[-1]}_"
    if expected_columns[0] == "___index_level_0___" and columns == ["__index_level_0__", "__index_level_0__"]:
        expected_columns[-1] = f"_{expected_columns[-1]}_"
    generic_arrow_norm_test(lmdb_version_store_arrow, "test_partially_named_multiindex", df, expected_columns)


@pytest.mark.parametrize("index_names", [["level 1", "level 2"], ["index", "__index__"], ["__index__", "index"]])
def test_named_multiindex_no_clash(lmdb_version_store_arrow, index_names):
    columns = ["col"]
    df = pd.DataFrame(
        np.zeros((1, len(columns))),
        columns=columns,
        index=pd.MultiIndex.from_product([[pd.Timestamp(0)], ["id"]], names=index_names),
    )
    generic_arrow_norm_test(lmdb_version_store_arrow, "test_named_multiindex_no_clash", df, index_names + columns)


def test_named_multiindex_duplicates_in_level_names(lmdb_version_store_arrow):
    index_names = ["level", "level"]
    columns = ["col"]
    df = pd.DataFrame(
        np.zeros((1, len(columns))),
        columns=columns,
        index=pd.MultiIndex.from_product([[pd.Timestamp(0)], ["id"]], names=index_names),
    )
    generic_arrow_norm_test(
        lmdb_version_store_arrow, "test_named_multiindex_duplicates_in_level_names", df, ["level", "_level_", "col"]
    )


@pytest.mark.parametrize("columns", [["level 1"], ["level 2"], ["level 1", "level 2"], ["level 1", "level 1"]])
def test_named_multiindex_duplicates_in_columns(lmdb_version_store_arrow, columns):
    index_names = ["level 1", "level 2"]
    df = pd.DataFrame(
        np.zeros((1, len(columns))),
        columns=columns,
        index=pd.MultiIndex.from_product([[pd.Timestamp(0)], ["id"]], names=index_names),
    )
    expected_columns = index_names
    taken_column_names = set(expected_columns)
    for col in columns:
        while col in taken_column_names:
            col = f"_{col}_"
        expected_columns.append(col)
        taken_column_names.add(col)
    generic_arrow_norm_test(
        lmdb_version_store_arrow, "test_named_multiindex_duplicates_in_columns", df, expected_columns
    )


@pytest.mark.parametrize("clash_level", [0, 1])
@pytest.mark.parametrize("col_name", ["", None, 5])
def test_named_multiindex_clash_special_col_names(lmdb_version_store_arrow, clash_level, col_name):
    index_names = ["level 1", "level 2"]
    index_names[clash_level] = str(col_name)
    df = pd.DataFrame(
        np.zeros((1, 1)),
        columns=[col_name],
        index=pd.MultiIndex.from_product([[pd.Timestamp(0)], ["id"]], names=index_names),
    )
    expected_columns = index_names + [f"_{col_name}_"]
    generic_arrow_norm_test(
        lmdb_version_store_arrow, "test_named_multiindex_clash_special_col_names", df, expected_columns
    )


def test_index_with_timezone(lmdb_version_store_arrow):
    df = pd.DataFrame(
        {"col": np.arange(10, dtype=np.int64)},
        index=pd.date_range(pd.Timestamp(year=2025, month=1, day=1, tz="America/New_York"), periods=10),
    )
    generic_arrow_norm_test(
        lmdb_version_store_arrow,
        "test_index_with_timezone",
        df,
        ["__index__", "col"],
        [pa.timestamp("ns", "America/New_York"), pa.int64()],
    )


def test_multi_index_with_tz(lmdb_version_store_arrow):
    df = pd.DataFrame(
        {"col": np.arange(10, dtype=np.int64())},
        index=[
            [chr(ord("a") + i // 5) for i in range(10)],
            [pd.Timestamp(year=2025, month=1, day=1 + i % 5, tz="America/Los_Angeles") for i in range(10)],
        ],
    )
    df.index.names = ["index1", "index2"]
    generic_arrow_norm_test(
        lmdb_version_store_arrow,
        "test_multi_index_with_tz",
        df,
        ["index1", "index2", "col"],
        [pa.large_string(), pa.timestamp("ns", "America/Los_Angeles"), pa.int64()],
    )


def test_multi_index_no_name_multiple_tz(lmdb_version_store_arrow):
    df = pd.DataFrame(
        {"col": np.arange(10, dtype=np.int64)},
        index=[
            [pd.Timestamp(year=2025, month=1, day=1 + i // 5, tz="Asia/Hong_Kong") for i in range(10)],
            [pd.Timestamp(year=2025, month=1, day=1 + i % 5, tz="America/Los_Angeles") for i in range(10)],
        ],
    )
    generic_arrow_norm_test(
        lmdb_version_store_arrow,
        "test_multi_index_no_name_multiple_tz",
        df,
        ["__index_level_0__", "__index_level_1__", "col"],
        [pa.timestamp("ns", "Asia/Hong_Kong"), pa.timestamp("ns", "America/Los_Angeles"), pa.int64()],
    )


def test_series_basic(lmdb_version_store_arrow):
    series = pd.Series(
        np.arange(10, dtype=np.int64), name="my series", index=pd.RangeIndex(start=3, step=5, stop=3 + 10 * 5)
    )
    generic_arrow_norm_test(lmdb_version_store_arrow, "test_series_basic", series, ["my series"])


def test_series_with_index(lmdb_version_store_arrow):
    series = pd.Series(
        np.arange(10, dtype=np.int64),
        name="my series",
        index=pd.date_range(pd.Timestamp(year=2025, month=1, day=1, tz="Europe/London"), periods=10),
    )
    generic_arrow_norm_test(
        lmdb_version_store_arrow,
        "test_series_with_index",
        series,
        ["__index__", "my series"],
        [pa.timestamp("ns", "Europe/London"), pa.int64()],
    )


def test_read_pickled(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_read_pickled"
    obj = {"a": ["b", "c"], "x": 122.3}
    lib.write(sym, obj)
    result = lib.read(sym).data
    assert obj == result


def test_custom_normalizer(custom_thing_with_registered_normalizer, lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_custom_normalizer"
    obj = custom_thing_with_registered_normalizer
    lib.write(sym, obj)
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.read(sym).data
