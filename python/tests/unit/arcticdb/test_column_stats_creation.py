"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from arcticc.pb2.column_stats_pb2 import ColumnStatsHeader, ColumnStatsType
from google.protobuf.any_pb2 import Any as ProtobufAny

from arcticdb_ext.exceptions import SchemaException, StorageException, UserInputException
from arcticdb_ext.storage import KeyType
from arcticdb_ext.version_store import NoSuchVersionException
from arcticdb import QueryBuilder
from arcticdb.util.test import config_context

pytestmark = pytest.mark.pipeline

ADMISSION_KEY = "VersionStore.NumProcessingUnitsLive"


def write_many_slices(lib, sym, n_appends):
    """Writes a symbol of n_appends row slices, each two rows of col_0/col_1/col_2."""
    base = pd.Timestamp("2000-01-01")
    for i in range(n_appends):
        df = pd.DataFrame(
            {"col_0": [f"a{i}", f"b{i}"], "col_1": [2 * i, 2 * i + 1], "col_2": [i, i + 1]},
            index=pd.date_range(base + pd.Timedelta(days=2 * i), periods=2),
        )
        lib.write(sym, df) if i == 0 else lib.append(sym, df)


df0 = pd.DataFrame(
    {"col_0": ["a", "b"], "col_1": [1, 2], "col_2": [6, 5]}, index=pd.date_range("2000-01-01", periods=2)
)
df1 = pd.DataFrame(
    {"col_0": ["c", "d"], "col_1": [3, 4], "col_2": [8, 7]}, index=pd.date_range("2000-01-03", periods=2)
)
df2 = pd.DataFrame(
    {"col_0": ["e", "f"], "col_1": [5, 6], "col_2": [10, 9]}, index=pd.date_range("2000-01-05", periods=2)
)


def index_columns_to_pl(lib, sym):
    pdf = lib.read_index(sym).reset_index()
    return pl.from_pandas(pdf[["start_index", "end_index"]]).unique(maintain_order=True)


def generate_symbol(lib, sym):
    lib.write(sym, df0)
    lib.append(sym, df1)
    return index_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [df0["col_1"].min(), df1["col_1"].min()]),
        pl.Series("v1_MAX(col_1)", [df0["col_1"].max(), df1["col_1"].max()]),
        pl.Series("v1_MIN(col_2)", [df0["col_2"].min(), df1["col_2"].min()]),
        pl.Series("v1_MAX(col_2)", [df0["col_2"].max(), df1["col_2"].max()]),
    )


def assert_stats_equal(received, expected):
    assert isinstance(received, pa.Table)
    assert isinstance(expected, pl.DataFrame)
    received_pl = pl.from_arrow(received)
    pl_assert_frame_equal(received_pl, expected, check_column_order=False, check_dtypes=False)


def test_column_stats_basic_flow(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_basic_flow"
    expected_column_stats = generate_symbol(lib, sym).select(
        ["start_index", "end_index", "v1_MIN(col_1)", "v1_MAX(col_1)"]
    )

    column_stats_dict = {"col_1": {"MINMAX"}}

    # These should just log warnings if no column stats exist
    lib.drop_column_stats(sym)
    lib.drop_column_stats(sym, column_stats_dict)

    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    lib.drop_column_stats(sym)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym)


def test_column_stats_infinity(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_infinity"
    df0 = pd.DataFrame({"col_1": [np.inf, 0.5]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [1.5, -np.inf]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [np.inf, -np.inf]}, index=pd.date_range("2000-01-05", periods=2))
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    expected_column_stats = index_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [df0["col_1"].min(), df1["col_1"].min(), df2["col_1"].min()]),
        pl.Series("v1_MAX(col_1)", [df0["col_1"].max(), df1["col_1"].max(), df2["col_1"].max()]),
    )
    column_stats_dict = {"col_1": {"MINMAX"}}

    lib.create_column_stats(sym, column_stats_dict)

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_nan_values(lmdb_version_store, any_output_format):
    lib = lmdb_version_store
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_nan_values"
    df0 = pd.DataFrame({"col_1": [1.0, 3.0]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [np.nan, 5.0]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5.0, np.nan]}, index=pd.date_range("2000-01-05", periods=2))
    df3 = pd.DataFrame({"col_1": [np.nan, np.nan]}, index=pd.date_range("2000-01-07", periods=2))
    df4 = pd.DataFrame({"col_1": [1.0, np.nan, 2.0]}, index=pd.date_range("2000-01-09", periods=3))
    df5 = pd.DataFrame({"col_1": [np.nan, 1.0, 2.0, np.nan]}, index=pd.date_range("2000-01-12", periods=4))
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    lib.append(sym, df4)
    lib.append(sym, df5)

    # We store nan stats iff the whole block is nan
    expected_column_stats = index_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [1.0, 5.0, 5.0, np.nan, 1.0, 1.0]),
        pl.Series("v1_MAX(col_1)", [3.0, 5.0, 5.0, np.nan, 2.0, 2.0]),
    )
    column_stats_dict = {"col_1": {"MINMAX"}}

    lib.create_column_stats(sym, column_stats_dict)

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_only_nan_values(lmdb_version_store, any_output_format):
    lib = lmdb_version_store
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_nan_values"
    df = pd.DataFrame({"col_1": [np.nan, np.nan]}, index=pd.date_range("2000-01-07", periods=2))
    lib.write(sym, df)

    expected_column_stats = index_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [np.nan]),
        pl.Series("v1_MAX(col_1)", [np.nan]),
    )
    column_stats_dict = {"col_1": {"MINMAX"}}

    lib.create_column_stats(sym, column_stats_dict)

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_nat_values(lmdb_version_store, any_output_format):
    lib = lmdb_version_store
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_nat_values"
    df0 = pd.DataFrame(
        {"col_1": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-06-01")]},
        index=pd.date_range("2000-01-01", periods=2),
    )
    df1 = pd.DataFrame(
        {"col_1": [pd.NaT, pd.Timestamp("2025-01-01")]},
        index=pd.date_range("2000-01-03", periods=2),
    )
    df2 = pd.DataFrame(
        {"col_1": [pd.Timestamp("2025-01-01"), pd.NaT]},
        index=pd.date_range("2000-01-05", periods=2),
    )
    df3 = pd.DataFrame(
        {"col_1": [pd.NaT, pd.NaT]},
        index=pd.date_range("2000-01-07", periods=2),
    )
    df4 = pd.DataFrame(
        {"col_1": [pd.Timestamp("2024-01-01"), pd.NaT, pd.Timestamp("2024-06-01")]},
        index=pd.date_range("2000-01-09", periods=3),
    )
    df5 = pd.DataFrame(
        {"col_1": [pd.NaT, pd.Timestamp("2023-01-01"), pd.Timestamp("2023-06-01"), pd.NaT]},
        index=pd.date_range("2000-01-12", periods=4),
    )
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    lib.append(sym, df4)
    lib.append(sym, df5)

    # We store NaT stats iff the whole block is NaT. The Arrow output path converts NaT to null,
    # so the public read_column_stats API surfaces None for the all-NaT block.
    expected_column_stats = index_columns_to_pl(lib, sym).with_columns(
        pl.Series(
            "v1_MIN(col_1)",
            [
                pd.Timestamp("2020-01-01").value,
                pd.Timestamp("2025-01-01").value,
                pd.Timestamp("2025-01-01").value,
                None,
                pd.Timestamp("2024-01-01").value,
                pd.Timestamp("2023-01-01").value,
            ],
            dtype=pl.Int64,
        ).cast(pl.Datetime("ns")),
        pl.Series(
            "v1_MAX(col_1)",
            [
                pd.Timestamp("2020-06-01").value,
                pd.Timestamp("2025-01-01").value,
                pd.Timestamp("2025-01-01").value,
                None,
                pd.Timestamp("2024-06-01").value,
                pd.Timestamp("2023-06-01").value,
            ],
            dtype=pl.Int64,
        ).cast(pl.Datetime("ns")),
    )
    column_stats_dict = {"col_1": {"MINMAX"}}

    lib.create_column_stats(sym, column_stats_dict)

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    nat_sentinel = np.iinfo(np.int64).min
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert len(keys) == 1
    raw_stats = lib_tool.read_to_dataframe(keys[0])
    assert raw_stats["v1_MIN(col_1)"].values.view("int64")[3] == nat_sentinel
    assert raw_stats["v1_MAX(col_1)"].values.view("int64")[3] == nat_sentinel


def test_column_stats_only_nat_values(lmdb_version_store, any_output_format):
    lib = lmdb_version_store
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_only_nat_values"
    df = pd.DataFrame({"col_1": [pd.NaT, pd.NaT]}, index=pd.date_range("2000-01-07", periods=2))
    lib.write(sym, df)

    expected_column_stats = index_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [None], dtype=pl.Int64).cast(pl.Datetime("ns")),
        pl.Series("v1_MAX(col_1)", [None], dtype=pl.Int64).cast(pl.Datetime("ns")),
    )
    column_stats_dict = {"col_1": {"MINMAX"}}

    lib.create_column_stats(sym, column_stats_dict)

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    nat_sentinel = np.iinfo(np.int64).min
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert len(keys) == 1
    raw_stats = lib_tool.read_to_dataframe(keys[0])
    assert raw_stats["v1_MIN(col_1)"].values.view("int64")[0] == nat_sentinel
    assert raw_stats["v1_MAX(col_1)"].values.view("int64")[0] == nat_sentinel


def test_column_stats_as_of(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_as_of"
    expected_column_stats = generate_symbol(lib, sym)[[0]].select(
        ["start_index", "end_index", "v1_MIN(col_1)", "v1_MAX(col_1)"]
    )
    column_stats_dict = {"col_1": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict, as_of=0)
    assert lib.get_column_stats_info(sym, as_of=0) == column_stats_dict
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)

    column_stats = lib.read_column_stats(sym, as_of=0)
    assert_stats_equal(column_stats, expected_column_stats)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym)

    lib.drop_column_stats(sym, as_of=0)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym, as_of=0)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym, as_of=0)


def test_column_stats_as_of_version_doesnt_exist(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_as_of_version_doesnt_exist"
    generate_symbol(lib, sym)

    lib.delete_version(sym, 0)

    column_stats_dict = {"col_1": {"MINMAX"}}
    with pytest.raises(NoSuchVersionException):
        lib.create_column_stats(sym, column_stats_dict, as_of=0)
    with pytest.raises(NoSuchVersionException):
        lib.get_column_stats_info(sym, as_of=0)
    with pytest.raises(NoSuchVersionException):
        lib.read_column_stats(sym, as_of=0)


# TODO: When more than one column stat type is implemented, change this to add multiple indexes across multiple columns
def test_column_stats_multiple_indexes_different_columns(
    version_store_factory, lib_name, encoding_version, any_output_format
):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_multiple_indexes"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    lib.drop_column_stats(sym, {"col_2": {"MINMAX"}})
    assert lib.get_column_stats_info(sym) == {"col_1": {"MINMAX"}}

    expected_column_stats = expected_column_stats.select(["start_index", "end_index", "v1_MIN(col_1)", "v1_MAX(col_1)"])
    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_empty_dict(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_empty_dict"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_empty_dict = dict()
    lib.create_column_stats(sym, column_stats_empty_dict)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    lib.drop_column_stats(sym, column_stats_empty_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict
    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)


def test_column_stats_empty_set(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_empty_set"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_empty_set = {"col_1": set()}
    lib.create_column_stats(sym, column_stats_empty_set)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    lib.drop_column_stats(sym, column_stats_empty_set)
    assert lib.get_column_stats_info(sym) == column_stats_dict
    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)


def test_column_stats_non_existent_column(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_non_existent_column"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_non_existent_column = {"non_existent_column": {"MINMAX"}}
    with pytest.raises(UserInputException) as exception_info:
        lib.create_column_stats(sym, column_stats_non_existent_column)
    assert "non_existent_column" in str(exception_info.value)

    column_stats_non_existent_column_and_existent = {"non_existent_column": {"MINMAX"}, "col_1": {"MINMAX"}}
    with pytest.raises(UserInputException) as exception_info:
        lib.create_column_stats(sym, column_stats_non_existent_column_and_existent)
    assert "non_existent_column" in str(exception_info.value)
    assert "E_INVALID_USER_ARGUMENT" in str(exception_info.value)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    lib.drop_column_stats(sym, column_stats_non_existent_column)
    assert lib.get_column_stats_info(sym) == column_stats_dict
    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)


def test_column_stats_non_existent_stat_type(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_non_existent_stat_type"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_non_existent_index_type = {"col_1": {"NON_EXISTENT_INDEX_TYPE"}}
    with pytest.raises(UserInputException):
        lib.create_column_stats(sym, column_stats_non_existent_index_type)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    with pytest.raises(UserInputException):
        lib.drop_column_stats(sym, column_stats_non_existent_index_type)
    assert lib.get_column_stats_info(sym) == column_stats_dict
    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)


def test_column_stats_pickled_symbol(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_pickled_symbol"
    lib.write(sym, 1)
    assert lib.is_symbol_pickled(sym)

    column_stats_dict = {"col_1": {"MINMAX"}}
    with pytest.raises(SchemaException):
        lib.create_column_stats(sym, column_stats_dict)


def test_column_stats_multiple_creates(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_multiple_creates"
    base_expected_column_stats = generate_symbol(lib, sym)

    column_stats_dict_1 = {"col_1": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict_1)
    assert lib.get_column_stats_info(sym) == column_stats_dict_1

    expected_column_stats = base_expected_column_stats.select(
        ["start_index", "end_index", "v1_MIN(col_1)", "v1_MAX(col_1)"]
    )
    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    # Re-adding the same index should be idempotent
    lib.create_column_stats(sym, column_stats_dict_1)
    assert lib.get_column_stats_info(sym) == column_stats_dict_1
    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    column_stats_dict_2 = {"col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict_2)
    column_stats_dict_merged = column_stats_dict_1.copy()
    column_stats_dict_merged.update(column_stats_dict_2)
    assert lib.get_column_stats_info(sym) == column_stats_dict_merged

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, base_expected_column_stats)


def test_column_stats_string_column_minmax(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_string_column_minmax"
    generate_symbol(lib, sym)

    column_stats_dict = {"col_0": {"MINMAX"}}
    with pytest.raises(SchemaException):
        lib.create_column_stats(sym, column_stats_dict)


def test_column_stats_duplicated_primary_index(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_duplicated_primary_index"

    total_df = pd.concat((df0, df1))
    lib.write(sym, total_df)
    expected_column_stats = index_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [df0["col_1"].min(), df1["col_1"].min()]),
        pl.Series("v1_MAX(col_1)", [df0["col_1"].max(), df1["col_1"].max()]),
    )

    column_stats_dict = {"col_1": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_dynamic_schema_missing_data(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        dynamic_schema=True,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_dynamic_schema_missing_data"

    df0 = pd.DataFrame({"col_1": [0.1, 0.2], "col_2": [0.3, 0.4]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_2": [0.5, 0.6]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [0.7, 0.8]}, index=pd.date_range("2000-01-05", periods=2))
    df3 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-07", periods=2))
    df4 = pd.DataFrame(
        {"col_1": [0.9, np.nan], "col_2": [np.nan, np.nan]}, index=pd.date_range("2000-01-09", periods=2)
    )
    df5 = pd.DataFrame({"col_5": np.array([10, 20], dtype=np.int64)}, index=pd.date_range("2000-01-11", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    lib.append(sym, df4)
    lib.append(sym, df5)

    # Slices that are missing the column come back as null via the validity bitmap. df4["col_2"] is
    # all-NaN but the column is present so the stat is computed and stored as NaN, not null.
    expected_column_stats = index_columns_to_pl(lib, sym).with_columns(
        pl.Series(
            "v1_MIN(col_1)",
            [df0["col_1"].min(), None, df2["col_1"].min(), None, df4["col_1"].min(), None],
        ),
        pl.Series(
            "v1_MAX(col_1)",
            [df0["col_1"].max(), None, df2["col_1"].max(), None, df4["col_1"].max(), None],
        ),
        pl.Series(
            "v1_MIN(col_2)",
            [df0["col_2"].min(), df1["col_2"].min(), None, None, df4["col_2"].min(), None],
        ),
        pl.Series(
            "v1_MAX(col_2)",
            [df0["col_2"].max(), df1["col_2"].max(), None, None, df4["col_2"].max(), None],
        ),
        pl.Series("v1_MIN(col_5)", [None, None, None, None, None, df5["col_5"].min()], dtype=pl.Int64),
        pl.Series("v1_MAX(col_5)", [None, None, None, None, None, df5["col_5"].max()], dtype=pl.Int64),
    )
    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}, "col_5": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_dynamic_schema_types_changing(
    version_store_factory, lib_name, encoding_version, any_output_format
):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        dynamic_schema=True,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_dynamic_schema_types_changing"

    df0 = pd.DataFrame(
        {
            "int_widening": np.arange(0, 2, dtype=np.uint8),
            "int_narrowing": np.arange(-1002, -1000, dtype=np.int16),
            "unsigned_to_wider_signed_int": np.arange(1000, 1002, dtype=np.uint16),
            "wider_signed_to_unsigned_int": np.arange(-1002, -1000, dtype=np.int32),
            "unsigned_to_signed_int_same_width": np.arange(1000, 1002, dtype=np.uint16),
            "int_to_float": np.arange(1000, 1002, dtype=np.uint16),
            "float_to_int": [1.5, 2.5],
        },
        index=pd.date_range("2000-01-01", periods=2),
    )

    df1 = pd.DataFrame(
        {
            "int_widening": np.arange(1000, 1002, dtype=np.uint16),
            "int_narrowing": np.arange(-2, 0, dtype=np.int8),
            "unsigned_to_wider_signed_int": np.arange(-1002, -1000, dtype=np.int32),
            "wider_signed_to_unsigned_int": np.arange(1000, 1002, dtype=np.uint16),
            "unsigned_to_signed_int_same_width": np.arange(-1002, -1000, dtype=np.int16),
            "int_to_float": [1.5, 2.5],
            "float_to_int": np.arange(1000, 1002, dtype=np.uint16),
        },
        index=pd.date_range("2000-01-03", periods=2),
    )

    lib.write(sym, df0)
    lib.append(sym, df1)

    def int_minmax(name):
        return [
            pl.Series(f"v1_MIN({name})", [int(df0[name].min()), int(df1[name].min())]),
            pl.Series(f"v1_MAX({name})", [int(df0[name].max()), int(df1[name].max())]),
        ]

    def float_minmax(name):
        return [
            pl.Series(f"v1_MIN({name})", [float(df0[name].min()), float(df1[name].min())]),
            pl.Series(f"v1_MAX({name})", [float(df0[name].max()), float(df1[name].max())]),
        ]

    expected_column_stats = index_columns_to_pl(lib, sym).with_columns(
        *int_minmax("int_widening"),
        *int_minmax("int_narrowing"),
        *int_minmax("unsigned_to_wider_signed_int"),
        *int_minmax("wider_signed_to_unsigned_int"),
        *int_minmax("unsigned_to_signed_int_same_width"),
        *float_minmax("int_to_float"),
        *float_minmax("float_to_int"),
    )
    column_stats_dict = {
        "int_widening": {"MINMAX"},
        "int_narrowing": {"MINMAX"},
        "unsigned_to_wider_signed_int": {"MINMAX"},
        "wider_signed_to_unsigned_int": {"MINMAX"},
        "unsigned_to_signed_int_same_width": {"MINMAX"},
        "int_to_float": {"MINMAX"},
        "float_to_int": {"MINMAX"},
    }
    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    schema = pl.from_arrow(column_stats).schema

    assert schema["v1_MIN(int_widening)"] == pl.UInt16
    assert schema["v1_MAX(int_widening)"] == pl.UInt16

    assert schema["v1_MIN(int_narrowing)"] == pl.Int16
    assert schema["v1_MAX(int_narrowing)"] == pl.Int16

    assert schema["v1_MIN(unsigned_to_wider_signed_int)"] == pl.Int32
    assert schema["v1_MAX(unsigned_to_wider_signed_int)"] == pl.Int32

    assert schema["v1_MIN(wider_signed_to_unsigned_int)"] == pl.Int32
    assert schema["v1_MAX(wider_signed_to_unsigned_int)"] == pl.Int32

    assert schema["v1_MIN(unsigned_to_signed_int_same_width)"] == pl.Int32
    assert schema["v1_MAX(unsigned_to_signed_int_same_width)"] == pl.Int32

    assert schema["v1_MIN(int_to_float)"] == pl.Float64
    assert schema["v1_MAX(int_to_float)"] == pl.Float64

    assert schema["v1_MIN(float_to_int)"] == pl.Float64
    assert schema["v1_MAX(float_to_int)"] == pl.Float64


def test_column_stats_object_deleted_with_index_key(lmdb_version_store, any_output_format):
    def clear():
        nonlocal expected_count
        lib.version_store.clear()
        expected_count = 0

    def create_stats():
        nonlocal expected_count
        lib.create_column_stats(sym, column_stats_dict)
        expected_count += 1

    def assert_column_stats_key_count():
        assert lib_tool.count_keys(KeyType.COLUMN_STATS) == expected_count

    def test_delete():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.delete(sym)
        expected_count = 0
        assert_column_stats_key_count()

    def test_delete_version():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.delete_version(sym, 0)
        expected_count = 1
        assert_column_stats_key_count()

    def test_delete_versions():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df2)
        create_stats()
        assert_column_stats_key_count()
        lib.delete_versions(sym, [0, 1])
        expected_count = 1
        assert_column_stats_key_count()

    def test_delete_snapshot():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        snapshot_name = "snapshot_name"
        lib.snapshot(snapshot_name)
        lib.delete(sym)
        # Version now kept alive by snapshot
        assert_column_stats_key_count()
        lib.delete_snapshot(snapshot_name)
        # Last reference to version 0 has been deleted, so column stats key should be deleted as well
        expected_count = 0
        assert_column_stats_key_count()

    def test_add_to_snapshot():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        snapshot_name = "snapshot_name"
        lib.snapshot(snapshot_name)
        lib.delete(sym)
        # Version now kept alive by snapshot
        assert_column_stats_key_count()
        lib.write(sym, df1)
        lib.add_to_snapshot(snapshot_name, [sym])
        # Last reference to version 0 has been deleted, so column stats key should be deleted as well
        expected_count = 0
        assert_column_stats_key_count()

    def test_remove_from_snapshot():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        snapshot_name = "snapshot_name"
        lib.snapshot(snapshot_name)
        lib.delete(sym)
        # Version now kept alive by snapshot
        assert_column_stats_key_count()
        lib.remove_from_snapshot(snapshot_name, [sym], [0])
        # Last reference to version 0 has been deleted, so column stats key should be deleted as well
        expected_count = 0
        assert_column_stats_key_count()

    def test_prune_previous_kwarg():
        nonlocal expected_count
        for operation in ["write", "append", "update", "write_metadata"]:
            lib.write(sym, df0)
            create_stats()
            assert_column_stats_key_count()
            getattr(lib, operation)(sym, df1, prune_previous_version=True)
            expected_count = 0
            assert_column_stats_key_count()
            clear()

    def test_prune_previous_kwarg_batch_methods():
        nonlocal expected_count
        for operation in ["batch_write", "batch_append", "batch_write_metadata"]:
            lib.write(sym, df0)
            create_stats()
            assert_column_stats_key_count()
            getattr(lib, operation)([sym], [df1], prune_previous_version=True)
            expected_count = 0
            assert_column_stats_key_count()
            clear()

    def test_prune_previous_api():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.prune_previous_versions(sym)
        expected_count = 1
        assert_column_stats_key_count()

    lib = lmdb_version_store
    lib._set_output_format_for_pipeline_tests(any_output_format)
    lib_tool = lib.library_tool()
    sym = "test_column_stats_object_deleted_with_index_key"
    column_stats_dict = {"col_1": {"MINMAX"}}
    expected_count = 0

    for test in [
        test_delete,
        test_delete_version,
        test_delete_versions,
        test_delete_snapshot,
        test_add_to_snapshot,
        test_remove_from_snapshot,
        test_prune_previous_kwarg,
        test_prune_previous_kwarg_batch_methods,
        test_prune_previous_api,
    ]:
        test()
        clear()


def read_column_stats_header(lib, sym):
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert len(keys) == 1
    seg = lib_tool.read_to_segment_in_memory(keys[0])
    any_msg = ProtobufAny()
    any_msg.ParseFromString(seg.metadata())
    header = ColumnStatsHeader()
    assert any_msg.Unpack(header)
    return header


def header_stat_pairs(header):
    return {
        (data_col_offset, entry.type)
        for data_col_offset, entry_list in header.stats_by_column.items()
        for entry in entry_list.entries
    }


def header_stat_count(header):
    return sum(len(entry_list.entries) for entry_list in header.stats_by_column.values())


def header_all_entries(header):
    """Yield (data_col_offset, entry) for every stat entry in the header."""
    for data_col_offset, entry_list in header.stats_by_column.items():
        for entry in entry_list.entries:
            yield data_col_offset, entry


def test_column_stats_header_metadata(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_header_metadata"
    generate_symbol(lib, sym)

    # Create stats for col_1
    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})
    header = read_column_stats_header(lib, sym)

    assert header.version == 1
    assert header_stat_count(header) == 2
    assert header_stat_pairs(header) == {
        (2, ColumnStatsType.MIN_V1),
        (2, ColumnStatsType.MAX_V1),
    }
    offsets = [entry.stats_seg_offset for _, entry in header_all_entries(header)]
    assert len(set(offsets)) == 2

    # Verify descriptor field names match the offsets
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    fields = lib_tool.read_descriptor(keys[0]).fields()
    for _, entry in header_all_entries(header):
        field_name = fields[entry.stats_seg_offset].name
        if entry.type == ColumnStatsType.MIN_V1:
            assert field_name == "v1_MIN(col_1)"
        else:
            assert field_name == "v1_MAX(col_1)"

    # Create stats for col_2 over existing col_1 stats
    lib.create_column_stats(sym, {"col_2": {"MINMAX"}})
    header = read_column_stats_header(lib, sym)

    assert header.version == 1
    assert header_stat_count(header) == 4
    assert header_stat_pairs(header) == {
        (2, ColumnStatsType.MIN_V1),
        (2, ColumnStatsType.MAX_V1),
        (3, ColumnStatsType.MIN_V1),
        (3, ColumnStatsType.MAX_V1),
    }
    offsets = [entry.stats_seg_offset for _, entry in header_all_entries(header)]
    assert len(set(offsets)) == 4

    # Drop col_1 stats
    lib.drop_column_stats(sym, {"col_1": {"MINMAX"}})
    header = read_column_stats_header(lib, sym)

    assert header.version == 1
    # if you change the structure, consider whether you need to change header.version too
    assert len(header.ListFields()) == 2
    assert header_stat_count(header) == 2
    assert header_stat_pairs(header) == {
        (3, ColumnStatsType.MIN_V1),
        (3, ColumnStatsType.MAX_V1),
    }

    keys = lib_tool.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    fields = lib_tool.read_descriptor(keys[0]).fields()
    for _, entry in header_all_entries(header):
        field_name = fields[entry.stats_seg_offset].name
        if entry.type == ColumnStatsType.MIN_V1:
            assert field_name == "v1_MIN(col_2)"
        else:
            assert field_name == "v1_MAX(col_2)"


def test_column_stats_duplicated_column_names(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_basic_flow_duplicated_column_names"
    df = pd.DataFrame([[7, 1, 6], [8, 2, 5]], index=pd.date_range("2000-01-01", periods=2))
    df.columns = ["col_0", "col_1", "col_1"]

    lib.write(sym, df)
    # Check we're testing the right thing
    saved_col_names = lib.get_info(sym)["normalization_metadata"].df.common.col_names
    assert len(saved_col_names) == 3
    assert "col_0" in saved_col_names
    assert "__col_col_1__1" in saved_col_names
    assert "__col_col_1__2" in saved_col_names

    column_stats_dict = {"col_1": {"MINMAX"}}
    with pytest.raises(UserInputException):
        lib.create_column_stats(sym, column_stats_dict)
    lt = lib.library_tool()
    stats_keys = lt.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert not stats_keys

    column_stats_dict = {"col_0": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    stats_keys = lt.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert len(stats_keys) == 1

    res = lib.read_column_stats(sym)
    expected = pl.DataFrame(
        {
            "start_index": pl.Series([pd.Timestamp("2000-01-01").value], dtype=pl.Int64).cast(pl.Datetime("ns")),
            "end_index": (
                pl.Series([(pd.Timestamp("2000-01-02") + pd.Timedelta(1)).value], dtype=pl.Int64).cast(
                    pl.Datetime("ns")
                )
            ),
            "v1_MIN(col_0)": [7],
            "v1_MAX(col_0)": [8],
        }
    )
    assert_stats_equal(res, expected)


@pytest.mark.parametrize("index_name", ("index", "some-other-name"))
def test_column_stats_col_called_index(
    version_store_factory, lib_name, encoding_version, any_output_format, index_name
):
    """Check some edge cases where the data column's name matches the index column. 'index' is used as an internal
    name for un-named indexes, so using it can expose some bugs."""
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_col_called_index"
    col_name = index_name or "index"
    df = pd.DataFrame({col_name: [0, 1]}, index=pd.date_range("2000-01-01", periods=2))
    lib.write(sym, df)

    column_stats_dict = {col_name: {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    stats_keys = lib.library_tool().find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert len(stats_keys) == 1

    if index_name == "index":
        expected_title = "__col_index__0"
    else:
        expected_title = index_name
    res = lib.read_column_stats(sym)
    expected = pl.DataFrame(
        {
            "start_index": pl.Series([pd.Timestamp("2000-01-01").value], dtype=pl.Int64).cast(pl.Datetime("ns")),
            "end_index": (
                pl.Series([(pd.Timestamp("2000-01-02") + pd.Timedelta(1)).value], dtype=pl.Int64).cast(
                    pl.Datetime("ns")
                )
            ),
            f"v1_MIN({expected_title})": [0],
            f"v1_MAX({expected_title})": [1],
        }
    )
    assert_stats_equal(res, expected)

    # Now do some filtering. We apply query builder filters to the index column first!
    # We should make sure we haven't messed them up.
    if index_name == "index":
        q = QueryBuilder()
        q = q[q[index_name] == pd.Timestamp("2000-01-01")]
        res = lib.read(sym, query_builder=q).data
        assert res.shape == (1, 1)
        assert res.values == [[0]]


def test_column_stats_none_key(version_store_factory, lib_name, encoding_version, any_output_format):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_none_key"
    df = pd.DataFrame({None: [0, 1], "col_one": [2, 3]}, index=pd.date_range("2000-01-01", periods=2))
    lib.write(sym, df)

    column_stats_dict = {None: {"MINMAX"}}
    with pytest.raises(UserInputException):
        lib.create_column_stats(sym, column_stats_dict)
    stats_keys = lib.library_tool().find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert len(stats_keys) == 0

    lib.create_column_stats(sym, {"col_one": {"MINMAX"}})
    with pytest.raises(UserInputException):
        lib.drop_column_stats(sym, column_stats_dict)


@pytest.mark.parametrize(
    "level_name, index_level_name, stored_col_name",
    [
        pytest.param("level", "level", "__idx__level", id="user_name"),
        pytest.param("__idx__level", "level", "__idx__level", id="mangled_name"),
        pytest.param("__fkidx__1", None, "__fkidx__1", id="unnamed_level"),
    ],
)
def test_column_stats_multiindex(
    version_store_factory, lib_name, encoding_version, any_output_format, level_name, index_level_name, stored_col_name
):
    """Column stats on a multiindex DataFrame: stats on inner index levels and data columns.
    The inner index level can be referenced by the user-facing name ("level"), the mangled name ("__idx__level"),
    or the __fkidx__ name for unnamed index levels."""
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_multiindex"

    # Two writes to get multiple segments. With segment_row_size=2, each write produces one segment.
    dt1 = pd.Timestamp("2000-01-01")
    dt2 = pd.Timestamp("2000-01-01T12:00")
    dt3 = pd.Timestamp("2000-01-02")
    dt4 = pd.Timestamp("2000-01-02T12:00")
    mi0 = pd.MultiIndex.from_arrays(
        [[dt1, dt2], [10, 20]],
        names=["datetime", index_level_name],
    )
    df0 = pd.DataFrame({"val": [100, 200]}, index=mi0)

    mi1 = pd.MultiIndex.from_arrays(
        [[dt3, dt4], [30, 40]],
        names=["datetime", index_level_name],
    )
    df1 = pd.DataFrame({"val": [300, 400]}, index=mi1)

    lib.write(sym, df0)
    lib.append(sym, df1)

    # Check we're testing the right thing
    saved_col_names = lib.get_info(sym)["normalization_metadata"].df.common.col_names
    assert len(saved_col_names) == 2
    assert stored_col_name in saved_col_names
    assert "val" in saved_col_names

    full_df = pd.concat([df0, df1])

    # Create column stats on the inner index level and a data column
    column_stats_dict = {level_name: {"MINMAX"}, "val": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == {stored_col_name: {"MINMAX"}, "val": {"MINMAX"}}

    # Read and verify the stats
    stats = lib.read_column_stats(sym)
    expected = index_columns_to_pl(lib, sym).with_columns(
        pl.Series(f"v1_MIN({stored_col_name})", [10, 30]),
        pl.Series(f"v1_MAX({stored_col_name})", [20, 40]),
        pl.Series("v1_MIN(val)", [100, 300]),
        pl.Series("v1_MAX(val)", [200, 400]),
    )
    assert_stats_equal(stats, expected)

    # QueryBuilder filter on the inner index level
    q = QueryBuilder()
    q = q[q[stored_col_name] == 20]
    result = lib.read(sym, query_builder=q).data
    expected_filtered = full_df[full_df.index.get_level_values(1) == 20]
    pd.testing.assert_frame_equal(result, expected_filtered)

    # QueryBuilder filter on the data column
    q = QueryBuilder()
    q = q[q["val"] >= 300]
    result = lib.read(sym, query_builder=q).data
    expected_filtered = full_df[full_df["val"] >= 300]
    pd.testing.assert_frame_equal(result, expected_filtered)

    # Drop stats for one column, verify the other remains
    lib.drop_column_stats(sym, {level_name: {"MINMAX"}})
    assert lib.get_column_stats_info(sym) == {"val": {"MINMAX"}}

    stats = lib.read_column_stats(sym)
    expected = expected.drop([f"v1_MIN({stored_col_name})", f"v1_MAX({stored_col_name})"])
    assert_stats_equal(stats, expected)

    # Drop all remaining stats
    lib.drop_column_stats(sym)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym)


def test_column_stats_multiindex_same_name_as_data_col(
    version_store_factory, lib_name, encoding_version, any_output_format
):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_multiindex"

    dt1 = pd.Timestamp("2000-01-01")
    dt2 = pd.Timestamp("2000-01-01T12:00")
    mi0 = pd.MultiIndex.from_arrays(
        [[dt1, dt2], [10, 20]],
        names=["datetime", "level"],
    )
    df0 = pd.DataFrame({"level": [100, 200]}, index=mi0)

    lib.write(sym, df0)
    # Check we're testing the right thing
    saved_col_names = lib.get_info(sym)["normalization_metadata"].df.common.col_names
    assert len(saved_col_names) == 2
    assert "__idx__level" in saved_col_names
    assert "level" in saved_col_names

    column_stats_dict = {"level": {"MINMAX"}}
    with pytest.raises(UserInputException):
        lib.create_column_stats(sym, column_stats_dict)


def test_column_stats_multiindex_outer_level_not_possible(
    version_store_factory, lib_name, encoding_version, any_output_format
):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "sym"

    dt1 = pd.Timestamp("2000-01-01")
    dt2 = pd.Timestamp("2000-01-02")
    mi = pd.MultiIndex.from_arrays(
        [[dt1, dt2], [10, 20]],
        names=["datetime", "level"],
    )
    df = pd.DataFrame({"val": [100, 200]}, index=mi)
    lib.write(sym, df)

    # Can't create stats over the real index
    with pytest.raises(UserInputException):
        lib.create_column_stats(sym, {"datetime": {"MINMAX"}})
    stats_keys = lib.library_tool().find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert not stats_keys

    # Note: We don't support writing a multi-indexed dataframe with duplicate level names, so no need to test that
    # with column stats.


def test_column_stats_create_tiny_thread_pool(
    version_store_factory, lib_name, encoding_version, any_output_format, tiny_thread_pool
):
    """Simple test with tiny thread pool to check against deadlocks from the parallel load of the index key
    and the column stats key."""
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_create_tiny_thread_pool"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_dict_1 = {"col_1": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict_1)
    assert lib.get_column_stats_info(sym) == column_stats_dict_1

    column_stats_dict_2 = {"col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict_2)
    column_stats_dict_merged = {**column_stats_dict_1, **column_stats_dict_2}
    assert lib.get_column_stats_info(sym) == column_stats_dict_merged

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_drop_tiny_thread_pool(
    version_store_factory, lib_name, encoding_version, any_output_format, tiny_thread_pool
):
    """Simple test with tiny thread pool to check against deadlocks from the parallel load of the index key
    and the column stats key."""
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_drop_tiny_thread_pool"
    generate_symbol(lib, sym)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}})

    lib.drop_column_stats(sym, {"col_1": {"MINMAX"}})
    assert lib.get_column_stats_info(sym) == {"col_2": {"MINMAX"}}

    lib.drop_column_stats(sym)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)


# k=0 is the kill switch: it admits every processing unit at once, disabling the memory bound.
@pytest.mark.parametrize("k", [0, 1, 2, 1000])
def test_column_stats_create_independent_of_admission_ceiling(
    version_store_factory, lib_name, encoding_version, any_output_format, k
):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}_{k}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_admission_ceiling"
    expected_column_stats = generate_symbol(lib, sym).select(
        ["start_index", "end_index", "v1_MIN(col_1)", "v1_MAX(col_1)", "v1_MIN(col_2)", "v1_MAX(col_2)"]
    )
    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}

    with config_context(ADMISSION_KEY, k):
        lib.create_column_stats(sym, column_stats_dict)

    assert lib.get_column_stats_info(sym) == column_stats_dict
    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)


@pytest.mark.parametrize("k", [1, 2])
def test_column_stats_create_admission_tiny_thread_pool(
    version_store_factory, lib_name, encoding_version, any_output_format, tiny_thread_pool, k
):
    """Test against deadlock with a small admission limit and single thread pools."""
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}_{k}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_admission_tiny_thread_pool"
    n_appends = 10
    write_many_slices(lib, sym, n_appends)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    with config_context(ADMISSION_KEY, k):
        lib.create_column_stats(sym, column_stats_dict)

    assert lib.get_column_stats_info(sym) == column_stats_dict
    # One stats row per row slice.
    assert lib.read_column_stats(sym).num_rows == n_appends


def test_column_stats_create_single_unit(
    version_store_factory, lib_name, encoding_version, any_output_format, tiny_thread_pool
):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_single_unit"
    df = pd.DataFrame({"col_1": [1.0, 3.0]}, index=pd.date_range("2000-01-01", periods=2))
    lib.write(sym, df)
    expected_column_stats = index_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [df["col_1"].min()]),
        pl.Series("v1_MAX(col_1)", [df["col_1"].max()]),
    )

    with config_context(ADMISSION_KEY, 1):
        lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)
