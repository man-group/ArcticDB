"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

"""
Systematic, field-by-field coverage of the *pandas* NormalizationMetadata.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.exceptions import ArcticDbNotYetImplemented
from arcticdb.util.test import assert_frame_equal, assert_series_equal
import arcticc.pb2.descriptors_pb2 as descriptors_pb2

ColumnName = descriptors_pb2.NormalizationMetadata.Pandas.ColumnName


def _read_norm_meta(lib, sym):
    """Return the persisted NormalizationMetadata protobuf for ``sym``."""
    version_query = lib._get_version_query(None)
    descriptor = lib.version_store.read_descriptor(sym, version_query)
    return descriptor.timeseries_descriptor.normalization


# ---------------------------------------------------------------------------
# input_type oneof + Pandas.mark
# ---------------------------------------------------------------------------


def test_dataframe_uses_df_input_type(lmdb_version_store_v1):
    """A DataFrame is stored under the ``df`` input_type; ``common.mark`` keeps the message non-empty."""
    lib = lmdb_version_store_v1
    sym = "test_dataframe_uses_df_input_type"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=3))
    lib.write(sym, df)

    norm_meta = _read_norm_meta(lib, sym)
    assert norm_meta.WhichOneof("input_type") == "df"
    assert norm_meta.df.common.mark
    assert_frame_equal(df, lib.read(sym).data)


def test_series_uses_series_input_type(lmdb_version_store_v1):
    """A Series is stored under the ``series`` input_type (normalized as a one-column frame)."""
    lib = lmdb_version_store_v1
    sym = "test_series_uses_series_input_type"
    series = pd.Series(np.arange(3, dtype=np.int64), index=pd.date_range("2025-01-01", periods=3), name="s")
    lib.write(sym, series)

    norm_meta = _read_norm_meta(lib, sym)
    assert norm_meta.WhichOneof("input_type") == "series"
    assert_series_equal(series, lib.read(sym).data)


# ---------------------------------------------------------------------------
# PandasIndex — RangeIndex (is_physically_stored / start / step / name / is_int)
# ---------------------------------------------------------------------------


def test_default_range_index_not_physically_stored(lmdb_version_store_v1):
    """A default RangeIndex is not physically stored; it is rebuilt from ``start``/``step``."""
    lib = lmdb_version_store_v1
    sym = "test_default_range_index_not_physically_stored"
    df = pd.DataFrame({"col": np.arange(4, dtype=np.int64)})
    lib.write(sym, df)

    index_meta = _read_norm_meta(lib, sym).df.common.index
    assert not index_meta.is_physically_stored
    assert index_meta.start == 0
    assert index_meta.step == 1
    received = lib.read(sym).data
    assert isinstance(received.index, pd.RangeIndex)
    assert_frame_equal(df, received)


def test_range_index_custom_start_and_step(lmdb_version_store_v1):
    """A non-default RangeIndex stores ``start`` and ``step`` so the exact index is rebuilt."""
    lib = lmdb_version_store_v1
    sym = "test_range_index_custom_start_and_step"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.RangeIndex(start=5, stop=11, step=2))
    lib.write(sym, df)

    index_meta = _read_norm_meta(lib, sym).df.common.index
    assert not index_meta.is_physically_stored
    assert index_meta.start == 5
    assert index_meta.step == 2
    assert_frame_equal(df, lib.read(sym).data)


def test_range_index_named(lmdb_version_store_v1):
    """A named RangeIndex stores and round-trips ``index.name``."""
    lib = lmdb_version_store_v1
    sym = "test_range_index_named"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.RangeIndex(3, name="my_range"))
    lib.write(sym, df)

    index_meta = _read_norm_meta(lib, sym).df.common.index
    assert index_meta.name == "my_range"
    assert_frame_equal(df, lib.read(sym).data)


# ---------------------------------------------------------------------------
# PandasIndex — physically stored index (is_physically_stored / name / fake_name / is_int / tz)
# ---------------------------------------------------------------------------


def test_named_datetime_index_physically_stored(lmdb_version_store_v1):
    """A named DatetimeIndex is physically stored with its name and ``fake_name`` False."""
    lib = lmdb_version_store_v1
    sym = "test_named_datetime_index_physically_stored"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=3))
    df.index.name = "ts"
    lib.write(sym, df)

    index_meta = _read_norm_meta(lib, sym).df.common.index
    assert index_meta.is_physically_stored
    assert index_meta.name == "ts"
    assert not index_meta.fake_name
    assert_frame_equal(df, lib.read(sym).data)


def test_empty_string_index_name(lmdb_version_store_v1):
    """An "" index name sets ``fake_name=False`` and ``name=""``"""
    lib = lmdb_version_store_v1
    sym = "test_empty_string_index_name"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.Index([10, 20, 30], dtype=np.int64))
    df.index.name = ""
    lib.write(sym, df)

    index_meta = _read_norm_meta(lib, sym).df.common.index
    assert not index_meta.fake_name
    assert index_meta.name == ""
    assert index_meta.is_physically_stored
    received = lib.read(sym).data
    assert received.index.name == ""
    assert_frame_equal(df, received)


def test_unnamed_index_sets_fake_name(lmdb_version_store_v1):
    """An unnamed index sets ``fake_name``; the name is restored to None on read."""
    lib = lmdb_version_store_v1
    sym = "test_unnamed_index_sets_fake_name"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.Index([10, 20, 30], dtype=np.int64))
    assert df.index.name is None
    lib.write(sym, df)

    index_meta = _read_norm_meta(lib, sym).df.common.index
    assert index_meta.fake_name
    assert index_meta.is_physically_stored
    received = lib.read(sym).data
    assert received.index.name is None
    assert_frame_equal(df, received)


def test_index_name_int(lmdb_version_store_v1):
    """An integer index name sets ``is_int`` so it is cast back to int on read."""
    lib = lmdb_version_store_v1
    sym = "test_index_name_int"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.Index([10, 20, 30], dtype=np.int64))
    df.index.name = 7
    lib.write(sym, df)

    index_meta = _read_norm_meta(lib, sym).df.common.index
    assert index_meta.is_int
    assert index_meta.name == "7"
    received = lib.read(sym).data
    assert received.index.name == 7
    assert_frame_equal(df, received)


def test_index_timezone(lmdb_version_store_v1):
    """A tz-aware DatetimeIndex stores its timezone and re-applies it on read."""
    lib = lmdb_version_store_v1
    sym = "test_index_timezone"
    df = pd.DataFrame(
        {"col": np.arange(3, dtype=np.int64)},
        index=pd.date_range("2025-01-01", periods=3, tz="America/New_York"),
    )
    df.index.name = "ts"
    lib.write(sym, df)

    index_meta = _read_norm_meta(lib, sym).df.common.index
    assert index_meta.tz == "America/New_York"
    assert_frame_equal(df, lib.read(sym).data)


@pytest.mark.xfail(reason="Pandas normalization does not preserve non-index column timezones", strict=True)
def test_non_index_column_timezone_not_preserved(lmdb_version_store_v1):
    """The pandas metadata carries a timezone only for the index (no per-column tz field)"""
    lib = lmdb_version_store_v1
    sym = "test_non_index_column_timezone_not_preserved"
    df = pd.DataFrame(
        {"col": pd.date_range("2024-06-01", periods=3, tz="Europe/London")},
        index=pd.date_range("2025-01-01", periods=3),
    )
    df.index.name = "ts"
    lib.write(sym, df)
    assert_frame_equal(df, lib.read(sym).data)


# ---------------------------------------------------------------------------
# Pandas.name / Pandas.has_name (Series only)
# ---------------------------------------------------------------------------


def test_series_named(lmdb_version_store_v1):
    """A named Series stores ``common.name`` and sets ``has_name``."""
    lib = lmdb_version_store_v1
    sym = "test_series_named"
    series = pd.Series(np.arange(3, dtype=np.int64), name="my_series")
    lib.write(sym, series)

    common = _read_norm_meta(lib, sym).series.common
    assert common.has_name
    assert common.name == "my_series"
    assert_series_equal(series, lib.read(sym).data)


def test_series_unnamed_has_name_false(lmdb_version_store_v1):
    """An unnamed Series leaves ``has_name`` False, so read restores ``name=None``."""
    lib = lmdb_version_store_v1
    sym = "test_series_unnamed_has_name_false"
    series = pd.Series(np.arange(3, dtype=np.int64))
    assert series.name is None
    lib.write(sym, series)

    common = _read_norm_meta(lib, sym).series.common
    assert not common.has_name
    assert common.name == ""
    received = lib.read(sym).data
    assert received.name is None
    assert_series_equal(series, received)


def test_series_empty_string_name(lmdb_version_store_v1):
    """``has_name`` lets an empty-string name survive as "" rather than None"""
    lib = lmdb_version_store_v1
    sym = "test_series_empty_string_name"
    series = pd.Series(np.arange(3, dtype=np.int64), name="")
    lib.write(sym, series)

    common = _read_norm_meta(lib, sym).series.common
    assert common.has_name
    assert common.name == ""
    received = lib.read(sym).data
    assert received.name == ""
    assert_series_equal(series, received)


@pytest.mark.xfail(reason="Pandas normalization does not preserve int series name", strict=True)
def test_series_int_name(lmdb_version_store_v1):
    """int series name should be preserved"""
    lib = lmdb_version_store_v1
    sym = "test_series_int_name"
    series = pd.Series(np.arange(3, dtype=np.int64), name=3)
    lib.write(sym, series)

    common = _read_norm_meta(lib, sym).series.common
    assert common.has_name
    assert common.name == "3"
    received = lib.read(sym).data
    assert received.name == 3
    assert_series_equal(series, received)


# ---------------------------------------------------------------------------
# Pandas.col_names (ColumnName: is_none / is_empty / is_int / original_name)
# ---------------------------------------------------------------------------


def test_column_name_none(lmdb_version_store_v1):
    """A None column name is stored as ``__none__0`` with ``is_none``; read restores None."""
    lib = lmdb_version_store_v1
    sym = "test_column_name_none"
    df = pd.DataFrame([[1, 2]], columns=[None, "b"], index=pd.date_range("2025-01-01", periods=1))
    lib.write(sym, df)

    col_names = dict(_read_norm_meta(lib, sym).df.common.col_names)
    assert col_names["__none__0"] == ColumnName(is_none=True)
    assert_frame_equal(df, lib.read(sym).data)


def test_column_name_empty(lmdb_version_store_v1):
    """An empty column name is stored as ``__empty__0`` with ``is_empty``; read restores ""."""
    lib = lmdb_version_store_v1
    sym = "test_column_name_empty"
    df = pd.DataFrame([[1, 2]], columns=["", "b"], index=pd.date_range("2025-01-01", periods=1))
    lib.write(sym, df)

    col_names = dict(_read_norm_meta(lib, sym).df.common.col_names)
    assert col_names["__empty__0"] == ColumnName(is_empty=True)
    assert_frame_equal(df, lib.read(sym).data)


def test_column_name_int(lmdb_version_store_v1):
    """An integer column name sets ``is_int`` with ``original_name``; read casts it back to int."""
    lib = lmdb_version_store_v1
    sym = "test_column_name_int"
    df = pd.DataFrame({5: np.arange(2, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=2))
    lib.write(sym, df)

    col_names = dict(_read_norm_meta(lib, sym).df.common.col_names)
    assert col_names["5"] == ColumnName(is_int=True, original_name="5")
    received = lib.read(sym).data
    assert list(received.columns) == [5]
    assert_frame_equal(df, received)


def test_column_name_original_name_on_clash_with_index(lmdb_version_store_v1):
    """A column clashing with the index name is renamed on disk but ``original_name`` restores it."""
    lib = lmdb_version_store_v1
    sym = "test_column_name_original_name_on_clash_with_index"
    df = pd.DataFrame({"a": np.arange(2, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=2))
    df.index.name = "a"
    lib.write(sym, df)

    col_names = dict(_read_norm_meta(lib, sym).df.common.col_names)
    assert any(v == ColumnName(original_name="a") for v in col_names.values())
    assert all(k.startswith("__col_a__") for k in col_names)
    received = lib.read(sym).data
    assert list(received.columns) == ["a"]
    assert_frame_equal(df, received)


def test_duplicate_column_names(lmdb_version_store_v1):
    """Duplicate columns are disambiguated on disk but share ``original_name``, so they round-trip."""
    lib = lmdb_version_store_v1
    sym = "test_duplicate_column_names"
    df = pd.DataFrame([[1, 2]], columns=["a", "a"], index=pd.date_range("2025-01-01", periods=1))
    lib.write(sym, df)

    col_names = dict(_read_norm_meta(lib, sym).df.common.col_names)
    assert col_names["__col_a__0"] == ColumnName(original_name="a")
    assert col_names["__col_a__1"] == ColumnName(original_name="a")
    received = lib.read(sym).data
    assert list(received.columns) == ["a", "a"]
    assert_frame_equal(df, received)


# ---------------------------------------------------------------------------
# Pandas.columns (columns.name / columns.fake_name) — the columns *axis* metadata
# ---------------------------------------------------------------------------


def test_columns_axis_named(lmdb_version_store_v1):
    """A named columns axis is stored in ``common.columns.name`` and restored on read."""
    lib = lmdb_version_store_v1
    sym = "test_columns_axis_named"
    df = pd.DataFrame({"a": [1], "b": [2]}, index=pd.date_range("2025-01-01", periods=1))
    df.columns.name = "features"
    lib.write(sym, df)

    common = _read_norm_meta(lib, sym).df.common
    assert common.columns.name == "features"
    assert not common.columns.fake_name
    received = lib.read(sym).data
    assert received.columns.name == "features"
    assert_frame_equal(df, received)


def test_columns_axis_unnamed_sets_fake_name(lmdb_version_store_v1):
    """An unnamed columns axis sets ``columns.fake_name``; read leaves ``df.columns.name`` None."""
    lib = lmdb_version_store_v1
    sym = "test_columns_axis_unnamed_sets_fake_name"
    df = pd.DataFrame({"a": [1], "b": [2]}, index=pd.date_range("2025-01-01", periods=1))
    assert df.columns.name is None
    lib.write(sym, df)

    common = _read_norm_meta(lib, sym).df.common
    assert common.columns.fake_name
    received = lib.read(sym).data
    assert received.columns.name is None
    assert_frame_equal(df, received)


# ---------------------------------------------------------------------------
# PandasDataFrame.has_synthetic_columns
# ---------------------------------------------------------------------------


def test_has_synthetic_columns(lmdb_version_store_v1):
    """Unlabelled (RangeIndex) columns set ``has_synthetic_columns``; read rebuilds a RangeIndex."""
    lib = lmdb_version_store_v1
    sym = "test_has_synthetic_columns"
    df = pd.DataFrame([[1, 2], [3, 4]], index=pd.date_range("2025-01-01", periods=2))
    assert isinstance(df.columns, pd.RangeIndex)
    lib.write(sym, df)

    assert _read_norm_meta(lib, sym).df.has_synthetic_columns
    received = lib.read(sym).data
    assert isinstance(received.columns, pd.RangeIndex)
    assert_frame_equal(df, received)


def test_named_columns_are_not_synthetic(lmdb_version_store_v1):
    """Explicit column labels leave ``has_synthetic_columns`` False."""
    lib = lmdb_version_store_v1
    sym = "test_named_columns_are_not_synthetic"
    df = pd.DataFrame({"a": [1], "b": [2]}, index=pd.date_range("2025-01-01", periods=1))
    lib.write(sym, df)

    assert not _read_norm_meta(lib, sym).df.has_synthetic_columns
    assert_frame_equal(df, lib.read(sym).data)


# ---------------------------------------------------------------------------
# Pandas.categories / Pandas.int_categories
# ---------------------------------------------------------------------------


def test_string_categorical_column(lmdb_version_store_v1):
    """A string categorical column stores its categories in ``common.categories``."""
    lib = lmdb_version_store_v1
    sym = "test_string_categorical_column"
    df = pd.DataFrame({"c": pd.Categorical(["a", "b", "a"])}, index=pd.date_range("2025-01-01", periods=3))
    lib.write(sym, df)

    common = _read_norm_meta(lib, sym).df.common
    assert list(common.categories["c"].category) == ["a", "b"]
    assert_frame_equal(df, lib.read(sym).data)


def test_int_categorical_column(lmdb_version_store_v1):
    """An integer categorical column uses ``common.int_categories``."""
    lib = lmdb_version_store_v1
    sym = "test_int_categorical_column"
    df = pd.DataFrame({"c": pd.Categorical([10, 20, 10])}, index=pd.date_range("2025-01-01", periods=3))
    lib.write(sym, df)

    common = _read_norm_meta(lib, sym).df.common
    assert list(common.int_categories["c"].category) == [10, 20]
    assert_frame_equal(df, lib.read(sym).data)


# ---------------------------------------------------------------------------
# PandasMultiIndex (field_count / name / tz / timezone / fake_field_pos / is_int)
# ---------------------------------------------------------------------------


def _multiindex_df(names, tzs=None, periods=2):
    """Build a DataFrame with a MultiIndex whose first level is a DatetimeIndex."""
    tzs = tzs or [None] * len(names)
    arrays = []
    for level, tz in enumerate(tzs):
        if level == 0 or tz is not None:
            arrays.append(pd.date_range("2025-01-01", periods=periods, tz=tz))
        else:
            arrays.append([f"v{level}_{i}" for i in range(periods)])
    index = pd.MultiIndex.from_arrays(arrays, names=names)
    return pd.DataFrame({"col": np.arange(periods, dtype=np.int64)}, index=index)


def test_multiindex_field_count(lmdb_version_store_v1):
    """``field_count`` is the number of index levels beyond the first."""
    lib = lmdb_version_store_v1
    sym = "test_multiindex_field_count"
    df = _multiindex_df(names=["l0", "l1", "l2"])
    lib.write(sym, df)

    assert _read_norm_meta(lib, sym).df.common.multi_index.field_count == 2
    assert_frame_equal(df, lib.read(sym).data)


def test_multiindex_first_level_name(lmdb_version_store_v1):
    """The first level name is stored in ``multi_index.name``; read restores all level names."""
    lib = lmdb_version_store_v1
    sym = "test_multiindex_first_level_name"
    df = _multiindex_df(names=["l0", "l1"])
    lib.write(sym, df)

    assert _read_norm_meta(lib, sym).df.common.multi_index.name == "l0"
    received = lib.read(sym).data
    assert list(received.index.names) == ["l0", "l1"]
    assert_frame_equal(df, received)


def test_multiindex_fake_field_pos(lmdb_version_store_v1):
    """Unnamed MultiIndex levels are recorded in ``fake_field_pos``; read restores their names to None."""
    lib = lmdb_version_store_v1
    sym = "test_multiindex_fake_field_pos"
    df = _multiindex_df(names=[None, None])
    lib.write(sym, df)

    assert set(_read_norm_meta(lib, sym).df.common.multi_index.fake_field_pos) == {0, 1}
    received = lib.read(sym).data
    assert list(received.index.names) == [None, None]
    assert_frame_equal(df, received)


def test_multiindex_first_level_tz(lmdb_version_store_v1):
    """A timezone on the first level is stored in ``multi_index.tz``."""
    lib = lmdb_version_store_v1
    sym = "test_multiindex_first_level_tz"
    df = _multiindex_df(names=["l0", "l1"], tzs=["America/New_York", None])
    lib.write(sym, df)

    assert _read_norm_meta(lib, sym).df.common.multi_index.tz == "America/New_York"
    assert_frame_equal(df, lib.read(sym).data)


def test_multiindex_higher_level_tz(lmdb_version_store_v1):
    """Timezones on levels >= 1 go into the per-level ``multi_index.timezone`` map."""
    lib = lmdb_version_store_v1
    sym = "test_multiindex_higher_level_tz"
    df = _multiindex_df(names=["l0", "l1"], tzs=[None, "Europe/London"])
    lib.write(sym, df)

    timezone = dict(_read_norm_meta(lib, sym).df.common.multi_index.timezone)
    assert timezone[1] == "Europe/London"
    assert_frame_equal(df, lib.read(sym).data)


@pytest.mark.xfail(reason="MultiIndex first-level name is stringified on write, so is_int is never set", strict=True)
def test_multiindex_first_level_int_name(lmdb_version_store_v1):
    """An integer first-level name should set ``multi_index.is_int`` and round-trip as an int, like
    a single index. It currently does not: the name is stringified before normalization, so
    ``is_int`` stays False and the name comes back as the string "7"."""
    lib = lmdb_version_store_v1
    sym = "test_multiindex_first_level_int_name"
    df = _multiindex_df(names=[7, "l1"])
    lib.write(sym, df)

    assert _read_norm_meta(lib, sym).df.common.multi_index.is_int
    received = lib.read(sym).data
    assert received.index.names[0] == 7


# ---------------------------------------------------------------------------
# Legacy / unimplemented fields
# ---------------------------------------------------------------------------


def test_multiindex_version_field_unused(lmdb_version_store_v1):
    """``PandasMultiIndex.version`` is legacy and stays at the default 0."""
    lib = lmdb_version_store_v1
    sym = "test_multiindex_version_field_unused"
    df = _multiindex_df(names=["l0", "l1"])
    lib.write(sym, df)

    assert _read_norm_meta(lib, sym).df.common.multi_index.version == 0


def test_multiindex_columns_not_supported(lmdb_version_store_v1):
    """``PandasMultiColumn`` is unimplemented: MultiIndex columns raise on write."""
    lib = lmdb_version_store_v1
    sym = "test_multiindex_columns_not_supported"
    df = pd.DataFrame(
        [[1, 2]],
        columns=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")]),
        index=pd.date_range("2025-01-01", periods=1),
    )
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.write(sym, df)
