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

from arcticdb import concat
from arcticdb.exceptions import ArcticDbNotYetImplemented, NormalizationException, SchemaException
from arcticdb.util.test import assert_frame_equal, assert_series_equal
import arcticc.pb2.descriptors_pb2 as descriptors_pb2

ColumnName = descriptors_pb2.NormalizationMetadata.Pandas.ColumnName


def _read_norm_meta(lib, sym):
    """Return the persisted NormalizationMetadata protobuf for ``sym``."""
    version_query = lib._get_version_query(None)
    descriptor = lib.version_store.read_descriptor(sym, version_query)
    return descriptor.timeseries_descriptor.normalization


def _read_df_or_series_common_meta(lib, sym):
    """Return the shared ``Pandas.common`` metadata regardless of the df/series input_type."""
    norm = _read_norm_meta(lib, sym)
    return getattr(norm, norm.WhichOneof("input_type")).common


def _build_df_or_series(series_or_df, index):
    """Build a DataFrame or Series with ``index`` and a single int column of matching length."""
    values = np.arange(len(index), dtype=np.int64)
    if series_or_df == "series":
        return pd.Series(values, index=index, name="s")
    return pd.DataFrame({"col": values}, index=index)


def _assert_df_or_series_roundtrip(lib, sym, original):
    received = lib.read(sym).data
    if isinstance(original, pd.Series):
        assert_series_equal(original, received)
    else:
        assert_frame_equal(original, received)


# ---------------------------------------------------------------------------
# input_type oneof + Pandas.mark
# ---------------------------------------------------------------------------


def test_dataframe_uses_df_input_type(in_memory_version_store):
    """A DataFrame is stored under the ``df`` input_type; ``common.mark`` keeps the message non-empty."""
    lib = in_memory_version_store
    sym = "test_dataframe_uses_df_input_type"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=3))
    lib.write(sym, df)

    norm_meta = _read_norm_meta(lib, sym)
    assert norm_meta.WhichOneof("input_type") == "df"
    assert norm_meta.df.common.mark
    assert_frame_equal(df, lib.read(sym).data)


def test_series_uses_series_input_type(in_memory_version_store):
    """A Series is stored under the ``series`` input_type (normalized as a one-column frame)."""
    lib = in_memory_version_store
    sym = "test_series_uses_series_input_type"
    series = pd.Series(np.arange(3, dtype=np.int64), index=pd.date_range("2025-01-01", periods=3), name="s")
    lib.write(sym, series)

    norm_meta = _read_norm_meta(lib, sym)
    assert norm_meta.WhichOneof("input_type") == "series"
    assert_series_equal(series, lib.read(sym).data)


# ---------------------------------------------------------------------------
# PandasIndex — RangeIndex (is_physically_stored / start / step / name / is_int)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_default_range_index_not_physically_stored(in_memory_version_store, series_or_df):
    """A default RangeIndex is not physically stored; it is rebuilt from ``start``/``step``."""
    lib = in_memory_version_store
    sym = "test_default_range_index_not_physically_stored"
    obj = _build_df_or_series(series_or_df, pd.RangeIndex(4))
    lib.write(sym, obj)

    common = _read_df_or_series_common_meta(lib, sym)
    assert common.WhichOneof("index_type") == "index"
    index_meta = common.index
    assert not index_meta.is_physically_stored
    assert index_meta.start == 0
    assert index_meta.step == 1
    assert isinstance(lib.read(sym).data.index, pd.RangeIndex)
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_range_index_custom_start_and_step(in_memory_version_store, series_or_df):
    """A non-default RangeIndex stores ``start`` and ``step`` so the exact index is rebuilt."""
    lib = in_memory_version_store
    sym = "test_range_index_custom_start_and_step"
    obj = _build_df_or_series(series_or_df, pd.RangeIndex(start=5, stop=11, step=2))
    lib.write(sym, obj)

    common = _read_df_or_series_common_meta(lib, sym)
    assert common.WhichOneof("index_type") == "index"
    index_meta = common.index
    assert not index_meta.is_physically_stored
    assert index_meta.start == 5
    assert index_meta.step == 2
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_range_index_named(in_memory_version_store, series_or_df):
    """A named RangeIndex stores and round-trips ``index.name``."""
    lib = in_memory_version_store
    sym = "test_range_index_named"
    obj = _build_df_or_series(series_or_df, pd.RangeIndex(3, name="my_range"))
    lib.write(sym, obj)

    common = _read_df_or_series_common_meta(lib, sym)
    assert common.WhichOneof("index_type") == "index"
    assert common.index.name == "my_range"
    _assert_df_or_series_roundtrip(lib, sym, obj)


# ---------------------------------------------------------------------------
# PandasIndex — physically stored index (is_physically_stored / name / fake_name / is_int / tz)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_named_datetime_index_physically_stored(in_memory_version_store, series_or_df):
    """A named DatetimeIndex is physically stored with its name, ``fake_name`` False, no tz, not int."""
    lib = in_memory_version_store
    sym = "test_named_datetime_index_physically_stored"
    obj = _build_df_or_series(series_or_df, pd.date_range("2025-01-01", periods=3))
    obj.index.name = "ts"
    lib.write(sym, obj)

    common = _read_df_or_series_common_meta(lib, sym)
    assert common.WhichOneof("index_type") == "index"
    index_meta = common.index
    assert index_meta.is_physically_stored
    assert index_meta.name == "ts"
    assert not index_meta.fake_name
    assert not index_meta.tz
    assert not index_meta.is_int
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_empty_string_index_name(in_memory_version_store, series_or_df):
    """An "" index name sets ``fake_name=False`` and ``name=""``"""
    lib = in_memory_version_store
    sym = "test_empty_string_index_name"
    obj = _build_df_or_series(series_or_df, pd.Index([10, 20, 30], dtype=np.int64))
    obj.index.name = ""
    lib.write(sym, obj)

    common = _read_df_or_series_common_meta(lib, sym)
    assert common.WhichOneof("index_type") == "index"
    index_meta = common.index
    assert not index_meta.fake_name
    assert index_meta.name == ""
    assert index_meta.is_physically_stored
    assert lib.read(sym).data.index.name == ""
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_unnamed_index_sets_fake_name(in_memory_version_store, series_or_df):
    """An unnamed index sets ``fake_name``; the name is restored to None on read."""
    lib = in_memory_version_store
    sym = "test_unnamed_index_sets_fake_name"
    obj = _build_df_or_series(series_or_df, pd.Index([10, 20, 30], dtype=np.int64))
    assert obj.index.name is None
    lib.write(sym, obj)

    common = _read_df_or_series_common_meta(lib, sym)
    assert common.WhichOneof("index_type") == "index"
    index_meta = common.index
    assert index_meta.fake_name
    assert index_meta.is_physically_stored
    assert lib.read(sym).data.index.name is None
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_index_name_int(in_memory_version_store, series_or_df):
    """An integer index name sets ``is_int`` so it is cast back to int on read."""
    lib = in_memory_version_store
    sym = "test_index_name_int"
    obj = _build_df_or_series(series_or_df, pd.Index([10, 20, 30], dtype=np.int64))
    obj.index.name = 7
    lib.write(sym, obj)

    common = _read_df_or_series_common_meta(lib, sym)
    assert common.WhichOneof("index_type") == "index"
    index_meta = common.index
    assert index_meta.is_int
    assert index_meta.name == "7"
    assert lib.read(sym).data.index.name == 7
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_index_timezone(in_memory_version_store, series_or_df):
    """A tz-aware DatetimeIndex stores its timezone and re-applies it on read."""
    lib = in_memory_version_store
    sym = "test_index_timezone"
    obj = _build_df_or_series(series_or_df, pd.date_range("2025-01-01", periods=3, tz="America/New_York"))
    obj.index.name = "ts"
    lib.write(sym, obj)

    common = _read_df_or_series_common_meta(lib, sym)
    assert common.WhichOneof("index_type") == "index"
    index_meta = common.index
    assert index_meta.tz == "America/New_York"
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.xfail(reason="Pandas normalization does not preserve non-index column timezones", strict=True)
def test_non_index_column_timezone_not_preserved(in_memory_version_store):
    """The pandas metadata carries a timezone only for the index (no per-column tz field)"""
    lib = in_memory_version_store
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


def test_dataframe_has_no_name(in_memory_version_store):
    """``name``/``has_name`` are Series-only: a DataFrame always leaves ``has_name`` False and ``name`` empty."""
    lib = in_memory_version_store
    sym = "test_dataframe_has_no_name"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=3))
    lib.write(sym, df)

    common = _read_norm_meta(lib, sym).df.common
    assert not common.has_name
    assert common.name == ""


def test_series_named(in_memory_version_store):
    """A named Series stores ``common.name`` and sets ``has_name``."""
    lib = in_memory_version_store
    sym = "test_series_named"
    series = pd.Series(np.arange(3, dtype=np.int64), name="my_series")
    lib.write(sym, series)

    common = _read_norm_meta(lib, sym).series.common
    assert common.has_name
    assert common.name == "my_series"
    assert_series_equal(series, lib.read(sym).data)


def test_series_unnamed_has_name_false(in_memory_version_store):
    """An unnamed Series leaves ``has_name`` False, so read restores ``name=None``."""
    lib = in_memory_version_store
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


def test_series_empty_string_name(in_memory_version_store):
    """``has_name`` lets an empty-string name survive as "" rather than None"""
    lib = in_memory_version_store
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
def test_series_int_name(in_memory_version_store):
    """int series name should be preserved"""
    lib = in_memory_version_store
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


def test_column_name_none(in_memory_version_store):
    """A None column name is stored as ``__none__0`` with ``is_none``; read restores None."""
    lib = in_memory_version_store
    sym = "test_column_name_none"
    df = pd.DataFrame([[1, 2]], columns=[None, "b"], index=pd.date_range("2025-01-01", periods=1))
    lib.write(sym, df)

    col_names = dict(_read_norm_meta(lib, sym).df.common.col_names)
    assert col_names["__none__0"] == ColumnName(is_none=True)
    assert_frame_equal(df, lib.read(sym).data)


def test_column_name_empty(in_memory_version_store):
    """An empty column name is stored as ``__empty__0`` with ``is_empty``; read restores ""."""
    lib = in_memory_version_store
    sym = "test_column_name_empty"
    df = pd.DataFrame([[1, 2]], columns=["", "b"], index=pd.date_range("2025-01-01", periods=1))
    lib.write(sym, df)

    col_names = dict(_read_norm_meta(lib, sym).df.common.col_names)
    assert col_names["__empty__0"] == ColumnName(is_empty=True)
    assert_frame_equal(df, lib.read(sym).data)


def test_column_name_int(in_memory_version_store):
    """An integer column name sets ``is_int`` with ``original_name``; read casts it back to int."""
    lib = in_memory_version_store
    sym = "test_column_name_int"
    df = pd.DataFrame({5: np.arange(2, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=2))
    lib.write(sym, df)

    col_names = dict(_read_norm_meta(lib, sym).df.common.col_names)
    assert col_names["5"] == ColumnName(is_int=True, original_name="5")
    received = lib.read(sym).data
    assert list(received.columns) == [5]
    assert_frame_equal(df, received)


def test_column_name_original_name_on_clash_with_index(in_memory_version_store):
    """A column clashing with the index name is renamed on disk but ``original_name`` restores it."""
    lib = in_memory_version_store
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


def test_duplicate_column_names(in_memory_version_store):
    """Duplicate columns are disambiguated on disk but share ``original_name``, so they round-trip."""
    lib = in_memory_version_store
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


def test_columns_axis_named(in_memory_version_store):
    """A named columns axis is stored in ``common.columns.name`` and restored on read."""
    lib = in_memory_version_store
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


def test_columns_axis_unnamed_sets_fake_name(in_memory_version_store):
    """An unnamed columns axis sets ``columns.fake_name``; read leaves ``df.columns.name`` None."""
    lib = in_memory_version_store
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


def test_has_synthetic_columns(in_memory_version_store):
    """Unlabelled (RangeIndex) columns set ``has_synthetic_columns``; read rebuilds a RangeIndex."""
    lib = in_memory_version_store
    sym = "test_has_synthetic_columns"
    df = pd.DataFrame([[1, 2], [3, 4]], index=pd.date_range("2025-01-01", periods=2))
    assert isinstance(df.columns, pd.RangeIndex)
    lib.write(sym, df)

    assert _read_norm_meta(lib, sym).df.has_synthetic_columns
    received = lib.read(sym).data
    assert isinstance(received.columns, pd.RangeIndex)
    assert_frame_equal(df, received)


def test_named_columns_are_not_synthetic(in_memory_version_store):
    """Explicit column labels leave ``has_synthetic_columns`` False."""
    lib = in_memory_version_store
    sym = "test_named_columns_are_not_synthetic"
    df = pd.DataFrame({"a": [1], "b": [2]}, index=pd.date_range("2025-01-01", periods=1))
    lib.write(sym, df)

    assert not _read_norm_meta(lib, sym).df.has_synthetic_columns
    assert_frame_equal(df, lib.read(sym).data)


# ---------------------------------------------------------------------------
# Pandas.categories / Pandas.int_categories
# ---------------------------------------------------------------------------


def test_string_categorical_column(in_memory_version_store):
    """A string categorical column stores its categories in ``common.categories``."""
    lib = in_memory_version_store
    sym = "test_string_categorical_column"
    df = pd.DataFrame({"c": pd.Categorical(["a", "b", "a"])}, index=pd.date_range("2025-01-01", periods=3))
    lib.write(sym, df)

    common = _read_norm_meta(lib, sym).df.common
    assert list(common.categories["c"].category) == ["a", "b"]
    assert_frame_equal(df, lib.read(sym).data)


def test_int_categorical_column(in_memory_version_store):
    """An integer categorical column uses ``common.int_categories``."""
    lib = in_memory_version_store
    sym = "test_int_categorical_column"
    df = pd.DataFrame({"c": pd.Categorical([10, 20, 10])}, index=pd.date_range("2025-01-01", periods=3))
    lib.write(sym, df)

    common = _read_norm_meta(lib, sym).df.common
    assert list(common.int_categories["c"].category) == [10, 20]
    assert_frame_equal(df, lib.read(sym).data)


# ---------------------------------------------------------------------------
# PandasMultiIndex (field_count / name / tz / timezone / fake_field_pos / is_int)
# ---------------------------------------------------------------------------


def _multiindex_df_or_series(series_or_df, names, tzs=None, periods=2):
    """Build a DataFrame or Series with a MultiIndex whose first level is a DatetimeIndex."""
    tzs = tzs or [None] * len(names)
    arrays = []
    for level, tz in enumerate(tzs):
        if level == 0 or tz is not None:
            arrays.append(pd.date_range("2025-01-01", periods=periods, tz=tz))
        else:
            arrays.append([f"v{level}_{i}" for i in range(periods)])
    index = pd.MultiIndex.from_arrays(arrays, names=names)
    values = np.arange(periods, dtype=np.int64)
    if series_or_df == "series":
        return pd.Series(values, index=index, name="s")
    return pd.DataFrame({"col": values}, index=index)


def _multi_index_norm_meta(lib, sym):
    """Assert the persisted index is a PandasMultiIndex and return it."""
    common = _read_df_or_series_common_meta(lib, sym)
    assert common.WhichOneof("index_type") == "multi_index"
    return common.multi_index


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_multiindex_field_count(in_memory_version_store, series_or_df):
    """``field_count`` is the number of index levels beyond the first."""
    lib = in_memory_version_store
    sym = "test_multiindex_field_count"
    obj = _multiindex_df_or_series(series_or_df, names=["l0", "l1", "l2"])
    lib.write(sym, obj)

    assert _multi_index_norm_meta(lib, sym).field_count == 2
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_multiindex_first_level_name(in_memory_version_store, series_or_df):
    """The first level name is stored in ``multi_index.name``; read restores all level names."""
    lib = in_memory_version_store
    sym = "test_multiindex_first_level_name"
    obj = _multiindex_df_or_series(series_or_df, names=["l0", "l1"])
    lib.write(sym, obj)

    assert _multi_index_norm_meta(lib, sym).name == "l0"
    assert list(lib.read(sym).data.index.names) == ["l0", "l1"]
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_multiindex_fake_field_pos(in_memory_version_store, series_or_df):
    """Unnamed MultiIndex levels are recorded in ``fake_field_pos``; read restores their names to None."""
    lib = in_memory_version_store
    sym = "test_multiindex_fake_field_pos"
    obj = _multiindex_df_or_series(series_or_df, names=[None, None])
    lib.write(sym, obj)

    assert set(_multi_index_norm_meta(lib, sym).fake_field_pos) == {0, 1}
    assert list(lib.read(sym).data.index.names) == [None, None]
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_multiindex_first_level_tz(in_memory_version_store, series_or_df):
    """A timezone on the first level is stored in ``multi_index.tz``."""
    lib = in_memory_version_store
    sym = "test_multiindex_first_level_tz"
    obj = _multiindex_df_or_series(series_or_df, names=["l0", "l1"], tzs=["America/New_York", None])
    lib.write(sym, obj)

    assert _multi_index_norm_meta(lib, sym).tz == "America/New_York"
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_multiindex_higher_level_tz(in_memory_version_store, series_or_df):
    """Timezones on levels >= 1 go into the per-level ``multi_index.timezone`` map."""
    lib = in_memory_version_store
    sym = "test_multiindex_higher_level_tz"
    obj = _multiindex_df_or_series(series_or_df, names=["l0", "l1"], tzs=[None, "Europe/London"])
    lib.write(sym, obj)

    timezone = dict(_multi_index_norm_meta(lib, sym).timezone)
    assert timezone == {1: "Europe/London"}
    _assert_df_or_series_roundtrip(lib, sym, obj)


@pytest.mark.parametrize("series_or_df", ["df", "series"])
@pytest.mark.xfail(reason="MultiIndex first-level name is stringified on write, so is_int is never set", strict=True)
def test_multiindex_first_level_int_name(in_memory_version_store, series_or_df):
    """An integer first-level name should set ``multi_index.is_int`` and round-trip as an int, like
    a single index. It currently does not: the name is stringified before normalization, so
    ``is_int`` stays False and the name comes back as the string "7"."""
    lib = in_memory_version_store
    sym = "test_multiindex_first_level_int_name"
    obj = _multiindex_df_or_series(series_or_df, names=[7, "l1"])
    lib.write(sym, obj)

    assert _multi_index_norm_meta(lib, sym).is_int
    assert lib.read(sym).data.index.names[0] == 7


# ---------------------------------------------------------------------------
# Legacy / unimplemented fields
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("series_or_df", ["df", "series"])
def test_multiindex_version_field_unused(in_memory_version_store, series_or_df):
    """``PandasMultiIndex.version`` is unused."""
    lib = in_memory_version_store
    sym = "test_multiindex_version_field_unused"
    obj = _multiindex_df_or_series(series_or_df, names=["l0", "l1"])
    lib.write(sym, obj)

    assert _multi_index_norm_meta(lib, sym).version == 0


def test_multiindex_columns_not_supported(in_memory_version_store):
    """``PandasMultiColumn`` is unimplemented: MultiIndex columns raise on write."""
    lib = in_memory_version_store
    sym = "test_multiindex_columns_not_supported"
    df = pd.DataFrame(
        [[1, 2]],
        columns=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")]),
        index=pd.date_range("2025-01-01", periods=1),
    )
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.write(sym, df)


# ---------------------------------------------------------------------------
# Combining a series with a dataframe via append/update/concat
# ---------------------------------------------------------------------------


def _series(values, start="2025-01-01"):
    return pd.Series(np.array(values, dtype=np.int64), index=pd.date_range(start, periods=len(values)), name="col")


def _one_col_frame(values, start="2025-01-01"):
    return pd.DataFrame({"col": np.array(values, dtype=np.int64)}, index=pd.date_range(start, periods=len(values)))


@pytest.mark.parametrize("lib_fixture", ["in_memory_library", "in_memory_library_dynamic"], ids=["static", "dynamic"])
@pytest.mark.parametrize("series_first", [True, False], ids=["series-first", "df-first"])
def test_append_series_and_dataframe_incompatible(request, lib_fixture, series_first):
    """append rejects mixing a Series and a same-named one-column DataFrame (E_INCOMPATIBLE_OBJECTS),
    regardless of schema or order."""
    lib = request.getfixturevalue(lib_fixture)
    first = _series([0, 1]) if series_first else _one_col_frame([0, 1])
    second = _one_col_frame([2, 3], "2025-01-03") if series_first else _series([2, 3], "2025-01-03")
    lib.write("sym", first)
    with pytest.raises(NormalizationException):
        lib.append("sym", second)


@pytest.mark.parametrize("lib_fixture", ["in_memory_library", "in_memory_library_dynamic"], ids=["static", "dynamic"])
@pytest.mark.parametrize("series_first", [True, False], ids=["series-first", "df-first"])
def test_update_series_and_dataframe_incompatible(request, lib_fixture, series_first):
    """update likewise rejects mixing a Series and a DataFrame (E_INCOMPATIBLE_OBJECTS)."""
    lib = request.getfixturevalue(lib_fixture)
    first = _series([0, 1, 2, 3]) if series_first else _one_col_frame([0, 1, 2, 3])
    second = _one_col_frame([9], "2025-01-02") if series_first else _series([9], "2025-01-02")
    lib.write("sym", first)
    with pytest.raises(NormalizationException):
        lib.update("sym", second)


@pytest.mark.parametrize("lib_fixture", ["in_memory_library", "in_memory_library_dynamic"], ids=["static", "dynamic"])
@pytest.mark.parametrize("series_first", [True, False], ids=["series-first", "df-first"])
def test_concat_series_and_dataframe_incompatible(request, lib_fixture, series_first):
    """concat (multi-symbol join) cannot join a Series to a DataFrame. The kind of object is a normalization
    concern, so this is reported as one, the same way append and update report it."""
    lib = request.getfixturevalue(lib_fixture)
    first = _series([0, 1]) if series_first else _one_col_frame([0, 1])
    second = _one_col_frame([2, 3], "2025-01-03") if series_first else _series([2, 3], "2025-01-03")
    lib.write("s0", first)
    lib.write("s1", second)
    with pytest.raises(NormalizationException) as e:
        concat(lib.read_batch(["s0", "s1"], lazy=True)).collect()
    assert "Cannot concat: a Series cannot be combined with a DataFrame" in str(e.value)
