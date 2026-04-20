"""
Tests for the Polars set_sorted() flag on the index column during ArcticDB reads.

Verifies that when data is read back with output_format="polars", the index column
has the SORTED_ASC flag set (based on SortedValue from the C++ StreamDescriptor).
"""

import numpy as np
import pandas as pd
import pytest
import polars as pl
from types import SimpleNamespace

from arcticdb_ext.version_store import SortedValue
from arcticdb.version_store._store import NativeVersionStore


def test_sorted_flag_on_datetime_index(lmdb_library):
    """Unnamed DatetimeIndex -> column '__index__' should have SORTED_ASC."""
    lib = lmdb_library
    df = pd.DataFrame(
        {"val": np.arange(10)},
        index=pd.date_range("2024-01-01", periods=10, freq="h"),
    )
    lib.write("sym", df)
    result = lib.read("sym", output_format="polars").data

    assert isinstance(result, pl.DataFrame)
    assert result["__index__"].flags["SORTED_ASC"] is True
    assert result["__index__"].flags["SORTED_DESC"] is False


def test_sorted_flag_on_named_datetime_index(lmdb_library):
    """Named DatetimeIndex -> column with that name should have SORTED_ASC."""
    lib = lmdb_library
    df = pd.DataFrame(
        {"val": np.arange(10)},
        index=pd.date_range("2024-01-01", periods=10, freq="h", name="timestamp"),
    )
    lib.write("sym", df)
    result = lib.read("sym", output_format="polars").data

    assert isinstance(result, pl.DataFrame)
    assert "timestamp" in result.columns
    assert result["timestamp"].flags["SORTED_ASC"] is True


def test_no_sorted_flag_on_range_index(lmdb_library):
    """RangeIndex is not physically stored — no column should carry a sorted flag."""
    lib = lmdb_library
    df = pd.DataFrame({"val": np.arange(10)})
    lib.write("sym", df)
    result = lib.read("sym", output_format="polars").data

    assert isinstance(result, pl.DataFrame)
    for col in result.columns:
        assert result[col].flags["SORTED_ASC"] is False, f"Column {col} should not have SORTED_ASC"


def test_sorted_flag_not_set_for_pandas_output(lmdb_library):
    """Reading as pandas (default) should not crash and returns a regular DataFrame."""
    lib = lmdb_library
    df = pd.DataFrame(
        {"val": np.arange(10)},
        index=pd.date_range("2024-01-01", periods=10, freq="h"),
    )
    lib.write("sym", df)
    result = lib.read("sym").data

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 10


@pytest.mark.storage
def test_sorted_flag_with_stage_finalize(arctic_library):
    """Stage shuffled data with sort_on_index=True, finalize, read as Polars -> SORTED_ASC."""
    lib = arctic_library
    symbol = "staged"

    df = pd.DataFrame(
        {"val": np.arange(50)},
        index=pd.date_range("2024-01-01", periods=50, freq="h"),
    )
    shuffled = df.sample(frac=1, random_state=42)

    lib.stage(symbol, shuffled, validate_index=False, sort_on_index=True)
    lib.finalize_staged_data(symbol)

    result = lib.read(symbol, output_format="polars").data

    assert isinstance(result, pl.DataFrame)
    assert result["__index__"].flags["SORTED_ASC"] is True


def test_value_columns_not_sorted(lmdb_library):
    """Only the index column should get the sorted flag, not value columns."""
    lib = lmdb_library
    # Both index and value columns are actually sorted in the data
    df = pd.DataFrame(
        {"sorted_val": np.arange(10), "another": np.arange(10)},
        index=pd.date_range("2024-01-01", periods=10, freq="h"),
    )
    lib.write("sym", df)
    result = lib.read("sym", output_format="polars").data

    assert isinstance(result, pl.DataFrame)
    assert result["__index__"].flags["SORTED_ASC"] is True
    assert result["sorted_val"].flags["SORTED_ASC"] is False
    assert result["another"].flags["SORTED_ASC"] is False


def _make_mock_read_result(sort_order, index_name="__index__", fake_name=True, is_physically_stored=True):
    """Build a minimal mock ReadResult with the given sort_order and index normalization metadata."""
    index = SimpleNamespace(name=index_name, fake_name=fake_name, is_physically_stored=is_physically_stored)
    common = SimpleNamespace(_index_type="index", index=index)
    common.WhichOneof = lambda field: "index" if field == "index_type" else None
    norm = SimpleNamespace(df=SimpleNamespace(common=common))
    norm.WhichOneof = lambda field: "df" if field == "input_type" else None
    return SimpleNamespace(sort_order=sort_order, norm=norm)


def test_descending_sort_order_sets_sorted_desc():
    """When sort_order is DESCENDING the Polars column should have SORTED_DESC."""
    data = pl.DataFrame({"__index__": pl.date_range(pd.Timestamp("2024-01-01"), periods=5, interval="1h", eager=True)})
    read_result = _make_mock_read_result(SortedValue.DESCENDING)

    result = NativeVersionStore._apply_polars_sorted_flag_to_index(data, read_result)

    assert result["__index__"].flags["SORTED_DESC"] is True
    assert result["__index__"].flags["SORTED_ASC"] is False


def test_unknown_sort_order_does_not_set_sorted_flag():
    """When sort_order is UNKNOWN no sorted flag should be applied even with a physical index."""
    data = pl.DataFrame({"__index__": pl.date_range(pd.Timestamp("2024-01-01"), periods=5, interval="1h", eager=True)})
    read_result = _make_mock_read_result(SortedValue.UNKNOWN)

    result = NativeVersionStore._apply_polars_sorted_flag_to_index(data, read_result)

    assert result["__index__"].flags["SORTED_ASC"] is False
    assert result["__index__"].flags["SORTED_DESC"] is False
