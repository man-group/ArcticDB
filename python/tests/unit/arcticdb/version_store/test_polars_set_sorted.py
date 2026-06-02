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


def test_unsorted_arrow_write_does_not_get_polars_sorted_flag(lmdb_version_store_arrow):
    """Polars writes with a non-monotonic index column must not carry SORTED_ASC on read."""
    lib = lmdb_version_store_arrow
    df = pl.DataFrame(
        {
            "ts": [
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
            ],
            "val": [1, 2, 3],
        },
        schema={"ts": pl.Datetime("ns"), "val": pl.Int64},
    )
    lib.write("sym", df, index_column=True)
    result = lib.read("sym", output_format="polars").data

    assert result["ts"].flags["SORTED_ASC"] is False
    assert result["ts"].flags["SORTED_DESC"] is False


def test_value_columns_not_sorted(lmdb_library):
    """Only the index column should get the sorted flag, not value columns."""
    lib = lmdb_library
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


def _make_norm_meta(input_type="df", index_type="index", is_physically_stored=True):
    """Build a minimal mock normalization metadata object."""
    index = SimpleNamespace(is_physically_stored=is_physically_stored)
    common = SimpleNamespace(index=index)
    common.WhichOneof = lambda field: index_type if field == "index_type" else None
    norm = SimpleNamespace(df=SimpleNamespace(common=common))
    norm.WhichOneof = lambda field: input_type if field == "input_type" else None
    return norm


def test_descending_sort_order_sets_sorted_desc():
    """When sort_order is DESCENDING the Polars column should have SORTED_DESC."""
    data = pl.DataFrame({"__index__": pd.date_range("2024-01-01", periods=5, freq="h")})

    result = NativeVersionStore._apply_polars_sorted_flag_to_index(data, SortedValue.DESCENDING, _make_norm_meta())

    assert result["__index__"].flags["SORTED_DESC"] is True
    assert result["__index__"].flags["SORTED_ASC"] is False


def test_unknown_sort_order_does_not_set_sorted_flag():
    """UNKNOWN means sortedness was not verified (e.g. Arrow writes), so no flag should be applied."""
    data = pl.DataFrame({"__index__": pd.date_range("2024-01-01", periods=5, freq="h")})

    result = NativeVersionStore._apply_polars_sorted_flag_to_index(data, SortedValue.UNKNOWN, _make_norm_meta())

    assert result["__index__"].flags["SORTED_ASC"] is False
    assert result["__index__"].flags["SORTED_DESC"] is False


def test_unsorted_sort_order_does_not_set_sorted_flag():
    """When sort_order is UNSORTED no sorted flag should be applied."""
    data = pl.DataFrame({"__index__": pd.date_range("2024-01-01", periods=5, freq="h")})

    result = NativeVersionStore._apply_polars_sorted_flag_to_index(data, SortedValue.UNSORTED, _make_norm_meta())

    assert result["__index__"].flags["SORTED_ASC"] is False
    assert result["__index__"].flags["SORTED_DESC"] is False


def test_multi_index_first_level_gets_sorted_flag():
    """For MultiIndex symbols, the first index column is marked sorted when the symbol is sorted."""
    data = pl.DataFrame(
        {
            "__index__": pd.date_range("2024-01-01", periods=5, freq="h"),
            "level_1": np.arange(5),
        }
    )

    result = NativeVersionStore._apply_polars_sorted_flag_to_index(
        data, SortedValue.ASCENDING, _make_norm_meta(index_type="multi_index")
    )

    assert result["__index__"].flags["SORTED_ASC"] is True
    assert result["level_1"].flags["SORTED_ASC"] is False
