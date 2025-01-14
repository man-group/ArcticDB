import random
import pandas as pd
import numpy as np
from datetime import datetime
import pytest
import sys
from numpy.testing import assert_array_equal

from pandas import MultiIndex
from arcticdb.version_store import NativeVersionStore
from arcticdb_ext.exceptions import (
    InternalException,
    NormalizationException,
    SortingException,
)
from arcticdb_ext import set_config_int
from arcticdb.util.test import random_integers, assert_frame_equal
from arcticdb.config import set_log_level


def test_append_simple(lmdb_version_store):
    symbol = "test_append_simple"
    df1 = pd.DataFrame({"x": np.arange(1, 10, dtype=np.int64)})
    lmdb_version_store.write(symbol, df1)
    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)

    df2 = pd.DataFrame({"x": np.arange(11, 20, dtype=np.int64)})
    lmdb_version_store.append(symbol, df2)
    vit = lmdb_version_store.read(symbol)
    expected = pd.concat([df1, df2], ignore_index=True)
    assert_frame_equal(vit.data, expected)


def test_append_unicode(lmdb_version_store):
    symbol = "test_append_unicode"
    uc = "\u0420\u043e\u0441\u0441\u0438\u044f"

    df1 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-03")],
        data={"a": ["123", uc]},
    )
    lmdb_version_store.write(symbol, df1)
    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)

    df2 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-04"), pd.Timestamp("2018-01-05")],
        data={"a": ["123", uc]},
    )
    lmdb_version_store.append(symbol, df2)
    vit = lmdb_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)


@pytest.mark.parametrize("empty_types", (True, False))
@pytest.mark.parametrize("dynamic_schema", (True, False))
def test_append_range_index(version_store_factory, empty_types, dynamic_schema):
    lib = version_store_factory(empty_types=empty_types, dynamic_schema=dynamic_schema)
    sym = "test_append_range_index"
    df_0 = pd.DataFrame({"col": [0, 1]}, index=pd.RangeIndex(0, 4, 2))
    lib.write(sym, df_0)

    # Appending another range index following on from what is there should work
    df_1 = pd.DataFrame({"col": [2, 3]}, index=pd.RangeIndex(4, 8, 2))
    lib.append(sym, df_1)
    expected = pd.concat([df_0, df_1])
    received = lib.read(sym).data
    assert_frame_equal(expected, received)

    # Appending a range starting earlier or later, or with a different step size, should fail
    for idx in [
        pd.RangeIndex(6, 10, 2),
        pd.RangeIndex(10, 14, 2),
        pd.RangeIndex(8, 14, 3),
    ]:
        with pytest.raises(NormalizationException):
            lib.append(sym, pd.DataFrame({"col": [4, 5]}, index=idx))


@pytest.mark.parametrize("empty_types", (True, False))
@pytest.mark.parametrize("dynamic_schema", (True, False))
def test_append_range_index_from_zero(version_store_factory, empty_types, dynamic_schema):
    lib = version_store_factory(empty_types=empty_types, dynamic_schema=dynamic_schema)
    sym = "test_append_range_index_from_zero"
    df_0 = pd.DataFrame({"col": [0, 1]}, index=pd.RangeIndex(-6, -2, 2))
    lib.write(sym, df_0)

    with pytest.raises(NormalizationException):
        lib.append(sym, pd.DataFrame({"col": [2, 3]}, index=pd.RangeIndex(0, 4, 2)))


def test_append_indexed(s3_version_store):
    symbol = "test_append_simple"
    idx1 = np.arange(0, 10)
    d1 = {"x": np.arange(10, 20, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    s3_version_store.write(symbol, df1)
    vit = s3_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)

    idx2 = np.arange(10, 20)
    d2 = {"x": np.arange(20, 30, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    s3_version_store.append(symbol, df2)
    vit = s3_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)


def test_append_string_of_different_sizes(lmdb_version_store):
    symbol = "test_append_simple"
    df1 = pd.DataFrame(data={"x": ["cat", "dog"]}, index=np.arange(0, 2))
    lmdb_version_store.write(symbol, df1)
    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)

    df2 = pd.DataFrame(
        data={"x": ["catandsomethingelse", "dogandsomethingevenlonger"]},
        index=np.arange(2, 4),
    )
    lmdb_version_store.append(symbol, df2)
    vit = lmdb_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)


def test_append_dynamic_schema_add_column(lmdb_version_store_dynamic_schema):
    symbol = "symbol"
    lib = lmdb_version_store_dynamic_schema
    df_1 = pd.DataFrame(data={"a": [1.0, 2.0]}, index=pd.date_range("2018-01-01", periods=2))
    df_2 = pd.DataFrame(data={"b": [3.0, 4.0]}, index=pd.date_range("2018-01-03", periods=2))

    lib.write(symbol, df_1)
    lib.append(symbol, df_2)

    expected_df = pd.concat([df_1, df_2])
    result_df = lib.read(symbol).data
    assert_frame_equal(result_df, expected_df)


def test_append_snapshot_delete(lmdb_version_store):
    symbol = "test_append_snapshot_delete"
    if sys.platform == "win32":
        # Keep it smaller on Windows due to restricted LMDB size
        row_count = 1000
    else:
        row_count = 1000000
    idx1 = np.arange(0, row_count)
    d1 = {"x": np.arange(row_count, 2 * row_count, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    lmdb_version_store.write(symbol, df1)
    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)

    lmdb_version_store.snapshot("my_snap")

    idx2 = np.arange(row_count, 2 * row_count)
    d2 = {"x": np.arange(2 * row_count, 3 * row_count, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    lmdb_version_store.append(symbol, df2)
    vit = lmdb_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)

    lmdb_version_store.delete(symbol)
    assert lmdb_version_store.list_versions() == []

    assert_frame_equal(lmdb_version_store.read(symbol, as_of="my_snap").data, df1)


def test_append_out_of_order_throws(lmdb_version_store):
    lib: NativeVersionStore = lmdb_version_store
    lib.write("a", pd.DataFrame({"c": [1, 2, 3]}, index=pd.date_range(0, periods=3)))
    with pytest.raises(Exception, match="1970-01-03"):
        lib.append("a", pd.DataFrame({"c": [4]}, index=pd.date_range(1, periods=1)))


@pytest.mark.parametrize("prune_previous_versions", [True, False])
def test_append_out_of_order_and_sort(lmdb_version_store_ignore_order, prune_previous_versions):
    symbol = "out_of_order"
    lmdb_version_store_ignore_order.version_store.remove_incomplete(symbol)

    num_rows = 1111
    dtidx = pd.date_range("1970-01-01", periods=num_rows)
    test = pd.DataFrame(
        {
            "uint8": random_integers(num_rows, np.uint8),
            "uint32": random_integers(num_rows, np.uint32),
        },
        index=dtidx,
    )
    chunk_size = 100
    list_df = [test[i : i + chunk_size] for i in range(0, test.shape[0], chunk_size)]
    random.shuffle(list_df)

    first = True
    for df in list_df:
        if first:
            lmdb_version_store_ignore_order.write(symbol, df)
            first = False
        else:
            lmdb_version_store_ignore_order.append(symbol, df)

    lmdb_version_store_ignore_order.version_store.sort_index(symbol, True, prune_previous_versions)
    vit = lmdb_version_store_ignore_order.read(symbol)
    assert_frame_equal(vit.data, test)

    versions = [version["version"] for version in lmdb_version_store_ignore_order.list_versions(symbol)]
    if prune_previous_versions:
        assert len(versions) == 1 and versions[0] == len(list_df)
    else:
        assert len(versions) == len(list_df) + 1
        for version in sorted(versions)[:-1]:
            assert_frame_equal(
                lmdb_version_store_ignore_order.read(symbol, as_of=version).data,
                pd.concat(list_df[0 : version + 1]),
            )


@pytest.mark.parametrize("dynamic_schema", (True, False))
@pytest.mark.parametrize("prune_previous_versions", [True, False])
@pytest.mark.parametrize("write_sorted", [True, False])
def test_sort_index(version_store_factory, dynamic_schema, prune_previous_versions, write_sorted):
    lib = version_store_factory(dynamic_schema=dynamic_schema, ignore_sort_order=not write_sorted)
    symbol = "symbol"

    df_1 = pd.DataFrame(data={"col": [1, 2]}, index=[pd.Timestamp(1), pd.Timestamp(2)])
    df_2 = pd.DataFrame(data={"col": [3, 4]}, index=[pd.Timestamp(3), pd.Timestamp(4)])
    if not write_sorted:
        df_1, df_2 = df_2, df_1
    combined_df = pd.concat([df_1, df_2])
    sorted_df = combined_df.sort_index(inplace=False)

    # df should be combined as is
    lib.write(symbol, df_1)
    lib.append(symbol, df_2)
    assert_frame_equal(lib.read(symbol).data, combined_df)

    # sort once
    lib.version_store.sort_index(symbol, dynamic_schema, prune_previous_versions)
    assert_frame_equal(lib.read(symbol).data, sorted_df)

    # sort again to verify it's idempotent
    lib.version_store.sort_index(symbol, dynamic_schema, prune_previous_versions)
    assert_frame_equal(lib.read(symbol).data, sorted_df)


def test_upsert_with_delete(lmdb_version_store_big_map):
    lib = lmdb_version_store_big_map
    symbol = "upsert_with_delete"
    lib.version_store.remove_incomplete(symbol)
    lib.version_store._set_validate_version_map()

    num_rows = 1111
    dtidx = pd.date_range("1970-01-01", periods=num_rows)
    test = pd.DataFrame(
        {
            "uint8": random_integers(num_rows, np.uint8),
            "uint32": random_integers(num_rows, np.uint32),
        },
        index=dtidx,
    )
    chunk_size = 100
    list_df = [test[i : i + chunk_size] for i in range(0, test.shape[0], chunk_size)]

    for idx, df in enumerate(list_df):
        if idx % 3 == 0:
            lib.delete(symbol)

        lib.append(symbol, df, write_if_missing=True)

    first = list_df[len(list_df) - 3]
    second = list_df[len(list_df) - 2]
    third = list_df[len(list_df) - 1]

    expected = pd.concat([first, second, third])
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, expected)


def test_append_numpy_array(lmdb_version_store):
    np1 = random_integers(10, np.uint32)
    lmdb_version_store.write("test_append_numpy_array", np1)
    np2 = random_integers(10, np.uint32)
    lmdb_version_store.append("test_append_numpy_array", np2)
    vit = lmdb_version_store.read("test_append_numpy_array")
    expected = np.concatenate((np1, np2))
    assert_array_equal(vit.data, expected)


def test_append_pickled_symbol(lmdb_version_store):
    symbol = "test_append_pickled_symbol"
    lmdb_version_store.write(symbol, np.arange(100).tolist())
    assert lmdb_version_store.is_symbol_pickled(symbol)
    with pytest.raises(InternalException):
        _ = lmdb_version_store.append(symbol, np.arange(100).tolist())


def test_append_not_sorted_exception(lmdb_version_store):
    symbol = "bad_append"

    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_initial_rows)
    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == True

    lmdb_version_store.write(symbol, df)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "ASCENDING"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_rows), 3)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df2.index.is_monotonic_increasing == False

    with pytest.raises(SortingException):
        lmdb_version_store.append(symbol, df2, validate_index=True)


@pytest.mark.parametrize("validate_index", (True, False))
def test_append_same_index_value(lmdb_version_store_v1, validate_index):
    lib = lmdb_version_store_v1
    sym = "test_append_same_index_value"
    df_0 = pd.DataFrame({"col": [1, 2]}, index=pd.date_range("2024-01-01", periods=2))
    lib.write(sym, df_0)

    df_1 = pd.DataFrame({"col": [3, 4]}, index=pd.date_range(df_0.index[-1], periods=2))
    lib.append(sym, df_1, validate_index=validate_index)
    expected = pd.concat([df_0, df_1])
    received = lib.read(sym).data
    assert_frame_equal(expected, received)
    assert lib.get_info(sym)["sorted"] == "ASCENDING"


def test_append_existing_not_sorted_exception(lmdb_version_store):
    symbol = "bad_append"

    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 3)
    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == False

    lmdb_version_store.write(symbol, df)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_rows)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df2.index.is_monotonic_increasing == True

    with pytest.raises(SortingException):
        lmdb_version_store.append(symbol, df2, validate_index=True)


def test_append_not_sorted_non_validate_index(lmdb_version_store):
    symbol = "bad_append"

    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_initial_rows)
    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == True

    lmdb_version_store.write(symbol, df)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "ASCENDING"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_rows), 3)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df2.index.is_monotonic_increasing == False
    lmdb_version_store.append(symbol, df2)


def test_append_not_sorted_multi_index_exception(lmdb_version_store):
    symbol = "bad_append"

    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx1 = pd.date_range(initial_timestamp, periods=num_initial_rows)
    dtidx2 = np.roll(np.arange(0, num_initial_rows), 3)
    df = pd.DataFrame(
        {"c": np.arange(0, num_initial_rows, dtype=np.int64)},
        index=pd.MultiIndex.from_arrays([dtidx1, dtidx2], names=["datetime", "level"]),
    )
    assert isinstance(df.index, MultiIndex) == True
    assert df.index.is_monotonic_increasing == True

    lmdb_version_store.write(symbol, df)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "ASCENDING"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx1 = np.roll(pd.date_range(initial_timestamp, periods=num_rows), 3)
    dtidx2 = np.arange(0, num_rows)
    df2 = pd.DataFrame(
        {"c": np.arange(0, num_rows, dtype=np.int64)},
        index=pd.MultiIndex.from_arrays([dtidx1, dtidx2], names=["datetime", "level"]),
    )
    assert df2.index.is_monotonic_increasing == False
    assert isinstance(df.index, MultiIndex) == True

    with pytest.raises(SortingException):
        lmdb_version_store.append(symbol, df2, validate_index=True)


def test_append_not_sorted_range_index_non_exception(lmdb_version_store):
    symbol = "bad_append"

    num_initial_rows = 20
    dtidx = pd.RangeIndex(0, num_initial_rows, 1)
    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=dtidx)

    lmdb_version_store.write(symbol, df)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNKNOWN"

    num_rows = 20
    dtidx = pd.RangeIndex(num_initial_rows, num_initial_rows + num_rows, 1)
    dtidx = np.roll(dtidx, 3)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df2.index.is_monotonic_increasing == False
    with pytest.raises(NormalizationException):
        lmdb_version_store.append(symbol, df2)


def test_append_mix_ascending_not_sorted(lmdb_version_store):
    symbol = "bad_append"

    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_initial_rows)
    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == True

    lmdb_version_store.write(symbol, df, validate_index=True)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "ASCENDING"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_rows)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df2.index.is_monotonic_increasing == True
    lmdb_version_store.append(symbol, df2, validate_index=True)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "ASCENDING"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2021-01-01")
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_rows), 3)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df2.index.is_monotonic_increasing == False
    lmdb_version_store.append(symbol, df2)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2022-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_rows)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df2.index.is_monotonic_increasing == True
    lmdb_version_store.append(symbol, df2)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"


def test_append_mix_descending_not_sorted(lmdb_version_store):
    symbol = "bad_append"

    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_initial_rows)
    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=reversed(dtidx))
    assert df.index.is_monotonic_decreasing == True

    lmdb_version_store.write(symbol, df)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "DESCENDING"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_rows)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=reversed(dtidx))
    assert df2.index.is_monotonic_decreasing == True
    lmdb_version_store.append(symbol, df2)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "DESCENDING"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2021-01-01")
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_rows), 3)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df2.index.is_monotonic_decreasing == False
    lmdb_version_store.append(symbol, df2)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2022-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_rows)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=reversed(dtidx))
    assert df2.index.is_monotonic_decreasing == True
    lmdb_version_store.append(symbol, df2)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"


def test_append_mix_ascending_descending(lmdb_version_store):
    symbol = "bad_append"

    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_initial_rows)
    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=reversed(dtidx))
    assert df.index.is_monotonic_decreasing == True

    lmdb_version_store.write(symbol, df)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "DESCENDING"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_rows)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df2.index.is_monotonic_increasing == True
    lmdb_version_store.append(symbol, df2)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"

    num_rows = 20
    initial_timestamp = pd.Timestamp("2022-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_rows)
    df2 = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=reversed(dtidx))
    assert df2.index.is_monotonic_decreasing == True
    lmdb_version_store.append(symbol, df2)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"


@pytest.mark.xfail(reason="Needs to be fixed with issue #496")
def test_append_with_cont_mem_problem(sym, lmdb_version_store_tiny_segment_dynamic):
    set_config_int("SymbolDataCompact.SegmentCount", 1)
    df0 = pd.DataFrame({"0": ["01234567890123456"]}, index=[pd.Timestamp(0)])
    df1 = pd.DataFrame({"0": ["012345678901234567"]}, index=[pd.Timestamp(1)])
    df2 = pd.DataFrame({"0": ["0123456789012345678"]}, index=[pd.Timestamp(2)])
    df3 = pd.DataFrame({"0": ["01234567890123456789"]}, index=[pd.Timestamp(3)])
    df = pd.concat([df0, df1, df2, df3])

    for _ in range(100):
        lib = lmdb_version_store_tiny_segment_dynamic
        lib.write(sym, df0).version
        lib.append(sym, df1).version
        lib.append(sym, df2).version
        lib.append(sym, df3).version
        assert lib.get_info(sym)["sorted"] == "ASCENDING"
        lib.version_store.defragment_symbol_data(sym, None)
        assert lib.get_info(sym)["sorted"] == "ASCENDING"
        res = lib.read(sym).data
        assert_frame_equal(df, res)


def test_append_docs_example(lmdb_version_store):
    # This test is really just the append example from the docs.
    # Other examples are included so that outputs can be easily re-generated.
    lib = lmdb_version_store

    # Write example
    cols = ["COL_%d" % i for i in range(50)]
    df = pd.DataFrame(np.random.randint(0, 50, size=(25, 50)), columns=cols)
    df.index = pd.date_range(datetime(2000, 1, 1, 5), periods=25, freq="H")
    print(df.head(2))
    lib.write("test_frame", df)

    # Read it back
    from_storage_df = lib.read("test_frame").data
    print(from_storage_df.head(2))

    # Slicing and filtering examples
    print(lib.read("test_frame", date_range=(df.index[5], df.index[8])).data)
    _range = (df.index[5], df.index[8])
    _cols = ["COL_30", "COL_31"]
    print(lib.read("test_frame", date_range=_range, columns=_cols).data)
    from arcticdb import QueryBuilder

    q = QueryBuilder()
    q = q[(q["COL_30"] > 30) & (q["COL_31"] < 50)]
    print(lib.read("test_frame", date_range=_range, colymns=_cols, query_builder=q).data)

    # Update example
    random_data = np.random.randint(0, 50, size=(25, 50))
    df2 = pd.DataFrame(random_data, columns=["COL_%d" % i for i in range(50)])
    df2.index = pd.date_range(datetime(2000, 1, 1, 5), periods=25, freq="H")
    df2 = df2.iloc[[0, 2]]
    print(df2)
    lib.update("test_frame", df2)
    print(lib.head("test_frame", 2))

    # Append example
    random_data = np.random.randint(0, 50, size=(5, 50))
    df_append = pd.DataFrame(random_data, columns=["COL_%d" % i for i in range(50)])
    print(df_append)
    df_append.index = pd.date_range(datetime(2000, 1, 2, 7), periods=5, freq="H")

    lib.append("test_frame", df_append)
    print(lib.tail("test_frame", 7).data)
    expected = pd.concat([df2, df.drop(df.index[:3]), df_append])
    assert_frame_equal(lib.read("test_frame").data, expected)

    print(lib.tail("test_frame", 7, as_of=0).data)


def test_read_incomplete_no_warning(s3_store_factory, sym, get_stderr):
    pytest.skip("This test is flaky due to trying to retrieve the log messages")
    lib = s3_store_factory(dynamic_strings=True, incomplete=True)
    lib_tool = lib.library_tool()
    symbol = sym

    write_df = pd.DataFrame({"a": [1, 2, 3]}, index=pd.DatetimeIndex([1, 2, 3]))
    lib_tool.append_incomplete(symbol, write_df)
    # Need to compact so that the APPEND_REF points to a non-existent APPEND_DATA (intentionally)
    lib.compact_incomplete(symbol, True, False, False, True)
    set_log_level("DEBUG")

    try:
        read_df = lib.read(symbol, date_range=(pd.to_datetime(0), pd.to_datetime(10))).data
        assert_frame_equal(read_df, write_df.tz_localize("UTC"))

        err = get_stderr()
        assert err.count("W arcticdb.storage | Failed to find segment for key") == 0
        assert err.count("D arcticdb.storage | Failed to find segment for key") == 1
    finally:
        set_log_level()


@pytest.mark.parametrize("prune_previous_versions", [True, False])
def test_defragment_read_prev_versions(sym, lmdb_version_store, prune_previous_versions):
    start_time, end_time = pd.to_datetime(("1990-1-1", "1995-1-1"))
    cols = ["a", "b", "c", "d"]
    index = pd.date_range(start_time, end_time, freq="D")
    original_df = pd.DataFrame(np.random.randn(len(index), len(cols)), index=index, columns=cols)
    lmdb_version_store.write(sym, original_df)
    expected_dfs = [original_df]

    for idx in range(100):
        update_start = end_time + pd.to_timedelta(idx, "days")
        update_end = update_start + pd.to_timedelta(10, "days")
        update_index = pd.date_range(update_start, update_end, freq="D")
        update_df = pd.DataFrame(
            np.random.randn(len(update_index), len(cols)),
            index=update_index,
            columns=cols,
        )
        lmdb_version_store.update(sym, update_df)
        next_expected_df = expected_dfs[-1].reindex(expected_dfs[-1].index.union(update_df.index))
        next_expected_df.loc[update_df.index] = update_df
        expected_dfs.append(next_expected_df)

    assert_frame_equal(lmdb_version_store.read(sym).data, expected_dfs[-1])
    assert len(lmdb_version_store.list_versions(sym)) == 101
    assert len(expected_dfs) == 101
    for version_id, expected_df in enumerate(expected_dfs):
        assert_frame_equal(lmdb_version_store.read(sym, as_of=version_id).data, expected_df)

    assert lmdb_version_store.is_symbol_fragmented(sym)
    assert lmdb_version_store.get_info(sym)["sorted"] == "ASCENDING"
    versioned_item = lmdb_version_store.defragment_symbol_data(sym, prune_previous_versions=prune_previous_versions)
    assert lmdb_version_store.get_info(sym)["sorted"] == "ASCENDING"
    assert versioned_item.version == 101
    if prune_previous_versions:
        assert len(lmdb_version_store.list_versions(sym)) == 1
        assert lmdb_version_store.list_versions(sym)[0]["version"] == 101
    else:
        assert len(lmdb_version_store.list_versions(sym)) == 102
        assert_frame_equal(lmdb_version_store.read(sym, as_of=0).data, expected_dfs[0])
        for version_id, expected_df in enumerate(expected_dfs):
            assert_frame_equal(lmdb_version_store.read(sym, as_of=version_id).data, expected_df)

    assert_frame_equal(lmdb_version_store.read(sym).data, expected_dfs[-1])


def test_defragment_no_work_to_do(sym, lmdb_version_store):
    df = pd.DataFrame({"a": [1, 2, 3]})
    lmdb_version_store.write(sym, df)
    assert_frame_equal(lmdb_version_store.read(sym).data, df)
    assert list(lmdb_version_store.list_versions(sym))[0]["version"] == 0
    with pytest.raises(InternalException):
        lmdb_version_store.defragment_symbol_data(sym)
