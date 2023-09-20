import random
import pandas as pd
import numpy as np
from datetime import datetime
from itertools import chain, product, combinations
import pytest
import sys
from numpy.testing import assert_array_equal
from hypothesis import given, assume, settings, strategies as st

from pandas import MultiIndex
from arcticdb.version_store._common import TimeFrame
from arcticdb.version_store import NativeVersionStore
from arcticdb.util.test import random_integers, assert_frame_equal
from arcticdb.util.hypothesis import InputFactories, use_of_function_scoped_fixtures_in_hypothesis_checked
from arcticdb_ext.exceptions import InternalException, NormalizationException, SortingException
from arcticdb_ext import set_config_int


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

    df2 = pd.DataFrame(data={"x": ["catandsomethingelse", "dogandsomethingevenlonger"]}, index=np.arange(2, 4))
    lmdb_version_store.append(symbol, df2)
    vit = lmdb_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)


def gen_params_append():
    # colnums
    p = [list(range(2, 5))]
    # periods
    periods = 6
    p.append([periods])
    # rownums
    p.append([1, 4, periods + 2])
    # cols
    p.append(list(chain(*[list(combinations(["a", "b", "c"], c)) for c in range(1, 4, 2)])))
    # tsbounds
    p.append([(j, i) for i in [1, periods - 1] for j in range(i)])
    # append_point
    p.append([k for k in range(1, periods - 1)])
    return random.sample(list(product(*p)), 500)


def gen_params_append_single():
    # colnums
    p = [[2]]
    # periods
    periods = 6
    p.append([periods])
    # rownums
    p.append([1])
    # cols
    p.append([["a"]])
    # tsbounds
    p.append([[0, 5]])
    # append_point
    p.append([1])
    return list(product(*p))


@pytest.mark.parametrize("colnum,periods,rownum,cols,tsbounds,append_point", gen_params_append())
def test_append_partial_read(version_store_factory, colnum, periods, rownum, cols, tsbounds, append_point):
    tz = "America/New_York"
    version_store = version_store_factory(col_per_group=colnum, row_per_segment=rownum)
    dtidx = pd.date_range("2019-02-06 11:43", periods=6).tz_localize(tz)
    a = np.arange(dtidx.shape[0])
    tf = TimeFrame(dtidx.values, columns_names=["a", "b", "c"], columns_values=[a, a + a, a * 10])
    c1 = dtidx[append_point]
    c2 = dtidx[append_point + 1]
    tf1 = tf.tsloc[:c1]
    sid = "XXX"
    version_store.write(sid, tf1)
    tf2 = tf.tsloc[c2:]
    version_store.append(sid, tf2)

    dtr = (dtidx[tsbounds[0]], dtidx[tsbounds[1]])
    vit = version_store.read(sid, date_range=dtr, columns=list(cols))
    rtf = tf.tsloc[dtr[0] : dtr[1]]
    col_names, col_values = zip(*[(c, v) for c, v in zip(rtf.columns_names, rtf.columns_values) if c in cols])
    rtf = TimeFrame(rtf.times, list(col_names), list(col_values))
    assert rtf == vit.data


@pytest.mark.parametrize("colnum,periods,rownum,cols,tsbounds,append_point", gen_params_append())
def test_incomplete_append_partial_read(version_store_factory, colnum, periods, rownum, cols, tsbounds, append_point):
    tz = "America/New_York"
    version_store = version_store_factory(col_per_group=colnum, row_per_segment=rownum)
    dtidx = pd.date_range("2019-02-06 11:43", periods=6).tz_localize(tz)
    a = np.arange(dtidx.shape[0])
    tf = TimeFrame(dtidx.values, columns_names=["a", "b", "c"], columns_values=[a, a + a, a * 10])
    c1 = dtidx[append_point]
    c2 = dtidx[append_point + 1]
    tf1 = tf.tsloc[:c1]
    sid = "XXX"
    version_store.write(sid, tf1)
    tf2 = tf.tsloc[c2:]
    version_store.append(sid, tf2, incomplete=True)

    dtr = (dtidx[tsbounds[0]], dtidx[tsbounds[1]])
    vit = version_store.read(sid, date_range=dtr, columns=list(cols), incomplete=True)
    rtf = tf.tsloc[dtr[0] : dtr[1]]
    col_names, col_values = zip(*[(c, v) for c, v in zip(rtf.columns_names, rtf.columns_values) if c in cols])
    rtf = TimeFrame(rtf.times, list(col_names), list(col_values))
    assert rtf == vit.data


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


def _random_integers(size, dtype):
    # We do not generate integers outside the int64 range
    platform_int_info = np.iinfo("int_")
    iinfo = np.iinfo(dtype)
    return np.random.randint(
        max(iinfo.min, platform_int_info.min), min(iinfo.max, platform_int_info.max), size=size
    ).astype(dtype)


def test_append_out_of_order_throws(lmdb_version_store):
    lib: NativeVersionStore = lmdb_version_store
    lib.write("a", pd.DataFrame({"c": [1, 2, 3]}, index=pd.date_range(0, periods=3)))
    with pytest.raises(Exception, match="1970-01-03"):
        lib.append("a", pd.DataFrame({"c": [4]}, index=pd.date_range(1, periods=1)))


def test_append_out_of_order_and_sort(lmdb_version_store_ignore_order):
    symbol = "out_of_order"
    lmdb_version_store_ignore_order.version_store.remove_incomplete(symbol)

    num_rows = 1111
    dtidx = pd.date_range("1970-01-01", periods=num_rows)
    test = pd.DataFrame(
        {"uint8": _random_integers(num_rows, np.uint8), "uint32": _random_integers(num_rows, np.uint32)}, index=dtidx
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

    lmdb_version_store_ignore_order.version_store.sort_index(symbol, True)
    vit = lmdb_version_store_ignore_order.read(symbol)
    assert_frame_equal(vit.data, test)


def test_upsert_with_delete(lmdb_version_store_big_map):
    lib = lmdb_version_store_big_map
    symbol = "upsert_with_delete"
    lib.version_store.remove_incomplete(symbol)
    lib.version_store._set_validate_version_map()

    num_rows = 1111
    dtidx = pd.date_range("1970-01-01", periods=num_rows)
    test = pd.DataFrame(
        {"uint8": _random_integers(num_rows, np.uint8), "uint32": _random_integers(num_rows, np.uint32)}, index=dtidx
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


# test_hypothesis_version_store.py covers the appends that are allowed.
# Only need to check the forbidden ones have useful error messages:
@pytest.mark.parametrize(
    "initial, append, match",
    [
        # (InputFactories.DF_RC_NON_RANGE, InputFactories.DF_DTI, "TODO(AN-722)"),
        (InputFactories.DF_RC, InputFactories.ND_ARRAY_1D, "(Pandas|ndarray)"),
        (InputFactories.DF_RC, InputFactories.DF_MULTI_RC, "index type incompatible"),
        (InputFactories.DF_RC, InputFactories.DF_RC_NON_RANGE, "range.*index which is incompatible"),
        (InputFactories.DF_RC, InputFactories.DF_RC_STEP, "different.*step"),
    ],
)
@pytest.mark.parametrize("swap", ["swap", ""])
def test_(initial: InputFactories, append: InputFactories, match, swap, lmdb_version_store: NativeVersionStore):
    lib = lmdb_version_store
    if swap:
        initial, append = append, initial
    init_data, next_start = initial.make(1, 3)
    lib.write("s", init_data)

    to_append, _ = append.make(abs(next_start), 1)
    with pytest.raises(NormalizationException):
        lib.append("s", to_append, match=match)


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
    assert info["sorted"] == "ASCENDING"

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


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None, max_examples=10)
@given(
    col_per_append_df=st.integers(2, 100),
    col_name_set=st.integers(1, 10000),
    num_rows_per_test_cycle=st.lists(st.lists(st.integers(1, 20), min_size=1, max_size=10), min_size=1, max_size=10),
    column_group_size=st.integers(2, 100),
    segment_row_size=st.integers(2, 100),
    dynamic_schema=st.booleans(),
    dynamic_strings=st.booleans(),
    df_in_str=st.booleans(),
)
def test_append_with_defragmentation(
    sym,
    col_per_append_df,
    col_name_set,
    num_rows_per_test_cycle,
    get_wide_df,
    column_group_size,
    segment_row_size,
    dynamic_schema,
    dynamic_strings,
    df_in_str,
    version_store_factory,
):
    def get_wide_and_long_df(start_idx, end_idx, col_per_append_df, col_name_set, df_in_str):
        df = pd.DataFrame()
        for idx in range(start_idx, end_idx):
            df = pd.concat([df, get_wide_df(idx, col_per_append_df, col_name_set)])
        if col_per_append_df == col_name_set:  # manually sort them for static schema, for newer version of panda
            df = df.reindex(sorted(list(df.columns)), axis=1)
        df = df.astype(str if df_in_str else np.float64)
        return df

    def get_no_of_segments_after_defragmentation(df, merged_segment_row_size):
        new_segment_row_size = no_of_segments = 0
        for start_row, end_row in pd.Series(df.end_row.values, index=df.start_row).to_dict().items():
            no_of_segments = no_of_segments + 1 if new_segment_row_size == 0 else no_of_segments
            new_segment_row_size += end_row - start_row
            if new_segment_row_size >= merged_segment_row_size:
                new_segment_row_size = 0
        return no_of_segments

    def get_no_of_column_merged_segments(df):
        return len(pd.Series(df.end_row.values, index=df.start_row).to_dict().items())

    def run_test(
        lib,
        col_per_append_df,
        col_name_set,
        merged_segment_row_size,
        df_in_str,
        before_compact,
        index_offset,
        num_of_rows,
    ):
        for num_of_row in num_of_rows:
            start_index = index_offset
            index_offset += num_of_row
            end_index = index_offset
            df = get_wide_and_long_df(start_index, end_index, col_per_append_df, col_name_set, df_in_str)
            before_compact = pd.concat([before_compact, df])
            if start_index == 0:
                lib.write(sym, df)
            else:
                lib.append(sym, df)
            segment_details = lib.read_index(sym)
            assert lib.is_symbol_fragmented(sym, None) is (
                get_no_of_segments_after_defragmentation(segment_details, merged_segment_row_size)
                != get_no_of_column_merged_segments(segment_details)
            )
        if get_no_of_segments_after_defragmentation(
            segment_details, merged_segment_row_size
        ) != get_no_of_column_merged_segments(segment_details):
            seg_details_before_compaction = lib.read_index(sym)
            lib.defragment_symbol_data(sym, None)
            res = lib.read(sym).data
            res = res.reindex(sorted(list(res.columns)), axis=1)
            res = res.replace("", 0.0)
            res = res.fillna(0.0)
            before_compact = before_compact.reindex(sorted(list(before_compact.columns)), axis=1)
            before_compact = before_compact.fillna(0.0)

            seg_details = lib.read_index(sym)

            assert_frame_equal(before_compact, res)

            assert len(seg_details) == get_no_of_segments_after_defragmentation(
                seg_details_before_compaction, merged_segment_row_size
            )
            indexs = (
                seg_details["end_index"].astype(str).str.rsplit(" ", n=2).agg(" ".join).reset_index()
            )  # start_index and end_index got merged into one column
            assert np.array_equal(indexs.iloc[1:, 0].astype(str).values, indexs.iloc[:-1, 1].astype(str).values)
        else:
            with pytest.raises(InternalException):
                lib.defragment_symbol_data(sym, None)
        return before_compact, index_offset

    assume(col_per_append_df <= col_name_set)
    assume(
        num_of_row % 2 != 0 for num_of_rows in num_rows_per_test_cycle for num_of_row in num_of_rows
    )  # Make sure at least one successful compaction run per cycle

    set_config_int("SymbolDataCompact.SegmentCount", 1)
    before_compact = pd.DataFrame()
    index_offset = 0
    lib = version_store_factory(
        column_group_size=column_group_size,
        segment_row_size=segment_row_size,
        dynamic_schema=dynamic_schema,
        dynamic_strings=dynamic_strings,
        reuse_name=True,
    )
    for num_of_rows in num_rows_per_test_cycle:
        before_compact, index_offset = run_test(
            lib,
            col_per_append_df,
            col_name_set if dynamic_schema else col_per_append_df,
            segment_row_size,
            df_in_str,
            before_compact,
            index_offset,
            num_of_rows,
        )


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
        lib.version_store.defragment_symbol_data(sym, None)
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
