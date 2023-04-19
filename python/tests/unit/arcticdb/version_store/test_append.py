import random
import pandas as pd
import numpy as np
from itertools import chain, product, combinations
import pytest
import sys
from numpy.testing import assert_array_equal

from arcticdb.version_store._common import TimeFrame
from arcticdb.version_store import NativeVersionStore
from arcticdb.util.test import random_integers, assert_frame_equal
from arcticdb.util.hypothesis import InputFactories
from arcticdb_ext.exceptions import InternalException, NormalizationException


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


def test_append_indexed(lmdb_version_store):
    symbol = "test_append_simple"
    idx1 = np.arange(0, 10)
    d1 = {"x": np.arange(10, 20, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    lmdb_version_store.write(symbol, df1)
    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)

    idx2 = np.arange(10, 20)
    d2 = {"x": np.arange(20, 30, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    lmdb_version_store.append(symbol, df2)
    vit = lmdb_version_store.read(symbol)
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


def test_upsert_with_delete(lmdb_version_store):
    symbol = "upsert_with_delete"
    lmdb_version_store.version_store.remove_incomplete(symbol)
    lmdb_version_store.version_store._set_validate_version_map()

    num_rows = 1111
    dtidx = pd.date_range("1970-01-01", periods=num_rows)
    test = pd.DataFrame(
        {"uint8": _random_integers(num_rows, np.uint8), "uint32": _random_integers(num_rows, np.uint32)}, index=dtidx
    )
    chunk_size = 100
    list_df = [test[i : i + chunk_size] for i in range(0, test.shape[0], chunk_size)]

    for idx, df in enumerate(list_df):
        if idx % 3 == 0:
            lmdb_version_store.delete(symbol)

        lmdb_version_store.append(symbol, df, write_if_missing=True)

    first = list_df[len(list_df) - 3]
    second = list_df[len(list_df) - 2]
    third = list_df[len(list_df) - 1]
    print(type(first))
    expected = pd.concat([first, second, third])
    vit = lmdb_version_store.read(symbol)
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
