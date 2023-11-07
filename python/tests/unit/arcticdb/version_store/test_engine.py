"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from itertools import product, chain, combinations

import numpy as np
from numpy import testing as nt
import pandas as pd
import pytest
import random
import sys
from arcticdb.version_store import TimeFrame


def gen_params():
    # colnums
    p = [list(range(2, 5))]
    # rownums
    periods = 6
    p.append(list(range(1, periods + 2)))
    # cols
    p.append(list(chain(*[list(combinations(["a", "b", "c"], c)) for c in range(1, 4)])))
    # tsbounds
    p.append([(j, i) for i in range(periods) for j in range(i)])
    return random.sample(list(product(*p)), 1000)


def gen_params_non_contiguous():
    # colnums
    p = [list(range(2, 5))]
    # rownums
    periods = 10
    p.append(list(range(1, periods + 2)))
    # cols
    p.append(list(chain(*[list(combinations([2, 5, 7], c)) for c in range(1, 4)])))
    # bounds
    p.append([(j, i) for i in range(20, 29) for j in range(i)])
    return random.sample(list(product(*p)), 1000)


@pytest.mark.parametrize("colnum,rownum,cols,tsbounds", gen_params())
def test_partial_write_read(version_store_factory, colnum, rownum, cols, tsbounds):
    tz = "America/New_York"
    version_store = version_store_factory(col_per_group=colnum, row_per_segment=rownum)
    dtidx = pd.date_range("2019-02-06 11:43", periods=6).tz_localize(tz)
    a = np.arange(dtidx.shape[0])
    tf = TimeFrame(dtidx.values, columns_names=["a", "b", "c"], columns_values=[a, a + a, a * 10])
    sid = "XXX"
    version_store.write(sid, tf)

    dtr = (dtidx[tsbounds[0]], dtidx[tsbounds[1]])
    vit = version_store.read(sid, date_range=dtr, columns=list(cols))

    rtf = tf.tsloc[dtr[0] : dtr[1]]
    col_names, col_values = zip(*[(c, v) for c, v in zip(rtf.columns_names, rtf.columns_values) if c in cols])
    rtf = TimeFrame(rtf.times, list(col_names), list(col_values))
    assert rtf == vit.data


@pytest.mark.skipif(sys.platform == "darwin", reason="Test broken on MacOS (issue #923)")
@pytest.mark.parametrize("colnum,rownum,cols,bounds", gen_params_non_contiguous())
def test_partial_write_non_contiguous(version_store_factory, colnum, rownum, cols, bounds):
    version_store = version_store_factory(col_per_group=colnum, row_per_segment=rownum)
    idx = np.arange(0, 10)
    data = {
        "x": np.arange(10, 20, dtype=np.int64),
        "y": np.arange(20, 30, dtype=np.int64),
        "z": np.arange(30, 40, dtype=np.int64),
    }
    df_orig = pd.DataFrame(data=data, index=idx)
    df = df_orig.pivot(index="y", values="x", columns="z")

    sid = "XXX"
    version_store.write(sid, df)
    # TODO add row-range limitation
    vit = version_store.read(sid)
    expected = df
    nt.assert_array_equal(vit.data.values, expected.values)


@pytest.mark.parametrize("colnum,rownum,cols,tsbounds", gen_params())
def test_partial_write_hashed(version_store_factory, colnum, rownum, cols, tsbounds):
    tz = "America/New_York"
    version_store = version_store_factory(
        col_per_group=colnum, row_per_segment=rownum, dynamic_schema=True, bucketize_dynamic=True
    )
    dtidx = pd.date_range("2019-02-06 11:43", periods=6).tz_localize(tz)
    a = np.arange(dtidx.shape[0])
    tf = TimeFrame(dtidx.values, columns_names=["a", "b", "c"], columns_values=[a, a + a, a * 10])
    sid = "XXX"
    version_store.write(sid, tf)

    dtr = (dtidx[tsbounds[0]], dtidx[tsbounds[1]])
    vit = version_store.read(sid, date_range=dtr, columns=list(cols))

    rtf = tf.tsloc[dtr[0] : dtr[1]]
    col_names, col_values = zip(*[(c, v) for c, v in zip(rtf.columns_names, rtf.columns_values) if c in cols])
    rtf = TimeFrame(rtf.times, list(col_names), list(col_values))
    assert rtf == vit.data
