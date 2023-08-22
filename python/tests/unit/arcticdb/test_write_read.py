"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pytest
from arcticdb.supported_types import uint_types, int_types, numeric_types, get_data_type
import numpy as np
from builtins import zip
from numpy.testing import assert_equal

from arcticdb_ext.types import (
    TypeDescriptor,
    StreamDescriptor,
    FieldDescriptor,
    Dimension,
    DataType,
    IndexDescriptor,
    IndexKind,
)
from arcticdb_ext.stream import FixedTickRowBuilder, SegmentHolder, FixedTimestampAggregator, TickReader


def _add_neg(a):
    a = np.array(a)
    return np.concatenate([a, -a])


def parametrize(label, groups):
    vals, ids = list(zip(*groups))
    return pytest.mark.parametrize(label, argvalues=vals, ids=ids)


def params(func, generator, label):
    res = []
    for args in generator:
        res.append((func(*args), label.format(*args)))
    return res


@parametrize(
    "a",
    params(lambda d: np.arange(10, dtype=d), ((d,) for d in numeric_types), "np.arange(10,dtype={})")
    + params(
        lambda d, s: np.arange(10, dtype=d)[::s],
        ((d, s) for d in numeric_types for s in _add_neg([1, 3, 15])),
        "np.arange(10,dtype={})[::{}]",
    )
    + params(
        lambda d: np.arange(10, dtype=d).reshape(2, 5),
        ((d,) for d in numeric_types),
        "np.arange(10,dtype={}).reshape(2,5)",
    )
    + params(
        lambda d, r, c: np.arange(100, dtype=d).reshape(4, 25)[::r, ::c],
        ((d, r, c) for d in numeric_types for r in _add_neg([1, 3, 7]) for c in _add_neg([1, 7, 150])),
        "np.arange(100,dtype={}).reshape(4,25)[::{},::{}]",
    ),
)
def test_simple_round_trip(a):
    fields = [FieldDescriptor(TypeDescriptor(DataType.NANOSECONDS_UTC64, Dimension.Dim0), "time")]
    dim = None
    if len(a.shape) == 1:
        dim = Dimension.Dim1
    elif len(a.shape) == 2:
        dim = Dimension.Dim2

    if dim is None:
        raise Exception("Unknown dimension")
    fields.append(FieldDescriptor(TypeDescriptor(get_data_type(a.dtype.type), dim), "xxx"))
    tsd = StreamDescriptor(123, IndexDescriptor(1, IndexKind.TIMESTAMP), fields)

    sh = SegmentHolder()
    agg = FixedTimestampAggregator(sh, tsd)
    assert agg.row_count == 0

    ts = 123
    with agg.start_row(ts) as rb:
        rb.set_array(1, a)

    assert agg.row_count == 1

    agg.commit()
    assert agg.row_count == 0

    rd = TickReader()
    rd.add_segment(sh.segment)
    assert rd.row_count == 1

    (ts2, a2) = rd.at(0)

    assert ts2 == ts
    assert_equal(a2, a)


def main():
    a = np.arange(10, dtype=np.int8)[::15]
    test_simple_round_trip(a)


if __name__ == "__main__":
    main()
