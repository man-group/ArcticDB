"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
from numpy.testing import assert_equal
import platform
import pandas as pd

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
from arcticdb.util.test import assert_frame_equal


def test_vl_string_simple():
    fields = [FieldDescriptor(TypeDescriptor(DataType.NANOSECONDS_UTC64, Dimension.Dim0), "time")]
    dim = Dimension.Dim0
    fields.append(FieldDescriptor(TypeDescriptor(DataType.ASCII_DYNAMIC64, dim), "string"))
    tsd = StreamDescriptor(123, IndexDescriptor(1, IndexKind.TIMESTAMP), fields)
    sh = SegmentHolder()
    agg = FixedTimestampAggregator(sh, tsd)
    assert agg.row_count == 0

    ts1 = 123
    s1 = str("Hello world")
    with agg.start_row(ts1) as rb:
        rb.set_string(1, s1)

    ts2 = 124
    s2 = str("I like egg sandwiches")
    with agg.start_row(ts2) as rb:
        rb.set_string(1, s2)

    ts3 = 124
    s3 = s1
    with agg.start_row(ts3) as rb:
        rb.set_string(1, s3)

    assert agg.row_count == 3
    agg.commit()
    assert agg.row_count == 0
    rd = TickReader()
    rd.add_segment(sh.segment)
    assert rd.row_count == 3

    (rts1, rs1) = rd.at(0)
    assert ts1 == rts1
    assert_equal(s1, rs1)

    (rts2, rs2) = rd.at(1)
    assert ts2 == rts2
    assert_equal(s2, rs2)

    (rts3, rs3) = rd.at(2)
    assert ts3 == rts3
    assert_equal(s3, rs3)
    assert_equal(s1, rs3)

    (ts3, s3) = rd.at(1)


def test_dynamic_string_list():
    fields = [FieldDescriptor(TypeDescriptor(DataType.NANOSECONDS_UTC64, Dimension.Dim0), "time")]
    dim = Dimension.Dim1
    fields.append(FieldDescriptor(TypeDescriptor(DataType.ASCII_DYNAMIC64, dim), "string"))
    tsd = StreamDescriptor(123, IndexDescriptor(1, IndexKind.TIMESTAMP), fields)
    sh = SegmentHolder()
    agg = FixedTimestampAggregator(sh, tsd)
    assert agg.row_count == 0

    ts1 = 123
    s1 = [str("Hello world"), str("Monkey business"), str("Nobody expects the Spanish Inquisition")]
    with agg.start_row(ts1) as rb:
        rb.set_string_list(1, s1)

    ts2 = 124
    s2 = [str("I like egg sandwiches"), str("Hello world"), str("Gravitas shortfall")]
    with agg.start_row(ts2) as rb:
        rb.set_string_list(1, s2)

    ts3 = 124
    s3 = s1
    with agg.start_row(ts3) as rb:
        rb.set_string_list(1, s3)

    assert agg.row_count == 3
    agg.commit()
    assert agg.row_count == 0
    rd = TickReader()
    rd.add_segment(sh.segment)
    assert rd.row_count == 3

    (rts1, rs1) = rd.at(0)
    assert ts1 == rts1
    assert_equal(s1, rs1)

    (rts2, rs2) = rd.at(1)
    assert ts2 == rts2
    assert_equal(s2, rs2)

    (rts3, rs3) = rd.at(2)
    assert ts3 == rts3
    assert_equal(s3, rs3)
    assert_equal(s1, rs3)

    (ts3, s3) = rd.at(1)


def test_fixed_string_simple():
    # TODO these are not the same in python3
    if platform.python_version_tuple()[0] == "2":
        a = np.array(["abc", "xy"])
        fields = [FieldDescriptor(TypeDescriptor(DataType.NANOSECONDS_UTC64, Dimension.Dim0), "time")]
        dim = Dimension.Dim1
        fields.append(FieldDescriptor(TypeDescriptor(DataType.ASCII_FIXED64, dim), "string"))
        tsd = StreamDescriptor(123, IndexDescriptor(1, IndexKind.TIMESTAMP), fields)
        sh = SegmentHolder()
        agg = FixedTimestampAggregator(sh, tsd)
        assert agg.row_count == 0

        ts1 = 123
        s1 = np.array(["Hello world", "Banana", "Wombat"])
        with agg.start_row(ts1) as rb:
            rb.set_string_array(1, s1)

        ts2 = 124
        s2 = np.array(["Magic", "Hoverfly", "Here is a string"])
        with agg.start_row(ts2) as rb:
            rb.set_string_array(1, s2)

        assert agg.row_count == 2
        agg.commit()
        assert agg.row_count == 0
        rd = TickReader()
        rd.add_segment(sh.segment)
        assert rd.row_count == 2

        (rts1, rs1) = rd.at(0)
        assert ts1 == rts1
        assert_equal(s1, rs1)

        (rts2, rs2) = rd.at(1)
        assert ts2 == rts2
        assert_equal(s2, rs2)


def test_write_fixed_coerce_dynamic(lmdb_version_store):
    row = pd.Series(["Aaba", "A", "B", "C", "Baca", "CABA", "dog", "cat"])
    df = pd.DataFrame({"x": row})
    lmdb_version_store.write("strings", df, dynamic_strings=False)
    vit = lmdb_version_store.read("strings", force_string_to_object=True)
    assert_frame_equal(df, vit.data)
