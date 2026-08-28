"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
from numpy.testing import assert_equal
import platform
import pandas as pd
import pytest

from arcticdb.exceptions import ArcticDbNotYetImplemented
from arcticdb.version_store._string_dtype import _ARROW_BACKED_STR_DTYPE_SUPPORTED, _use_pyarrow_strings_in_pandas
from arcticdb_ext.exceptions import UserInputException
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
from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal, arrow_string_read, assert_null_string


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


def test_string_encoding_error_message(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment

    # Broken index
    df = pd.DataFrame({"working_column": ["hello", "bonjour", "nihao"]}, index=["hello", "bonjour", 5])
    with pytest.raises(UserInputException) as e:
        lib.write("sym_broken_index", df, dynamic_strings=True)
    exception_message = str(e.value)
    assert all(string in exception_message for string in ["index", "row 2", "int"])

    # Broken non-index column
    df = pd.DataFrame({"broken_column": ["hello", "bonjour", np.arange(5)]}, index=np.arange(3))
    with pytest.raises(UserInputException) as e:
        lib.write("sym_broken_non_index_column", df, dynamic_strings=True)
    exception_message = str(e.value)
    assert all(string in exception_message for string in ["broken_column", "row 2", "ndarray"])

    # Append
    df = pd.DataFrame({"broken_column": ["hello", "bonjour", "nihao"]}, index=np.arange(3))
    lib.write("sym_append", df, dynamic_strings=True)
    df = pd.DataFrame({"broken_column": ["hello", "bonjour", 5.5]}, index=np.arange(3))
    with pytest.raises(UserInputException) as e:
        lib.append("sym_append", df, dynamic_strings=True)
    exception_message = str(e.value)
    assert all(string in exception_message for string in ["broken_column", "row 2", "float"])

    # Update
    df = pd.DataFrame(
        {"broken_column": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]},
        index=pd.date_range("2000-01-01", periods=10),
    )
    lib.write("sym_update", df, dynamic_strings=True)
    df = pd.DataFrame({"broken_column": ["hello", "bonjour", 5.5]}, index=pd.date_range("2000-01-03", periods=3))
    with pytest.raises(UserInputException) as e:
        lib.update("sym_update", df, dynamic_strings=True)
    exception_message = str(e.value)
    assert all(string in exception_message for string in ["broken_column", "row 2", "float"])


def test_write_dynamic_simple(lmdb_version_store_v2):
    row = pd.Series(["Aaba", "A", "B", "C", "Baca", "CABA", "dog", "cat", "here is a very long one"])
    df = pd.DataFrame({"x": row})
    lmdb_version_store_v2.write("strings", df, dynamic_strings=True)
    vit = lmdb_version_store_v2.read("strings")
    assert_frame_equal(df, vit.data)


@pytest.mark.parametrize("filter_kind", ["date_range", "row_range"])
@pytest.mark.parametrize("expect_empty", [False, True])
def test_read_filtered_string_column(
    lmdb_version_store_v2, read_string_dtype, filter_kind, expect_empty, skip_consolidation
):
    lib = lmdb_version_store_v2
    if skip_consolidation:
        lib._normalizer.df.set_skip_df_consolidation()
    index = pd.date_range("2026-01-01", periods=10)
    values = [f"str_{i}" for i in range(10)]
    lib.write("strings", pd.DataFrame({"x": values}, index=index), dynamic_strings=True)
    if expect_empty:
        read_kwargs = (
            {"date_range": (pd.Timestamp("2027-01-01"), pd.Timestamp("2027-01-02"))}
            if filter_kind == "date_range"
            else {"row_range": (100, 110)}
        )
        expected_slice = slice(0, 0)
    else:
        read_kwargs = {"date_range": (index[3], index[7])} if filter_kind == "date_range" else {"row_range": (3, 8)}
        expected_slice = slice(3, 8)
    with arrow_string_read(read_string_dtype):
        received = lib.read("strings", **read_kwargs).data
    assert list(received.index) == list(index[expected_slice])
    assert list(received["x"]) == values[expected_slice]
    assert (str(received["x"].dtype) == "str") == read_string_dtype


def test_read_row_range_default_index_string_first_column(lmdb_version_store_v2, read_string_dtype, skip_consolidation):
    lib = lmdb_version_store_v2
    if skip_consolidation:
        lib._normalizer.df.set_skip_df_consolidation()
    values = [f"str_{i}" for i in range(10)]
    lib.write("strings", pd.DataFrame({"x": values}), dynamic_strings=True)
    with arrow_string_read(read_string_dtype):
        received = lib.read("strings", row_range=(3, 8)).data
    assert len(received) == 5
    assert list(received.index) == list(range(5))
    assert list(received["x"]) == values[3:8]
    assert (str(received["x"].dtype) == "str") == read_string_dtype


def test_read_string_column_dtype(lmdb_version_store_v2, read_string_dtype, skip_consolidation):
    if skip_consolidation:
        lmdb_version_store_v2._normalizer.df.set_skip_df_consolidation()
    values = ["Aaba", "A", "B", "C", "Baca", "CABA", "dog", "cat", "here is a very long one"]
    # Multiple adjacent string columns so that consolidation has something to merge into one block, and a
    # trailing numeric column that must not be pulled into it.
    data = {"x": values, "y": [v.upper() for v in values], "z": [v[::-1] for v in values], "n": range(len(values))}
    lmdb_version_store_v2.write("strings", pd.DataFrame(data), dynamic_strings=True)
    with arrow_string_read(read_string_dtype):
        expected = pd.DataFrame(data)
        vit = lmdb_version_store_v2.read("strings")
    assert_frame_equal(expected, vit.data)
    for col in ("x", "y", "z"):
        assert (str(vit.data[col].dtype) == "str") == read_string_dtype


def test_none_vs_nan_null_distinction(lmdb_version_store_v2, read_string_dtype):
    # object dtype preserves the None-vs-NaN distinction on read; the arrow-backed str dtype does not:
    # its only null sentinel is np.nan, so None and NaN both come back as np.nan and are indistinguishable.
    lib = lmdb_version_store_v2
    # positions:            str    None    np.nan       float nan     str
    values = ["x", None, np.nan, float("nan"), "y"]
    # Force object input so None is stored distinctly from NaN (future.infer_string would collapse it
    # at construction); the distinction we assert below is about the read dtype.
    lib.write("s", pd.DataFrame({"c": pd.Series(values, dtype=object)}), dynamic_strings=True)
    with arrow_string_read(read_string_dtype):
        col = lib.read("s").data["c"]
    assert list(col.isna()) == [False, True, True, True, False]
    assert col.iloc[0] == "x" and col.iloc[4] == "y"
    assert (str(col.dtype) == "str") == read_string_dtype
    # The written None comes back as None under object but as np.nan under the str dtype, whose only null
    # sentinel is np.nan. A written NaN is NaN either way.
    assert_null_string(col.iloc[1], read_string_dtype)
    assert np.isnan(col.iloc[2]) and np.isnan(col.iloc[3])


def test_isnull_filter_treats_none_and_nan_alike(lmdb_version_store_v2, read_string_dtype):
    # Regardless of read dtype, an isnull filter matches both None- and NaN-written nulls.
    lib = lmdb_version_store_v2
    lib.write("s", pd.DataFrame({"c": pd.Series(["x", None, np.nan, "y"], dtype=object)}), dynamic_strings=True)
    q = QueryBuilder()
    q = q[q["c"].isnull()]
    with arrow_string_read(read_string_dtype):
        col = lib.read("s", query_builder=q).data["c"]
    assert len(col) == 2
    assert list(col.isna()) == [True, True]
    assert (str(col.dtype) == "str") == read_string_dtype


def test_isnull_filter_string_column_dtype_independent(lmdb_version_store_v2, read_string_dtype):
    # Unlike test_isnull_filter_treats_none_and_nan_alike, the input dtype is left to pandas, so under the
    # infer_string CI variant this writes a str-dtype column. That makes it the only isnull-filter test
    # exercising the arrow-backed write path, and it pins that the filter result is the same either way.
    lib = lmdb_version_store_v2
    lib.write("s", pd.DataFrame({"x": ["a", None, np.nan, "b"]}), dynamic_strings=True)
    q = QueryBuilder()
    q = q[q["x"].isnull()]
    with arrow_string_read(read_string_dtype):
        col = lib.read("s", query_builder=q).data["x"]
    assert len(col) == 2
    assert list(col.isna()) == [True, True]
    assert (str(col.dtype) == "str") == read_string_dtype


def test_read_string_index(lmdb_version_store_v2, read_string_dtype):
    lib = lmdb_version_store_v2
    df = pd.DataFrame({"v": [1, 2, 3]}, index=pd.Index(["a", "b", "c"], name="k"))
    lib.write("s", df, dynamic_strings=True)
    with arrow_string_read(read_string_dtype):
        r = lib.read("s").data
    assert list(r.index) == ["a", "b", "c"]
    assert list(r["v"]) == [1, 2, 3]
    assert (str(r.index.dtype) == "str") == read_string_dtype


def test_read_dynamic_schema_backfilled_string_column_truncation(version_store_factory, read_string_dtype):
    lib = version_store_factory(dynamic_schema=True, segment_row_size=100, dynamic_strings=True)
    idx = pd.date_range("2026-01-01", periods=200, freq="D")
    lib.write("sym", pd.DataFrame({"a": np.arange(100)}, index=idx[:100]))
    lib.append(
        "sym",
        pd.DataFrame({"a": np.arange(100, 200), "s": [f"s{i}" for i in range(100, 200)]}, index=idx[100:]),
    )
    # Trims inside the first (0-99) slice, where `s` is absent and backfilled.
    with arrow_string_read(read_string_dtype):
        r = lib.read("sym", date_range=(idx[50], idx[149])).data
    assert len(r) == 100
    assert list(r.index) == list(idx[50:150])
    assert list(r["a"]) == list(range(50, 150))
    assert r["s"].iloc[:50].isna().all()
    assert list(r["s"].iloc[50:]) == [f"s{i}" for i in range(100, 150)]
    assert (str(r["s"].dtype) == "str") == read_string_dtype


def test_write_arrow_backed_string_index(lmdb_version_store_v2, read_string_dtype):
    # Explicitly constructs the arrow-backed str index on write (rather than relying on the future.infer_string
    # CI leg to produce one incidentally), to pin the write/append path for this index dtype directly.
    if not _ARROW_BACKED_STR_DTYPE_SUPPORTED:
        pytest.skip("pandas too old for the arrow-backed str dtype (StringDtype na_value, added in 2.3)")
    lib = lmdb_version_store_v2
    arrow_str_dtype = pd.StringDtype(storage="pyarrow", na_value=np.nan)
    idx1 = pd.Index(pd.array([f"k{i}" for i in range(5)], dtype=arrow_str_dtype), name="k")
    lib.write("s", pd.DataFrame({"v": range(5)}, index=idx1), dynamic_strings=True)
    idx2 = pd.Index(pd.array([f"k{i}" for i in range(5, 10)], dtype=arrow_str_dtype), name="k")
    lib.append("s", pd.DataFrame({"v": range(5, 10)}, index=idx2), dynamic_strings=True)

    with arrow_string_read(read_string_dtype):
        full = lib.read("s").data
        sliced = lib.read("s", row_range=(3, 8)).data

    assert list(full.index) == [f"k{i}" for i in range(10)]
    assert list(full["v"]) == list(range(10))
    assert (str(full.index.dtype) == "str") == read_string_dtype

    assert list(sliced.index) == [f"k{i}" for i in range(3, 8)]
    assert list(sliced["v"]) == list(range(3, 8))
    assert (str(sliced.index.dtype) == "str") == read_string_dtype


@pytest.mark.parametrize(
    "storage, na_value, match",
    [
        ("python", "pd.NA", "pd.NA"),
        ("pyarrow", "pd.NA", "pd.NA"),
        ("python", "nan", "pyarrow-backed storage"),
    ],
)
def test_write_unsupported_string_dtype_rejected(lmdb_version_store_v2, storage, na_value, match):
    # Only StringDtype(storage="pyarrow", na_value=np.nan) is supported. pd.NA is rejected because its
    # three-valued comparison semantics cannot be reproduced on read or by the filter engine; python storage
    # has no arrow buffer to hand over and is not supported yet.
    if not _ARROW_BACKED_STR_DTYPE_SUPPORTED:
        pytest.skip("pandas too old for the arrow-backed str dtype (StringDtype na_value, added in 2.3)")
    lib = lmdb_version_store_v2
    dtype = pd.StringDtype(storage=storage, na_value=pd.NA if na_value == "pd.NA" else np.nan)
    values = pd.array(["a", None, "b"], dtype=dtype)
    with pytest.raises(ArcticDbNotYetImplemented, match=match):
        lib.write("s", pd.DataFrame({"c": values}), dynamic_strings=True)
    with pytest.raises(ArcticDbNotYetImplemented, match=match):
        lib.write("s", pd.DataFrame({"v": range(3)}, index=pd.Index(values, name="k")), dynamic_strings=True)


def test_none_column_name_read_dtype(lmdb_version_store_v2, read_string_dtype):
    # A column named None reads back matching pandas: under infer_string the columns axis is the str
    # dtype and None shows as nan; otherwise it is an object Index holding None. Build the input as
    # object so the None header is stored (infer_string would collapse it to nan at construction).
    lib = lmdb_version_store_v2
    lib.write("s", pd.DataFrame([[1, 2]], columns=pd.Index(["a", None], dtype=object)))
    with arrow_string_read(read_string_dtype):
        cols = lib.read("s").data.columns
    assert cols[0] == "a"
    if read_string_dtype:
        assert str(cols.dtype) == "str"
        assert np.isnan(cols[1])
    else:
        assert cols.dtype == object
        assert list(cols) == ["a", None]


def test_int_and_none_column_names_preserved(lmdb_version_store_v2, read_string_dtype):
    # A column axis mixing an int and None must stay object with the exact labels (not coerced to
    # float64 [1.0, nan]) even under infer_string, since pandas cannot represent it as str.
    lib = lmdb_version_store_v2
    lib.write("s", pd.DataFrame([[1, 2]], columns=pd.Index([100, None], dtype=object)))
    with arrow_string_read(read_string_dtype):
        cols = lib.read("s").data.columns
    assert cols.dtype == object
    assert list(cols) == [100, None]


class ArbitraryClass:
    def __init__(self):
        self.x = "blah"


class StringInheritingClass(str):
    def __init__(self):
        super().__init__()


class BytesInheritingClass(bytes):
    def __init__(self):
        super().__init__()


@pytest.mark.parametrize(
    "first_value", [b"blah", "blah", ArbitraryClass(), StringInheritingClass(), BytesInheritingClass()]
)
@pytest.mark.parametrize(
    "second_value", [b"blah", "blah", ArbitraryClass(), StringInheritingClass(), BytesInheritingClass()]
)
def test_mixed_types_errors(lmdb_version_store_v1, first_value, second_value):
    lib = lmdb_version_store_v1
    sym = "test_mixed_types_errors"
    df = pd.DataFrame({"col": [first_value, second_value]})
    if first_value == second_value:
        pytest.skip()
    if _use_pyarrow_strings_in_pandas() and {type(first_value), type(second_value)} == {StringInheritingClass, str}:
        pytest.skip(
            "future.infer_string coerces a str-subclass/plain-str mix to a clean str column, so no error is raised"
        )
    # The first value is used to determine the dtype, so we get a different exception when the first value is of a
    # non-normalizable type
    exception_type = (
        ArcticDbNotYetImplemented
        if isinstance(first_value, (ArbitraryClass, StringInheritingClass, BytesInheritingClass))
        else UserInputException
    )
    with pytest.raises(exception_type):
        lib.write(sym, df)
