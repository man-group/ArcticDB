"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from hypothesis import given, settings
import hypothesis.strategies as st
import numpy as np
import pandas as pd
from polars.testing import assert_frame_equal as assert_frame_equal_pl
import pyarrow as pa
import pytest

from arcticdb_ext.exceptions import InternalException
import arcticdb.toolbox.query_stats as qs
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
)
from arcticdb.util.test import assert_frame_equal, query_stats_operation_count, random_strings_of_length
from tests.util.mark import MACOS, WINDOWS
from tests.util.naughty_strings import read_big_list_of_naughty_strings

pytestmark = pytest.mark.pipeline


def generic_compact_data_inline_test(lib, sym, df, **append_kwargs):
    qs.reset_stats()  # Clear any leftover stats from a previous failed run
    vit_before_compaction = lib.read(sym, output_format="PANDAS" if isinstance(df, pd.DataFrame) else "POLARS")
    oracle_sym = sym + "_oracle"
    lib.write(oracle_sym, vit_before_compaction.data)
    lib.append(oracle_sym, df, compact_data_inline=False, **append_kwargs)
    # Use Polars so that sparse data checking is proper
    expected = lib.read(oracle_sym, output_format="POLARS").data
    pre_compaction_index = lib.read_index(sym)
    pre_compaction_data_keys = len(pre_compaction_index)

    with qs.query_stats():
        lib.append(sym, df, compact_data_inline=True, **append_kwargs)
        stats = qs.get_query_stats()
    qs.reset_stats()
    rows_per_segment = lib.lib_cfg().lib_desc.version.write_options.segment_row_size
    if rows_per_segment == 0:
        rows_per_segment = 100_000
    vit_after_compaction = lib.read(sym, output_format="POLARS")
    received = vit_after_compaction.data
    assert_frame_equal_pl(expected, received)
    post_compaction_index = lib.read_index(sym)
    row_counts = post_compaction_index["end_row"] - post_compaction_index["start_row"]
    # Definitions taken from CompactDataClause constructor
    min_rows_per_segment = max((2 * rows_per_segment) // 3, 1)
    max_rows_per_segment = max((4 * rows_per_segment) // 3, rows_per_segment + 1)
    # There might be fewer rows in total than min_rows_per_segment
    min_rows_per_segment = min(min_rows_per_segment, len(expected))
    assert row_counts.min() >= min_rows_per_segment
    assert row_counts.max() <= max_rows_per_segment

    post_compaction_data_keys = len(post_compaction_index)
    new_data_keys = len(post_compaction_index[post_compaction_index["version_id"] > vit_before_compaction.version])
    compacted_data_keys = pre_compaction_data_keys - (post_compaction_data_keys - new_data_keys)
    assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == compacted_data_keys
    assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == new_data_keys
    # Doing a compaction would now have no impact
    compact_data_info = lib.compact_data_explain_plan(sym)
    assert not compact_data_info.will_do_work


@pytest.mark.parametrize("index", [None, "ts"])
def test_basic(in_memory_store_factory, clear_query_stats, index):
    lib = in_memory_store_factory()
    sym = "test_basic"
    df_0 = pd.DataFrame(
        {"col": np.arange(20)}, index=None if index is None else pd.date_range("2026-01-01", periods=20)
    )
    lib.write(sym, df_0)
    df_1 = pd.DataFrame(
        {"col": np.arange(20, 30)}, index=None if index is None else pd.date_range("2026-01-21", periods=10)
    )
    generic_compact_data_inline_test(lib, sym, df_1)


def test_existing_zero_rows(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_existing_zero_rows"
    # Zero-rowed data gets stored with a datetime index
    df_0 = pd.DataFrame({"col": np.arange(0)})
    lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": np.arange(15)}, index=pd.date_range("2026-01-21", periods=15))
    generic_compact_data_inline_test(lib, sym, df_1)


@pytest.mark.parametrize("write_if_missing", [True, False])
@pytest.mark.parametrize("compact_data_inline", [True, False])
def test_write_if_missing(in_memory_store_factory, write_if_missing, compact_data_inline):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_write_if_missing"
    df = pd.DataFrame({"col": np.arange(15)})
    if write_if_missing:
        lib.append(sym, df, compact_data_inline=compact_data_inline, write_if_missing=write_if_missing)
        assert_frame_equal(df, lib.read(sym).data)
        index = lib.read_index(sym)
        row_counts = (index["end_row"] - index["start_row"]).to_list()
        # See comment in LocalVersionedEngine::append_internal as to why this isn't [8, 7] when compact_data_inline is
        # True
        assert row_counts == [10, 5]
    else:
        with pytest.raises(InternalException):
            lib.append(sym, df, compact_data_inline=compact_data_inline, write_if_missing=write_if_missing)


def test_metadata(in_memory_store_factory):
    lib = in_memory_store_factory()
    sym = "test_metadata"
    lib.write(sym, pd.DataFrame({"col": [0]}), metadata="0")
    lib.append(sym, pd.DataFrame({"col": [1]}), metadata="1", compact_data_inline=True)
    vit = lib.read(sym)
    assert vit.metadata == "1"
    assert_frame_equal(vit.data, pd.DataFrame({"col": [0, 1]}))
    assert len(lib.read_index(sym)) == 1


@pytest.mark.parametrize("index", [None, "ts"])
def test_compact_whole_symbol(in_memory_store_factory, clear_query_stats, index):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_compact_whole_symbol"
    df = pd.DataFrame({"col": np.arange(20)}, index=None if index is None else pd.date_range("2026-01-01", periods=20))
    lib.write(sym, df[:5])
    lib.append(sym, df[5:10])
    lib.append(sym, df[10:15])
    generic_compact_data_inline_test(lib, sym, df[15:])


@pytest.mark.parametrize("index", [None, "ts"])
def test_compact_leftover_slices(in_memory_store_factory, clear_query_stats, index):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_compact_leftover_slices"
    df = pd.DataFrame({"col": np.arange(20)}, index=None if index is None else pd.date_range("2026-01-01", periods=20))
    lib.write(sym, df[:5])
    generic_compact_data_inline_test(lib, sym, df[5:])


def test_existing_data_compacted(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_existing_data_compacted"
    df = pd.DataFrame({"col": np.arange(20)})
    lib.write(sym, df[:10])
    generic_compact_data_inline_test(lib, sym, df[10:])


@pytest.mark.parametrize("total_rows", [25, 30, 35])
def test_tail_of_existing_data_already_compacted(in_memory_store_factory, clear_query_stats, total_rows):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_tail_of_existing_data_already_compacted"
    df = pd.DataFrame({"col": np.arange(total_rows)})
    lib.write(sym, df[:5])
    lib.append(sym, df[5:10])
    lib.append(sym, df[10:20])
    assert len(lib.read_index(sym)) == 3
    generic_compact_data_inline_test(lib, sym, df[20:])


@pytest.mark.parametrize("index", [None, "ts"])
@pytest.mark.parametrize("segment_row_size", [100_000, 10, 5, 2])
def test_basic_dynamic_schema(in_memory_store_factory, clear_query_stats, index, segment_row_size):
    lib = in_memory_store_factory(segment_row_size=segment_row_size, dynamic_schema=True)
    sym = "test_basic_dynamic_schema"
    df_0 = pd.DataFrame(
        {
            "col_0": np.arange(20, dtype=np.float64),
            "col_1": np.arange(20, 40, dtype=np.float64),
            "col_2": np.arange(40, 60, dtype=np.float64),
        },
        index=None if index is None else pd.date_range("2026-01-01", periods=20),
    )
    lib.write(sym, df_0)
    df_1 = pd.DataFrame(
        {
            "col_3": np.arange(100, 110, dtype=np.float64),
            "col_2": np.arange(60, 70, dtype=np.float64),
            "col_1": np.arange(40, 50, dtype=np.float64),
        },
        index=None if index is None else pd.date_range("2026-01-21", periods=10),
    )
    generic_compact_data_inline_test(lib, sym, df_1)


@pytest.mark.parametrize("index", [None, "ts"])
@pytest.mark.parametrize("segment_row_size", [100_000, 10, 5])
def test_column_slicing(in_memory_store_factory, clear_query_stats, index, segment_row_size):
    lib = in_memory_store_factory(segment_row_size=segment_row_size, column_group_size=2)
    sym = "test_column_slicing"
    df_0 = pd.DataFrame(
        {f"col_{idx}": np.arange(20) for idx in range(5)},
        index=None if index is None else pd.date_range("2026-01-01", periods=20),
    )
    lib.write(sym, df_0)
    df_1 = pd.DataFrame(
        {f"col_{idx}": np.arange(20, 30) for idx in range(5)},
        index=None if index is None else pd.date_range("2026-01-21", periods=10),
    )
    generic_compact_data_inline_test(lib, sym, df_1)


@pytest.mark.parametrize("names", [None, ["ts", None], [None, "level 2"], ["ts", "level 2"]])
def test_multiindex(in_memory_store_factory, clear_query_stats, names):
    lib = in_memory_store_factory(segment_row_size=10, dynamic_strings=True)
    sym = "test_multiindex"
    num_rows = 20
    df = pd.DataFrame(
        {"col": np.arange(num_rows)},
        index=pd.MultiIndex.from_product(
            [pd.date_range("2026-01-01", periods=num_rows // 2), ["GOOG", "AAPL"]], names=names
        ),
    )
    lib.write(sym, df[:5])
    generic_compact_data_inline_test(lib, sym, df[5:])


def test_string_none_nan_handling(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory(dynamic_strings=True)
    sym = "test_string_none_nan_handling"
    df = pd.DataFrame({"col": ["hello", np.nan, np.nan, None, None, None, np.nan, np.nan, None, None]})
    lib.write(sym, df[:5], coerce_columns={"col": object})
    generic_compact_data_inline_test(lib, sym, df[5:], coerce_columns={"col": object})


@pytest.mark.parametrize("dynamic_strings_first", [True, False])
def test_fixed_width_and_dynamic_strings(in_memory_store_factory, clear_query_stats, dynamic_strings_first):
    lib = in_memory_store_factory()
    sym = "test_fixed_width_and_dynamic_strings"
    # Include two segments with different widths of strings
    df = pd.DataFrame({"col": ["a", "bb", "ccc", "dddd", "eeeee", "f", "gg", "hhhhhhhhhhhhhh", "i"]})
    lib.write(sym, df[:3], dynamic_strings=dynamic_strings_first)
    lib.append(sym, df[3:5], dynamic_strings=dynamic_strings_first)
    lib.append(sym, df[5:7], dynamic_strings=not dynamic_strings_first)
    generic_compact_data_inline_test(lib, sym, df[7:], dynamic_strings=not dynamic_strings_first)


@pytest.mark.parametrize("dynamic_strings_first", [True, False])
def test_blns(in_memory_store_factory, clear_query_stats, dynamic_strings_first):
    lib = in_memory_store_factory()
    sym = "test_blns"
    df = pd.DataFrame({"col": read_big_list_of_naughty_strings()})
    lib.write(sym, df[: len(df) // 2], dynamic_strings=dynamic_strings_first)
    generic_compact_data_inline_test(lib, sym, df[len(df) // 2 :], dynamic_strings=not dynamic_strings_first)


def test_append_empty_frame_no_work_to_do(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_append_empty_frame_no_work_to_do"
    lib.write(sym, pd.DataFrame({"col": np.arange(10)}), metadata="0")
    # Schema checks happen after empty input frame checks, so we don't need the same column set
    # This metadata behaviour isn't ideal, but we should be consistent whether compact_data_inline is True or False
    # See Monday issue 18103677039
    lib.append(sym, pd.DataFrame(), metadata="1")
    vit = lib.read(sym)
    assert vit.version == 0
    assert vit.metadata == "0"
    lib.append(sym, pd.DataFrame(), metadata="1", compact_data_inline=True)
    vit = lib.read(sym)
    assert vit.version == 0
    assert vit.metadata == "0"


def test_append_empty_frame_compacts_existing_data(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_append_empty_frame_compacts_existing_data"
    lib.write(sym, pd.DataFrame({"col": np.arange(5)}))
    lib.append(sym, pd.DataFrame({"col": np.arange(5, 10)}))
    # Schema checks happen after empty input frame checks, so we don't need the same column set
    lib.append(sym, pd.DataFrame())
    assert lib.read(sym).version == 1
    lib.append(sym, pd.DataFrame(), compact_data_inline=True)
    assert lib.read(sym).version == 2
    assert len(lib.read_index(sym)) == 1


@pytest.mark.parametrize("rows_to_append", [5, 10, 15, 20])
def test_fortran_ordered_data(in_memory_store_factory, clear_query_stats, rows_to_append):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_fortran_ordered_data"
    cols = ["col_0", "col_1"]
    df_0 = pd.DataFrame(np.random.randint(0, 100, size=(5, 2)), columns=cols)
    lib.write(sym, df_0)
    df_1 = pd.DataFrame(np.random.randint(0, 100, size=(rows_to_append, 2)), columns=cols)
    generic_compact_data_inline_test(lib, sym, df_1)


@pytest.mark.parametrize("index", [None, "ts"])
def test_column_filtered_read(in_memory_store_factory, clear_query_stats, index):
    lib = in_memory_store_factory(column_group_size=2, segment_row_size=10)
    sym = "test_column_filtered_read"
    num_rows = 20
    df = pd.DataFrame(
        {
            "col_a": np.arange(num_rows),
            "col_b": np.arange(num_rows, 2 * num_rows),
            "col_c": np.arange(2 * num_rows, 3 * num_rows),
        },
        index=None if index is None else pd.date_range("2026-01-01", periods=num_rows),
    )
    lib.write(sym, df[:5])
    for i in range(1, 4):
        generic_compact_data_inline_test(lib, sym, df[i * 5 : (i + 1) * 5])
    expected_col_a = df[["col_a"]]
    expected_col_bc = df[["col_b", "col_c"]]
    assert_frame_equal(expected_col_a, lib.read(sym, columns=["col_a"]).data)
    assert_frame_equal(expected_col_bc, lib.read(sym, columns=["col_b", "col_c"]).data)


@pytest.mark.parametrize("rows_per_segment", [3, 7, 10])
def test_date_range_read(in_memory_store_factory, clear_query_stats, rows_per_segment):
    lib = in_memory_store_factory(segment_row_size=rows_per_segment, dynamic_strings=True)
    sym = "test_date_range_read"
    num_rows = 100
    index = pd.date_range("2026-01-01", periods=num_rows)
    df = pd.DataFrame(
        {"ints": np.arange(num_rows), "strings": 20 * ["hello", None, "gutentag", np.nan, "konichiwa"]}, index=index
    )
    lib.write(sym, df[:5])
    for i in range(1, 20):
        generic_compact_data_inline_test(lib, sym, df[i * 5 : (i + 1) * 5])
    mid = index[num_rows // 2]
    expected_first_half = df[:mid]
    expected_second_half = df[mid:]
    assert_frame_equal(expected_first_half, lib.read(sym, date_range=(index[0], mid)).data)
    assert_frame_equal(expected_second_half, lib.read(sym, date_range=(mid, index[-1])).data)


def test_read_previous_version(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_read_previous_version"
    df = pd.DataFrame({"col": np.arange(10)})
    lib.write(sym, df[:5])
    generic_compact_data_inline_test(lib, sym, df[5:])
    assert_frame_equal(df[:5], lib.read(sym, as_of=0).data)
    assert_frame_equal(df, lib.read(sym, as_of=1).data)
    assert_frame_equal(df, lib.read(sym).data)


def test_schema_mismatch_static(in_memory_store_factory):
    lib = in_memory_store_factory()
    sym = "test_schema_mismatch_static"
    df_0 = pd.DataFrame({"col_0": [0]})
    lib.write(sym, df_0)
    # Different column sets
    df_1 = pd.DataFrame({"col_1": [0]})
    with pytest.raises(Exception) as e_without_arg:
        lib.append(sym, df_1)
    with pytest.raises(Exception) as e_with_arg:
        lib.append(sym, df_1, compact_data_inline=True)
    assert e_with_arg.type == e_without_arg.type
    assert e_with_arg.typename == e_without_arg.typename
    assert e_with_arg.value.args[0] == e_without_arg.value.args[0]
    # Different column type
    df_1 = pd.DataFrame({"col_0": ["hello"]})
    with pytest.raises(Exception) as e_without_arg:
        lib.append(sym, df_1)
    with pytest.raises(Exception) as e_with_arg:
        lib.append(sym, df_1, compact_data_inline=True)
    assert e_with_arg.type == e_without_arg.type
    assert e_with_arg.typename == e_without_arg.typename
    assert e_with_arg.value.args[0] == e_without_arg.value.args[0]


def test_schema_mismatch_dynamic(in_memory_store_factory):
    lib = in_memory_store_factory(dynamic_schema=True)
    sym = "test_schema_mismatch_dynamic"
    df_0 = pd.DataFrame({"col_0": [0]})
    lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col_0": ["hello"]})
    with pytest.raises(Exception) as e_without_arg:
        lib.append(sym, df_1)
    with pytest.raises(Exception) as e_with_arg:
        lib.append(sym, df_1, compact_data_inline=True)
    assert e_with_arg.type == e_without_arg.type
    assert e_with_arg.typename == e_without_arg.typename
    assert e_with_arg.value.args[0] == e_without_arg.value.args[0]


# We are more interested in the slicing than the data, so the parameters are for:
# - number of rows and columns
# - library slicing settings
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    # Making these parameters too large results in all the time being spent in numpy generating random numbers
    num_rows=st.integers(1, 1_000),
    num_cols=st.integers(1, 20),
    # The more interesting cases are when num_rows > rows_per_segment
    rows_per_segment=st.integers(10, 100),
    cols_per_segment=st.integers(1, 20),
    # Shrinks towards False, which is the simpler case
    sparse=st.booleans(),
)
@pytest.mark.skipif(
    WINDOWS or MACOS,
    reason="""
        On macOS/Windows the low timestamp resolution can cause duplicate keys when
        successive operations land within the same clock tick.
        TODO: Fix the underlying issue and remove this workaround (monday ticket ref 11777175142)
""",
)
def test_hypothesis_static_schema(
    in_memory_store_factory, clear_query_stats, num_rows, num_cols, rows_per_segment, cols_per_segment, sparse
):
    rng = np.random.default_rng(42)
    lib = in_memory_store_factory(
        column_group_size=cols_per_segment, segment_row_size=rows_per_segment, dynamic_strings=True, name="_unique_"
    )
    lib._set_allow_arrow_input()
    sym = "test_hypothesis_static_schema"
    supported_types = [
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.float32,
        np.float64,
        bool,
        str,
        np.datetime64,
    ]
    col_types = rng.choice(supported_types, num_cols)
    data = {}
    string_values = random_strings_of_length(10, 5, True)
    for idx in range(num_cols):
        col_name = f"col_{idx}"
        col_type = col_types[idx]
        if np.issubdtype(col_type, np.integer):
            arr = rng.integers(np.iinfo(col_type).min, np.iinfo(col_type).max, num_rows, col_type, True)
        elif np.issubdtype(col_type, np.floating):
            arr = rng.random(num_rows, col_type)
        elif col_type == bool:
            arr = rng.random(num_rows) > 0.5
        elif col_type == str:
            arr = rng.choice(string_values, num_rows)
        else:
            # datetime
            arr = pd.date_range("2026-01-01", freq="s", periods=num_rows).values
            rng.shuffle(arr)
        if sparse:
            null_mask = rng.random(num_rows) < 0.5
            arr = pa.array(arr, mask=null_mask)
        else:
            arr = pa.array(arr)
        data[col_name] = arr
    table = pa.table(data)
    # Append random numbers of rows between 1 and 2 * rows_per_segment
    remaining_rows = num_rows
    first_iteration = True
    while remaining_rows > 0:
        rows_to_take = rng.integers(1, 2 * rows_per_segment)
        if first_iteration:
            lib.write(sym, table.slice(length=rows_to_take))
            first_iteration = False
        else:
            generic_compact_data_inline_test(lib, sym, table.slice(length=rows_to_take))
        # lib.append(sym, table.slice(length=rows_to_take), compact_data_inline=True)
        table = table.slice(offset=rows_to_take)
        remaining_rows -= rows_to_take


# We are more interested in the slicing than the data, so the parameters are for:
# - number of rows
# - library slicing settings
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    # Making these parameters too large results in all the time being spent in numpy generating random numbers
    num_rows=st.integers(1, 1_000),
    # The more interesting cases are when num_rows > rows_per_segment
    rows_per_segment=st.integers(10, 100),
    # Shrinks towards False, which is the simpler case
    sparse=st.booleans(),
)
@pytest.mark.skipif(
    WINDOWS or MACOS,
    reason="""
        On macOS/Windows the low timestamp resolution can cause duplicate keys when
        successive operations land within the same clock tick.
        TODO: Fix the underlying issue and remove this workaround (monday ticket ref 11777175142)
""",
)
def test_hypothesis_dynamic_schema(in_memory_store_factory, clear_query_stats, num_rows, rows_per_segment, sparse):
    rng = np.random.default_rng(42)
    lib = in_memory_store_factory(dynamic_schema=True, dynamic_strings=True, name="_unique_")
    lib._set_allow_arrow_input()
    sym = "test_hypothesis_dynamic_schema"
    unsigned_int_types = [np.uint8, np.uint16, np.uint32, np.uint64]
    signed_int_types = [np.int8, np.int16, np.int32, np.int64]
    float_types = [np.float32, np.float64]
    # Two string columns as stringpool dedup make them more complicated
    cols = {
        "unsigned_ints": unsigned_int_types,
        "signed_ints": signed_int_types,
        "floats": float_types,
        # Exclude uint64 as it cannot be combined with signed int types at write time
        "numeric": unsigned_int_types[:3] + signed_int_types + float_types,
        "bools": [bool],
        "timestamps": [np.datetime64],
        "strings_1": [str],
        "strings_2": [str],
    }
    all_col_names = list(cols.keys())
    string_values = random_strings_of_length(10, 5, True)
    # Append random numbers of rows between 1 and 2 * rows_per_segment
    remaining_rows = num_rows
    first_iteration = True
    while remaining_rows > 0:
        rows_to_take = rng.integers(1, 2 * rows_per_segment)
        # Pick a subset of columns
        num_columns = rng.integers(1, len(cols) + 1)
        col_names = rng.choice(all_col_names, num_columns, False)
        data = {}
        for col_name in col_names:
            col_type = rng.choice(cols[col_name])
            if np.issubdtype(col_type, np.integer):
                arr = rng.integers(np.iinfo(col_type).min, np.iinfo(col_type).max, rows_to_take, col_type, True)
            elif np.issubdtype(col_type, np.floating):
                arr = rng.random(rows_to_take, col_type)
            elif col_type == bool:
                arr = rng.random(rows_to_take) > 0.5
            elif col_type == str:
                arr = rng.choice(string_values, rows_to_take)
            else:
                # datetime
                arr = pd.date_range("2026-01-01", freq="s", periods=rows_to_take).values
                rng.shuffle(arr)
            if sparse:
                null_mask = rng.random(rows_to_take) < 0.5
                arr = pa.array(arr, mask=null_mask)
            else:
                arr = pa.array(arr)
            data[col_name] = arr
        table = pa.table(data)
        if first_iteration:
            lib.write(sym, table)
            first_iteration = False
        else:
            generic_compact_data_inline_test(lib, sym, table)
        remaining_rows -= rows_to_take
