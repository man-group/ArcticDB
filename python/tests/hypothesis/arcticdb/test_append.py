import random
import pandas as pd
import numpy as np
import pytest
from arcticdb.version_store import NativeVersionStore
from arcticdb_ext.exceptions import InternalException, NormalizationException, ArcticException as ArcticNativeException
from arcticdb_ext.version_store import StreamDescriptorMismatch
from arcticdb_ext import set_config_int
from hypothesis import given, assume, settings, strategies as st
from itertools import chain, product, combinations
from tests.util.mark import SLOW_TESTS_MARK
from arcticdb.version_store._common import TimeFrame

from arcticdb.util.test import assert_frame_equal, random_seed_context
from arcticdb.util.hypothesis import (
    InputFactories,
    use_of_function_scoped_fixtures_in_hypothesis_checked,
)


def gen_params_append():
    # Note: We use sorted() and deterministic selection instead of random.sample() to ensure
    # consistent test collection order across pytest-xdist workers. pytest-xdist requires that
    # all workers collect tests in the same order, and using unordered containers (like sets)
    # or random sampling can cause "Different tests were collected between workers" errors.
    # See: https://pytest-xdist.readthedocs.io/en/stable/known-limitations.html
    with random_seed_context():
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
        # Sort the product list first to ensure deterministic ordering across workers
        all_params = sorted(list(product(*p)))
        # Use deterministic selection: take first 10 after sorting
        result = sorted(all_params[:10])
    return result


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
    lib_tool = version_store.library_tool()
    dtidx = pd.date_range("2019-02-06 11:43", periods=6).tz_localize(tz)
    a = np.arange(dtidx.shape[0])
    tf = TimeFrame(dtidx.values, columns_names=["a", "b", "c"], columns_values=[a, a + a, a * 10])
    c1 = dtidx[append_point]
    c2 = dtidx[append_point + 1]
    tf1 = tf.tsloc[:c1]
    sid = "XXX"
    version_store.write(sid, tf1)
    tf2 = tf.tsloc[c2:]
    lib_tool.append_incomplete(sid, tf2)

    dtr = (dtidx[tsbounds[0]], dtidx[tsbounds[1]])
    vit = version_store.read(sid, date_range=dtr, columns=list(cols), incomplete=True)
    rtf = tf.tsloc[dtr[0] : dtr[1]]
    col_names, col_values = zip(*[(c, v) for c, v in zip(rtf.columns_names, rtf.columns_values) if c in cols])
    rtf = TimeFrame(rtf.times, list(col_names), list(col_values))
    assert rtf == vit.data


# test_hypothesis_version_store.py covers the appends that are allowed.
# Only need to check the forbidden ones have useful error messages:
@pytest.mark.parametrize(
    "initial, append, match",
    [
        # (InputFactories.DF_RC_NON_RANGE, InputFactories.DF_DTI, "TODO(AN-722)"),
        (InputFactories.DF_RC, InputFactories.ND_ARRAY_1D, "DataFrame"),
        (InputFactories.DF_RC, InputFactories.DF_MULTI_RC, "incompatible"),
        (
            InputFactories.DF_RC,
            InputFactories.DF_RC_NON_RANGE,
            "range",
        ),
        (InputFactories.DF_RC, InputFactories.DF_RC_STEP, "step"),
    ],
)
@pytest.mark.parametrize("swap", ["swap", ""])
def test_(
    initial: InputFactories,
    append: InputFactories,
    match,
    swap,
    lmdb_version_store: NativeVersionStore,
):
    lib = lmdb_version_store
    if swap:
        initial, append = append, initial
    init_data, next_start = initial.make(1, 3)
    lib.write("s", init_data)

    to_append, _ = append.make(abs(next_start), 1)
    with pytest.raises(NormalizationException) as e:
        lib.append("s", to_append)
    assert match in str(e.value)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    col_per_append_df=st.integers(2, 100),
    col_name_set=st.integers(1, 10000),
    num_rows_per_test_cycle=st.lists(st.lists(st.integers(1, 20), min_size=1, max_size=10), max_size=2),
    column_group_size=st.integers(2, 100),
    segment_row_size=st.integers(2, 100),
    dynamic_schema=st.booleans(),
    dynamic_strings=st.booleans(),
    df_in_str=st.booleans(),
)
@SLOW_TESTS_MARK
@pytest.mark.skip(reason="Needs to be fixed with issue #496")
@pytest.mark.storage
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
    basic_store_factory,
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
            assert lib.get_info(sym)["sorted"] == "ASCENDING"
            lib.defragment_symbol_data(sym, None)
            assert lib.get_info(sym)["sorted"] == "ASCENDING"
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
            assert np.array_equal(
                indexs.iloc[1:, 0].astype(str).values,
                indexs.iloc[:-1, 1].astype(str).values,
            )
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
    lib = basic_store_factory(
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


def test_regular_append_dynamic_schema_named_index(
    lmdb_version_store_tiny_segment_dynamic,
):
    lib = lmdb_version_store_tiny_segment_dynamic
    sym = "test_parallel_append_dynamic_schema_named_index"
    df_0 = pd.DataFrame({"col_0": [0], "col_1": [0.5]}, index=pd.date_range("2024-01-01", periods=1))
    df_0.index.name = "date"
    df_1 = pd.DataFrame({"col_0": [1]}, index=pd.date_range("2024-01-02", periods=1))
    lib.write(sym, df_0)
    with pytest.raises(StreamDescriptorMismatch) as exception_info:
        lib.append(sym, df_1)

    assert "date" in str(exception_info.value)


@pytest.mark.parametrize(
    "idx", [pd.date_range(pd.Timestamp("2020-01-01"), periods=3), pd.RangeIndex(start=0, stop=3, step=1)]
)
def test_append_bool_named_col(lmdb_version_store_dynamic_schema, idx):
    symbol = "bad_append"

    initial = pd.DataFrame({"col": [1, 2, 3]}, index=idx)
    lmdb_version_store_dynamic_schema.write(symbol, initial)

    bad_df = pd.DataFrame({True: [4, 5, 6]}, index=idx)

    # The normalization exception is getting reraised as an ArcticNativeException so we check for that
    with pytest.raises(ArcticNativeException):
        lmdb_version_store_dynamic_schema.append(symbol, bad_df)

    assert_frame_equal(lmdb_version_store_dynamic_schema.read(symbol).data, initial)
