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

from arcticdb_ext.version_store import NoSuchVersionException
import arcticdb.toolbox.query_stats as qs
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
)
from arcticdb.util.test import (
    assert_frame_equal,
    assert_series_equal,
    max_rows_per_segment,
    min_rows_per_segment,
    query_stats_operation_count,
    random_strings_of_length,
)
from tests.util.mark import MACOS, WINDOWS
from tests.util.naughty_strings import read_big_list_of_naughty_strings

pytestmark = pytest.mark.pipeline


def generic_append_compact_data_test(lib, sym, df, batch=False, **append_kwargs):
    qs.reset_stats()  # Clear any leftover stats from a previous failed run
    vit_before_compaction = lib.read(sym, output_format="PANDAS" if isinstance(df, pd.DataFrame) else "POLARS")
    oracle_sym = sym + "_oracle"
    lib.write(oracle_sym, vit_before_compaction.data)
    lib.append(oracle_sym, df, compact_data=False, **append_kwargs)
    # Use Polars so that sparse data checking is proper
    expected = lib.read(oracle_sym, output_format="POLARS").data
    pre_compaction_index = lib.read_index(sym)
    pre_compaction_data_keys = len(pre_compaction_index)

    with qs.query_stats():
        (
            lib.batch_append([sym], [df], compact_data=True, **append_kwargs)
            if batch
            else lib.append(sym, df, compact_data=True, **append_kwargs)
        )
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
    # There might be fewer rows in total than min_rows
    min_rows = min(min_rows_per_segment(rows_per_segment), len(expected))
    assert row_counts.min() >= min_rows
    assert row_counts.max() <= max_rows_per_segment(rows_per_segment)

    post_compaction_data_keys = len(post_compaction_index)
    new_data_keys = len(post_compaction_index[post_compaction_index["version_id"] > vit_before_compaction.version])
    compacted_data_keys = pre_compaction_data_keys - (post_compaction_data_keys - new_data_keys)
    assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == compacted_data_keys
    assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == new_data_keys
    # Doing a compaction would now have no impact
    compact_data_info = lib.compact_data_explain_plan(sym)
    assert not compact_data_info.will_do_work


# batch_append does not support the coerce_columns argument
def test_string_none_nan_handling(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory(dynamic_strings=True)
    sym = "test_string_none_nan_handling"
    df = pd.DataFrame({"col": ["hello", np.nan, np.nan, None, None, None, np.nan, np.nan, None, None]})
    lib.write(sym, df[:5], coerce_columns={"col": object})
    generic_append_compact_data_test(lib, sym, df[5:], coerce_columns={"col": object})


@pytest.mark.parametrize("batch", [False, True])
class TestAppendCompactData:
    @pytest.mark.parametrize("index", [None, "ts"])
    def test_basic(self, in_memory_store_factory, clear_query_stats, batch, index):
        lib = in_memory_store_factory()
        sym = "test_basic"
        df_0 = pd.DataFrame(
            {"col": np.arange(20)}, index=None if index is None else pd.date_range("2026-01-01", periods=20)
        )
        lib.write(sym, df_0)
        df_1 = pd.DataFrame(
            {"col": np.arange(20, 30)}, index=None if index is None else pd.date_range("2026-01-21", periods=10)
        )
        generic_append_compact_data_test(lib, sym, df_1, batch)

    def test_frequent_append_io_counts_compact_once(self, in_memory_store_factory, clear_query_stats, batch):
        lib = in_memory_store_factory()
        sym = "test_frequent_append_io_counts_compact_once"
        df = pd.DataFrame({"col": np.arange(200_000)}, index=pd.date_range("2026-01-01", freq="s", periods=200_000))
        for idx in range(99):
            lib.append(sym, df[idx * 2_000 : (idx + 1) * 2_000])
        with qs.query_stats():
            (
                lib.batch_append([sym], [df[99 * 2_000 :]], compact_data=True)
                if batch
                else lib.append(sym, df[99 * 2_000 :], compact_data=True)
            )
            stats = qs.get_query_stats()
        qs.reset_stats()
        received = lib.read(sym).data
        assert_frame_equal(df, received)
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 99
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 2
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_INDEX") == 1
        assert len(lib.read_index(sym)) == 2

    def test_frequent_append_io_counts_compact_every_time(self, in_memory_store_factory, clear_query_stats, batch):
        lib = in_memory_store_factory()
        sym = "test_frequent_append_io_counts_compact_every_time"
        df = pd.DataFrame({"col": np.arange(200_000)}, index=pd.date_range("2026-01-01", freq="s", periods=200_000))
        for idx in range(100):
            with qs.query_stats():
                (
                    lib.batch_append([sym], [df[idx * 2_000 : (idx + 1) * 2_000]], compact_data=True)
                    if batch
                    else lib.append(sym, df[idx * 2_000 : (idx + 1) * 2_000], compact_data=True)
                )
                stats = qs.get_query_stats()
            qs.reset_stats()
            received = lib.read(sym).data
            assert_frame_equal(df[: (idx + 1) * 2_000], received)
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == (0 if idx == 0 else 1)
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") <= 1
            assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") <= 2
            assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_INDEX") == 1
            assert len(lib.read_index(sym)) <= 2

    def test_pyarrow_tables(self, in_memory_version_store_arrow, clear_query_stats, batch):
        lib = in_memory_version_store_arrow
        sym = "test_pyarrow_tables"
        table_0 = pa.table({"col": pa.array(np.arange(20))})
        lib.write(sym, table_0)
        table_1 = pa.table({"col": pa.array(np.arange(20, 30))})
        with qs.query_stats():
            (
                lib.batch_append([sym], [table_1], compact_data=True)
                if batch
                else lib.append(sym, table_1, compact_data=True)
            )
            stats = qs.get_query_stats()
        qs.reset_stats()
        vit = lib.read(sym)
        assert vit.version == 1
        expected = pa.concat_tables([table_0, table_1])
        assert expected.equals(vit.data)
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 1
        assert len(lib.read_index(sym)) == 1

    @pytest.mark.parametrize("index", [None, "ts"])
    def test_series(self, in_memory_store_factory, clear_query_stats, index, batch):
        lib = in_memory_store_factory()
        sym = "test_series"
        series_0 = pd.Series(np.arange(20), index=None if index is None else pd.date_range("2026-01-01", periods=20))
        lib.write(sym, series_0)
        series_1 = pd.Series(
            np.arange(20, 30), index=None if index is None else pd.date_range("2026-01-21", periods=10)
        )
        with qs.query_stats():
            (
                lib.batch_append([sym], [series_1], compact_data=True)
                if batch
                else lib.append(sym, series_1, compact_data=True)
            )
            stats = qs.get_query_stats()
        qs.reset_stats()
        vit = lib.read(sym)
        assert vit.version == 1
        expected = pd.concat([series_0, series_1])
        if index is None:
            expected.reset_index(drop=True, inplace=True)
        assert_series_equal(expected, vit.data)
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 1
        assert len(lib.read_index(sym)) == 1

    def test_numpy_arrays(self, in_memory_store_factory, clear_query_stats, batch):
        lib = in_memory_store_factory()
        sym = "test_numpy_arrays"
        array_0 = np.arange(20)
        lib.write(sym, array_0)
        array_1 = np.arange(20, 30)
        with qs.query_stats():
            (
                lib.batch_append([sym], [array_1], compact_data=True)
                if batch
                else lib.append(sym, array_1, compact_data=True)
            )
            stats = qs.get_query_stats()
        qs.reset_stats()
        vit = lib.read(sym)
        assert vit.version == 1
        expected = np.concatenate([array_0, array_1])
        assert (vit.data == expected).all()
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 1
        assert len(lib.read_index(sym)) == 1

    def test_existing_zero_rows(self, in_memory_store_factory, clear_query_stats, batch):
        lib = in_memory_store_factory(segment_row_size=10)
        sym = "test_existing_zero_rows"
        # Zero-rowed data gets stored with a datetime index
        df_0 = pd.DataFrame({"col": np.arange(0)})
        lib.write(sym, df_0)
        df_1 = pd.DataFrame({"col": np.arange(15)}, index=pd.date_range("2026-01-21", periods=15))
        generic_append_compact_data_test(lib, sym, df_1, batch=batch)

    @pytest.mark.parametrize("write_if_missing", [True, False])
    @pytest.mark.parametrize("compact_data", [True, False])
    def test_write_if_missing(self, in_memory_store_factory, write_if_missing, compact_data, batch):
        lib = in_memory_store_factory(segment_row_size=10)
        sym = "test_write_if_missing"
        df = pd.DataFrame({"col": np.arange(15)})
        if write_if_missing:
            (
                lib.batch_append([sym], [df], compact_data=compact_data, write_if_missing=write_if_missing)
                if batch
                else lib.append(sym, df, compact_data=compact_data, write_if_missing=write_if_missing)
            )
            assert_frame_equal(df, lib.read(sym).data)
            index = lib.read_index(sym)
            row_counts = (index["end_row"] - index["start_row"]).to_list()
            # See comment in LocalVersionedEngine::append_internal as to why this isn't [8, 7] when compact_data is
            # True
            assert row_counts == [10, 5]
        else:
            with pytest.raises(NoSuchVersionException):
                (
                    lib.batch_append([sym], [df], compact_data=compact_data, write_if_missing=write_if_missing)
                    if batch
                    else lib.append(sym, df, compact_data=compact_data, write_if_missing=write_if_missing)
                )

    def test_metadata(self, in_memory_store_factory, batch):
        lib = in_memory_store_factory()
        sym = "test_metadata"
        lib.write(sym, pd.DataFrame({"col": [0]}), metadata="0")
        (
            lib.batch_append([sym], [pd.DataFrame({"col": [1]})], metadata_vector=["1"], compact_data=True)
            if batch
            else lib.append(sym, pd.DataFrame({"col": [1]}), metadata="1", compact_data=True)
        )
        vit = lib.read(sym)
        assert vit.metadata == "1"
        assert_frame_equal(vit.data, pd.DataFrame({"col": [0, 1]}))
        assert len(lib.read_index(sym)) == 1

    @pytest.mark.parametrize("index", [None, "ts"])
    def test_compact_whole_symbol(self, in_memory_store_factory, clear_query_stats, index, batch):
        lib = in_memory_store_factory(segment_row_size=10)
        sym = "test_compact_whole_symbol"
        df = pd.DataFrame(
            {"col": np.arange(20)}, index=None if index is None else pd.date_range("2026-01-01", periods=20)
        )
        lib.write(sym, df[:5])
        lib.append(sym, df[5:10])
        lib.append(sym, df[10:15])
        generic_append_compact_data_test(lib, sym, df[15:], batch)

    @pytest.mark.parametrize("index", [None, "ts"])
    def test_compact_leftover_slices(self, in_memory_store_factory, clear_query_stats, index, batch):
        lib = in_memory_store_factory(segment_row_size=10)
        sym = "test_compact_leftover_slices"
        df = pd.DataFrame(
            {"col": np.arange(20)}, index=None if index is None else pd.date_range("2026-01-01", periods=20)
        )
        lib.write(sym, df[:5])
        generic_append_compact_data_test(lib, sym, df[5:], batch)

    def test_existing_data_compacted(self, in_memory_store_factory, clear_query_stats, batch):
        lib = in_memory_store_factory(segment_row_size=10)
        sym = "test_existing_data_compacted"
        df = pd.DataFrame({"col": np.arange(20)})
        lib.write(sym, df[:10])
        generic_append_compact_data_test(lib, sym, df[10:], batch)

    @pytest.mark.parametrize("total_rows", [25, 30, 35])
    def test_tail_of_existing_data_already_compacted(
        self, in_memory_store_factory, clear_query_stats, total_rows, batch
    ):
        lib = in_memory_store_factory(segment_row_size=10)
        sym = "test_tail_of_existing_data_already_compacted"
        df = pd.DataFrame({"col": np.arange(total_rows)})
        lib.write(sym, df[:5])
        lib.append(sym, df[5:10])
        lib.append(sym, df[10:20])
        assert len(lib.read_index(sym)) == 3
        generic_append_compact_data_test(lib, sym, df[20:], batch)

    @pytest.mark.parametrize("index", [None, "ts"])
    @pytest.mark.parametrize("segment_row_size", [100_000, 10, 5, 2])
    def test_dynamic_schema_col_ordering(
        self, in_memory_store_factory, clear_query_stats, index, segment_row_size, batch
    ):
        lib = in_memory_store_factory(segment_row_size=segment_row_size, dynamic_schema=True)
        sym = "test_dynamic_schema_col_ordering"
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
        generic_append_compact_data_test(lib, sym, df_1, batch)

    @pytest.mark.parametrize("segment_row_size", [100_000, 10, 5, 2])
    def test_dynamic_schema_type_promotion(self, in_memory_store_factory, clear_query_stats, segment_row_size, batch):
        lib = in_memory_store_factory(segment_row_size=segment_row_size, dynamic_schema=True)
        sym = "test_dynamic_schema_type_promotion"
        df_0 = pd.DataFrame(
            {
                "col_0": np.arange(20, dtype=np.float64),
                "col_1": np.arange(20, 40, dtype=np.uint8),
                "col_2": np.arange(40, 60, dtype=np.int16),
            },
        )
        lib.write(sym, df_0)
        df_1 = pd.DataFrame(
            {
                "col_0": np.arange(100, 110, dtype=np.int32),
                "col_1": np.arange(60, 70, dtype=np.uint16),
                "col_2": np.arange(40, 50, dtype=np.uint16),
            },
        )
        generic_append_compact_data_test(lib, sym, df_1, batch)

    @pytest.mark.parametrize("index", [None, "ts"])
    @pytest.mark.parametrize("segment_row_size", [100_000, 10, 5])
    def test_column_slicing(self, in_memory_store_factory, clear_query_stats, index, segment_row_size, batch):
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
        generic_append_compact_data_test(lib, sym, df_1, batch)

    @pytest.mark.parametrize("names", [None, ["ts", None], [None, "level 2"], ["ts", "level 2"]])
    def test_multiindex(self, in_memory_store_factory, clear_query_stats, names, batch):
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
        with qs.query_stats():
            (
                lib.batch_append([sym], [df[5:]], compact_data=True)
                if batch
                else lib.append(sym, df[5:], compact_data=True)
            )
            stats = qs.get_query_stats()
        qs.reset_stats()
        vit = lib.read(sym)
        assert vit.version == 1
        assert_frame_equal(df, vit.data)
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 2
        assert len(lib.read_index(sym)) == 2

    @pytest.mark.parametrize("dynamic_strings_first", [True, False])
    def test_fixed_width_and_dynamic_strings(
        self, in_memory_store_factory, clear_query_stats, dynamic_strings_first, batch
    ):
        lib = in_memory_store_factory()
        sym = "test_fixed_width_and_dynamic_strings"
        # Include two segments with different widths of strings
        df = pd.DataFrame({"col": ["a", "bb", "ccc", "dddd", "eeeee", "f", "gg", "hhhhhhhhhhhhhh", "i"]})
        lib.write(sym, df[:3], dynamic_strings=dynamic_strings_first)
        lib.append(sym, df[3:5], dynamic_strings=dynamic_strings_first)
        lib.append(sym, df[5:7], dynamic_strings=not dynamic_strings_first)
        generic_append_compact_data_test(lib, sym, df[7:], batch, dynamic_strings=not dynamic_strings_first)

    @pytest.mark.parametrize("dynamic_strings_first", [True, False])
    def test_blns(self, in_memory_store_factory, clear_query_stats, dynamic_strings_first, batch):
        lib = in_memory_store_factory()
        sym = "test_blns"
        df = pd.DataFrame({"col": read_big_list_of_naughty_strings()})
        lib.write(sym, df[: len(df) // 2], dynamic_strings=dynamic_strings_first)
        generic_append_compact_data_test(lib, sym, df[len(df) // 2 :], batch, dynamic_strings=not dynamic_strings_first)

    def test_append_empty_frame_compacts_existing_data(self, in_memory_store_factory, clear_query_stats, batch):
        lib = in_memory_store_factory(segment_row_size=10)
        sym = "test_append_empty_frame_compacts_existing_data"
        lib.write(sym, pd.DataFrame({"col": np.arange(5)}))
        lib.append(sym, pd.DataFrame({"col": np.arange(5, 10)}))
        # Schema checks happen after empty input frame checks, so we don't need the same column set
        with qs.query_stats():
            lib.append(sym, pd.DataFrame())
            stats = qs.get_query_stats()
        qs.reset_stats()
        assert lib.read(sym).version == 2
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 0
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 0
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_INDEX") == 1
        with qs.query_stats():
            (
                lib.batch_append([sym], [pd.DataFrame()], compact_data=True)
                if batch
                else lib.append(sym, pd.DataFrame(), compact_data=True)
            )
            stats = qs.get_query_stats()
        qs.reset_stats()
        assert lib.read(sym).version == 3
        assert len(lib.read_index(sym)) == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 2
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == 1
        assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_INDEX") == 1

    @pytest.mark.parametrize("rows_to_append", [5, 10, 15, 20])
    def test_fortran_ordered_data(self, in_memory_store_factory, clear_query_stats, rows_to_append, batch):
        lib = in_memory_store_factory(segment_row_size=10)
        sym = "test_fortran_ordered_data"
        cols = ["col_0", "col_1"]
        df_0 = pd.DataFrame(np.random.randint(0, 100, size=(5, 2)), columns=cols)
        lib.write(sym, df_0)
        df_1 = pd.DataFrame(np.random.randint(0, 100, size=(rows_to_append, 2)), columns=cols)
        generic_append_compact_data_test(lib, sym, df_1, batch)

    @pytest.mark.parametrize("index", [None, "ts"])
    def test_column_filtered_read(self, in_memory_store_factory, clear_query_stats, index, batch):
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
            generic_append_compact_data_test(lib, sym, df[i * 5 : (i + 1) * 5], batch)
        expected_col_a = df[["col_a"]]
        expected_col_bc = df[["col_b", "col_c"]]
        assert_frame_equal(expected_col_a, lib.read(sym, columns=["col_a"]).data)
        assert_frame_equal(expected_col_bc, lib.read(sym, columns=["col_b", "col_c"]).data)

    @pytest.mark.parametrize("rows_per_segment", [3, 7, 10])
    def test_date_range_read(self, in_memory_store_factory, clear_query_stats, rows_per_segment, batch):
        lib = in_memory_store_factory(segment_row_size=rows_per_segment, dynamic_strings=True)
        sym = "test_date_range_read"
        num_rows = 100
        index = pd.date_range("2026-01-01", periods=num_rows)
        df = pd.DataFrame(
            {"ints": np.arange(num_rows), "strings": 20 * ["hello", None, "gutentag", np.nan, "konichiwa"]}, index=index
        )
        lib.write(sym, df[:5])
        for i in range(1, 20):
            generic_append_compact_data_test(lib, sym, df[i * 5 : (i + 1) * 5], batch)
        mid = index[num_rows // 2]
        expected_first_half = df[:mid]
        expected_second_half = df[mid:]
        assert_frame_equal(expected_first_half, lib.read(sym, date_range=(index[0], mid)).data)
        assert_frame_equal(expected_second_half, lib.read(sym, date_range=(mid, index[-1])).data)

    def test_read_previous_version(self, in_memory_store_factory, clear_query_stats, batch):
        lib = in_memory_store_factory()
        sym = "test_read_previous_version"
        df = pd.DataFrame({"col": np.arange(10)})
        lib.write(sym, df[:5])
        generic_append_compact_data_test(lib, sym, df[5:], batch)
        assert_frame_equal(df[:5], lib.read(sym, as_of=0).data)
        assert_frame_equal(df, lib.read(sym, as_of=1).data)
        assert_frame_equal(df, lib.read(sym).data)

    def test_schema_mismatch_static(self, in_memory_store_factory, batch):
        lib = in_memory_store_factory()
        sym = "test_schema_mismatch_static"
        df_0 = pd.DataFrame({"col_0": [0]})
        lib.write(sym, df_0)
        # Different column sets
        df_1 = pd.DataFrame({"col_1": [0]})
        with pytest.raises(Exception) as e_without_arg:
            lib.batch_append([sym], [df_1]) if batch else lib.append(sym, df_1)
        with pytest.raises(Exception) as e_with_arg:
            lib.batch_append([sym], [df_1], compact_data=True) if batch else lib.append(sym, df_1, compact_data=True)
        assert e_with_arg.type == e_without_arg.type
        assert e_with_arg.typename == e_without_arg.typename
        assert e_with_arg.value.args[0] == e_without_arg.value.args[0]
        # Different column type
        df_1 = pd.DataFrame({"col_0": ["hello"]})
        with pytest.raises(Exception) as e_without_arg:
            lib.batch_append([sym], [df_1]) if batch else lib.append(sym, df_1)
        with pytest.raises(Exception) as e_with_arg:
            lib.batch_append([sym], [df_1], compact_data=True) if batch else lib.append(sym, df_1, compact_data=True)
        assert e_with_arg.type == e_without_arg.type
        assert e_with_arg.typename == e_without_arg.typename
        assert e_with_arg.value.args[0] == e_without_arg.value.args[0]

    def test_schema_mismatch_dynamic(self, in_memory_store_factory, batch):
        lib = in_memory_store_factory(dynamic_schema=True)
        sym = "test_schema_mismatch_dynamic"
        df_0 = pd.DataFrame({"col_0": [0]})
        lib.write(sym, df_0)
        df_1 = pd.DataFrame({"col_0": ["hello"]})
        with pytest.raises(Exception) as e_without_arg:
            lib.batch_append([sym], [df_1]) if batch else lib.append(sym, df_1)
        with pytest.raises(Exception) as e_with_arg:
            lib.batch_append([sym], [df_1], compact_data=True) if batch else lib.append(sym, df_1, compact_data=True)
        assert e_with_arg.type == e_without_arg.type
        assert e_with_arg.typename == e_without_arg.typename
        assert e_with_arg.value.args[0] == e_without_arg.value.args[0]


def test_batch_basic(in_memory_store_factory):
    lib = in_memory_store_factory()
    num_symbols = 3
    syms = [f"test_batch_duplicated_symbol_{i}" for i in range(num_symbols)]
    write_dfs = [pd.DataFrame({"col": [i]}, index=[pd.Timestamp(0)]) for i in range(num_symbols)]
    lib.batch_write(syms, write_dfs)
    append_dfs = [pd.DataFrame({"col": [i + 10]}, index=[pd.Timestamp(1)]) for i in range(num_symbols)]
    lib.batch_append(syms, append_dfs, compact_data=True)
    expected_dfs = [pd.concat([write_dfs[i], append_dfs[i]]) for i in range(num_symbols)]
    for sym, expected_df in zip(syms, expected_dfs):
        received_df = lib.read(sym).data
        assert_frame_equal(expected_df, received_df)
        assert len(lib.read_index(sym)) == 1


def test_batch_upsert_all(in_memory_store_factory):
    lib = in_memory_store_factory()
    num_symbols = 3
    syms = [f"test_batch_upsert_all_{i}" for i in range(num_symbols)]
    append_dfs = [pd.DataFrame({"col": [i + 10]}, index=[pd.Timestamp(1)]) for i in range(num_symbols)]
    lib.batch_append(syms, append_dfs, compact_data=True)
    for sym, append_df in zip(syms, append_dfs):
        received_df = lib.read(sym).data
        assert_frame_equal(append_df, received_df)
        assert len(lib.read_index(sym)) == 1


def test_batch_upsert_some(in_memory_store_factory):
    lib = in_memory_store_factory()
    num_symbols = 4
    syms = [f"test_batch_duplicated_symbol_{i}" for i in range(num_symbols)]
    write_dfs = [pd.DataFrame({"col": [i]}, index=[pd.Timestamp(0)]) for i in range(num_symbols // 2)]
    lib.batch_write(syms[: num_symbols // 2], write_dfs)
    append_dfs = [pd.DataFrame({"col": [i + 10]}, index=[pd.Timestamp(1)]) for i in range(num_symbols)]
    lib.batch_append(syms, append_dfs, compact_data=True)
    expected_dfs = [pd.concat([write_dfs[i], append_dfs[i]]) for i in range(num_symbols // 2)] + append_dfs[
        num_symbols // 2 :
    ]
    for sym, expected_df in zip(syms, expected_dfs):
        received_df = lib.read(sym).data
        assert_frame_equal(expected_df, received_df)
        assert len(lib.read_index(sym)) == 1


# GIL must be taken for all symbols while compacting with these strings
def test_batch_blns(in_memory_store_factory):
    lib = in_memory_store_factory()
    num_symbols = 10
    num_chunks = 5
    syms = [f"test_batch_blns_{i}" for i in range(num_symbols)]
    df = pd.DataFrame({"col": read_big_list_of_naughty_strings()})
    rows_per_chunk = len(df) // num_chunks
    lib.batch_write(syms, [df[:rows_per_chunk] for _ in range(num_symbols)])
    for chunk in range(1, num_chunks):
        lib.batch_append(
            syms,
            [df[chunk * rows_per_chunk : (chunk + 1) * rows_per_chunk] for _ in range(num_symbols)],
            compact_data=True,
        )
    for sym in syms:
        received_df = lib.read(sym).data
        assert_frame_equal(df, received_df)
        assert len(lib.read_index(sym)) == 1


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
            # This basically does lib.append(sym, table.slice(length=rows_to_take), compact_data=True), plus some
            # read-only checks
            generic_append_compact_data_test(lib, sym, table.slice(length=rows_to_take))
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
            # This basically does lib.append(sym, table, compact_data=True), plus some read-only checks
            generic_append_compact_data_test(lib, sym, table)
        remaining_rows -= rows_to_take
