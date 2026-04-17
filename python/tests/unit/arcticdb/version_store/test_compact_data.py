"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from hypothesis import given, settings, assume
import hypothesis.strategies as st
import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.exceptions import DuplicateKeyException, SchemaException, StorageException
from arcticdb_ext.storage import KeyType
from arcticdb.exceptions import ArcticNativeException, UserInputException
import arcticdb.toolbox.query_stats as qs
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
)
from arcticdb.util.test import assert_frame_equal, config_context, query_stats_operation_count, random_strings_of_length
from tests.util.mark import MACOS
from tests.util.naughty_strings import read_big_list_of_naughty_strings


def generic_compact_data_test(lib, sym, method_arg=None):
    qs.reset_stats()  # Clear any leftover stats from a previous failed run
    pickled = lib.is_symbol_pickled(sym)
    vit_before_compaction = lib.read(sym)
    expected = vit_before_compaction.data
    pre_compaction_data_keys = len(lib.read_index(sym))
    with qs.query_stats():
        lib.compact_data_experimental(sym, rows_per_segment=method_arg)
        stats = qs.get_query_stats()
    qs.reset_stats()
    rows_per_segment = (
        lib.lib_cfg().lib_desc.version.write_options.segment_row_size if method_arg is None else method_arg
    )
    if rows_per_segment == 0:
        rows_per_segment = 100_000
    received = lib.read(sym).data
    if pickled:
        assert received == expected
    else:
        assert_frame_equal(expected, received)
    index = lib.read_index(sym)
    row_counts = index["end_row"] - index["start_row"]
    # Definitions taken from CompactDataClause constructor
    min_rows_per_segment = max((2 * rows_per_segment) // 3, 1)
    max_rows_per_segment = max((4 * rows_per_segment) // 3, rows_per_segment + 1)
    if not pickled:
        # There might be fewer rows in total than min_rows_per_segment
        min_rows_per_segment = min(min_rows_per_segment, len(expected))
    assert row_counts.min() >= min_rows_per_segment
    assert row_counts.max() <= max_rows_per_segment

    post_compaction_data_keys = len(index)
    new_data_keys = len(index[index["version_id"] > vit_before_compaction.version])
    compacted_data_keys = pre_compaction_data_keys - (post_compaction_data_keys - new_data_keys)
    assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == compacted_data_keys
    assert query_stats_operation_count(stats, "Memory_PutObject", "TABLE_DATA") == new_data_keys
    # Second compaction should always be a no-op
    generic_compact_data_test_noop(lib, sym, rows_per_segment)


def generic_compact_data_test_noop(lib, sym, rows_per_segment=None):
    qs.reset_stats()  # Clear any leftover stats from a previous failed run
    pickled = lib.is_symbol_pickled(sym)
    vit_before_compaction = lib.read(sym)
    expected = vit_before_compaction.data
    pre_compaction_index = lib.read_index(sym)
    pre_compaction_data_keys = len(lib.read_index(sym))
    with qs.query_stats():
        compacted_version = lib.compact_data_experimental(sym, rows_per_segment=rows_per_segment).version
        stats = qs.get_query_stats()
    qs.reset_stats()
    assert vit_before_compaction.version == compacted_version
    received = lib.read(sym).data
    if pickled:
        assert received == expected
    else:
        assert_frame_equal(expected, received)
    post_compaction_index = lib.read_index(sym)
    assert_frame_equal(post_compaction_index, pre_compaction_index)
    new_data_keys = len(post_compaction_index[post_compaction_index["version_id"] > vit_before_compaction.version])
    assert new_data_keys == 0
    assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 0
    # No objects should be written at all in this case
    assert "Memory_PutObject" not in stats["storage_operations"]


def test_docstring_example_v1(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"col": np.arange(100_000)})
    for idx in range(100):
        lib.append("sym", df[idx * 1_000 : (idx + 1) * 1_000])
    assert len(lib.read_index("sym")) == 100
    lib.compact_data_experimental("sym")
    assert len(lib.read_index("sym")) == 1


def test_docstring_example_v2(lmdb_library):
    lib = lmdb_library
    df = pd.DataFrame({"col": np.arange(100_000)})
    for idx in range(100):
        lib.append("sym", df[idx * 1_000 : (idx + 1) * 1_000])
    lib_tool = lib._dev_tools.library_tool()
    assert len(lib_tool.read_index("sym")) == 100
    lib.compact_data_experimental("sym")
    assert len(lib_tool.read_index("sym")) == 1


def test_compact_data_symbol_doesnt_exist(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_compact_data_symbol_doesnt_exist"
    with pytest.raises(StorageException) as e:
        lib.compact_data_experimental(sym)
    assert sym in str(e.value)


@pytest.mark.parametrize("rows_per_segment", [0, -1, -100_000])
def test_compact_data_invalid_rows_per_segment(lmdb_version_store_v1, rows_per_segment):
    lib = lmdb_version_store_v1
    sym = "test_compact_data_invalid_rows_per_segment"
    with pytest.raises(ArcticNativeException):
        lib.compact_data_experimental(sym, rows_per_segment=rows_per_segment)


def test_compact_data_maintain_metadata(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_compact_data_maintain_metadata"
    df = pd.DataFrame({"col": np.arange(10)})
    lib.write(sym, df)
    metadata = {"hello": "world"}
    lib.append(sym, df, metadata=metadata)
    assert lib.read_metadata(sym).metadata == metadata
    lib.compact_data_experimental(sym)
    assert len(lib.read_index(sym)) == 1
    assert lib.read_metadata(sym).metadata == metadata


@pytest.mark.parametrize("lib_config_value", [1, 2, 3, 5, 7, 10])
@pytest.mark.parametrize("method_arg", [1, 2, 3, 5, 7, 10])
def test_compact_data_explicit_rows_per_segment(
    in_memory_store_factory, clear_query_stats, lib_config_value, method_arg
):
    rng = np.random.default_rng()
    lib = in_memory_store_factory(segment_row_size=lib_config_value, dynamic_strings=True)
    sym = "test_compact_data_explicit_rows_per_segment"
    df = pd.DataFrame(
        {
            "ints": np.arange(30, dtype=np.int64),
            "floats": np.arange(30, 60, dtype=np.float32),
            "bools": rng.random(30) > 0.5,
            # Include multiple string columns with overlap of the values to test string pool construction
            "strings_1": 6 * ["hello", None, "gutentag", np.nan, "konichiwa"],
            "strings_2": 6 * ["hello", "bonjour", "gutentag", "nihao", "konichiwa"],
        }
    )
    lib.write(sym, df)
    generic_compact_data_test(lib, sym, method_arg)


@pytest.mark.parametrize("method_argument", [1, 8, 10, 13, 100])
def test_compact_data_widely_varying_row_counts(in_memory_store_factory, clear_query_stats, method_argument):
    rng = np.random.default_rng()
    lib = in_memory_store_factory(segment_row_size=100, dynamic_strings=True)
    sym = "test_compact_data_widely_varying_row_counts"
    df = pd.DataFrame(
        {
            "ints": np.arange(303, dtype=np.int64),
            "floats": np.arange(303, dtype=np.float32),
            "bools": rng.random(303) > 0.5,
            "strings": 101 * ["hello", "bonjour", "gutentag"],
        }
    )
    # Produce alternating 100 and 1 row segments
    lib.write(sym, df[:100])
    lib.append(sym, df[100:101])
    lib.append(sym, df[101:201])
    lib.append(sym, df[201:202])
    lib.append(sym, df[202:302])
    lib.append(sym, df[302:])
    generic_compact_data_test(lib, sym, method_argument)


@pytest.mark.parametrize("rows_per_segment", [1, 2, 3, 5, 7, 10])
@pytest.mark.parametrize("initial_rows", [20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
@pytest.mark.parametrize("append_rows", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_compact_data_append(in_memory_store_factory, clear_query_stats, rows_per_segment, initial_rows, append_rows):
    rng = np.random.default_rng()
    lib = in_memory_store_factory(segment_row_size=rows_per_segment, dynamic_strings=True)
    sym = "test_compact_data_append"
    string_values = random_strings_of_length(5, 5, True)
    df = pd.DataFrame(
        {
            "ints": np.arange(initial_rows + append_rows, dtype=np.int64),
            "floats": np.arange(initial_rows + append_rows, 2 * (initial_rows + append_rows), dtype=np.int64),
            "bools": rng.random(initial_rows + append_rows) > 0.5,
            "strings": rng.choice(string_values, initial_rows + append_rows),
        }
    )
    lib.write(sym, df[:initial_rows])
    lib.append(sym, df[initial_rows:])
    generic_compact_data_test(lib, sym)


@pytest.mark.parametrize("rows_per_segment", [1, 2, 3, 5, 7, 10])
@pytest.mark.parametrize("initial_rows", [20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
@pytest.mark.parametrize("update_rows", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_compact_data_update(in_memory_store_factory, clear_query_stats, rows_per_segment, initial_rows, update_rows):
    rng = np.random.default_rng()
    lib = in_memory_store_factory(segment_row_size=rows_per_segment, dynamic_strings=True)
    sym = "test_compact_data_update"
    string_values = random_strings_of_length(5, 5, True)
    write_df = pd.DataFrame(
        {
            "ints": np.arange(initial_rows, dtype=np.int64),
            "floats": np.arange(initial_rows, 2 * initial_rows, dtype=np.float32),
            "bools": rng.random(initial_rows) > 0.5,
            "strings": rng.choice(string_values, initial_rows),
        },
        index=pd.date_range("2026-01-01", periods=initial_rows),
    )
    lib.write(sym, write_df)
    update_df = pd.DataFrame(
        {
            "ints": np.arange(initial_rows, initial_rows + update_rows, dtype=np.int64),
            "floats": np.arange(initial_rows, initial_rows + update_rows, dtype=np.float32),
            "bools": rng.random(update_rows) > 0.5,
            "strings": rng.choice(string_values, update_rows),
        },
        index=pd.date_range("2026-01-15", periods=update_rows),
    )
    lib.update(sym, update_df)
    generic_compact_data_test(lib, sym)


@pytest.mark.parametrize("index", [None, pd.date_range("2026-01-01", periods=50)])
def test_compact_data_column_slicing(in_memory_store_factory, clear_query_stats, index):
    rows_per_segment = 10
    lib = in_memory_store_factory(column_group_size=2, segment_row_size=rows_per_segment)
    sym = "test_compact_data_column_slicing"
    num_rows = 50
    df = pd.DataFrame({f"col_{idx}": np.arange(idx * num_rows, (idx + 1) * num_rows) for idx in range(5)}, index=index)
    lib.write(sym, df[: num_rows // 2])
    lib.append(sym, df[num_rows // 2 :])
    generic_compact_data_test(lib, sym)


@pytest.mark.parametrize("names", [None, ["ts", None], [None, "level 2"], ["ts", "level 2"]])
def test_compact_data_multiindex(in_memory_store_factory, clear_query_stats, names):
    rows_per_segment = 100
    lib = in_memory_store_factory(segment_row_size=rows_per_segment, dynamic_strings=True)
    sym = "test_compact_data_multiindex"
    num_rows = rows_per_segment
    df = pd.DataFrame(
        {"col": np.arange(num_rows)},
        index=pd.MultiIndex.from_product(
            [pd.date_range("2026-01-01", periods=num_rows // 2), ["GOOG", "AAPL"]], names=names
        ),
    )
    lib.write(sym, df[: num_rows // 2])
    lib.append(sym, df[num_rows // 2 :])
    generic_compact_data_test(lib, sym)


@pytest.mark.parametrize("rows_per_segment", [3, 7, 10])
def test_compact_data_many_appends(in_memory_store_factory, clear_query_stats, rows_per_segment):
    lib = in_memory_store_factory(segment_row_size=rows_per_segment, dynamic_strings=True)
    sym = "test_compact_data_many_appends"
    df = pd.DataFrame({"ints": np.arange(50), "strings": 10 * ["hello", None, "gutentag", np.nan, "konichiwa"]})
    for i in range(50):
        lib.append(sym, df[i : i + 1])
    generic_compact_data_test(lib, sym)


def test_compact_data_newest_version_deleted(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_compact_data_newest_version_deleted"
    df = pd.DataFrame({"col": np.arange(30)})
    metadata = {"hello": "world"}
    lib.write(sym, df[:10])
    lib.append(sym, df[10:20], metadata=metadata)
    lib.append(sym, df[20:])
    lib.delete_version(sym, 2)
    generic_compact_data_test(lib, sym)
    vit = lib.read(sym)
    assert vit.version == 3
    assert_frame_equal(vit.data, df[:20])
    assert vit.metadata == metadata


def test_compact_data_newest_version_deleted_noop(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_compact_data_newest_version_deleted_noop"
    df = pd.DataFrame({"col": np.arange(30)})
    metadata = {"hello": "world"}
    lib.write(sym, df[:10])
    lib.append(sym, df[10:20], metadata=metadata)
    lib.append(sym, df[20:])
    lib.delete_version(sym, 2)
    generic_compact_data_test_noop(lib, sym)
    vit = lib.read(sym)
    assert vit.version == 1
    assert_frame_equal(vit.data, df[:20])


def test_compact_data_read_previous_version(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_compact_data_read_previous_version"
    df = pd.DataFrame({"col": np.arange(10)})
    lib.write(sym, df[:5])  # v0
    lib.append(sym, df[5:])  # v1
    lib.compact_data_experimental(sym)  # v2
    assert_frame_equal(df[:5], lib.read(sym, as_of=0).data)
    assert_frame_equal(df, lib.read(sym, as_of=1).data)
    assert_frame_equal(df, lib.read(sym).data)


@pytest.mark.parametrize("rows_per_segment", [3, 7, 10])
def test_compact_data_date_range_read(in_memory_store_factory, rows_per_segment):
    lib = in_memory_store_factory(segment_row_size=rows_per_segment, dynamic_strings=True)
    sym = "test_compact_data_date_range_read"
    num_rows = 100
    index = pd.date_range("2026-01-01", periods=num_rows)
    df = pd.DataFrame(
        {"ints": np.arange(num_rows), "strings": 20 * ["hello", None, "gutentag", np.nan, "konichiwa"]}, index=index
    )
    for i in range(20):
        lib.append(sym, df[i * 5 : (i + 1) * 5])
    mid = index[num_rows // 2]
    expected_first_half = lib.read(sym, date_range=(index[0], mid)).data
    expected_second_half = lib.read(sym, date_range=(mid, index[-1])).data
    lib.compact_data_experimental(sym)
    assert_frame_equal(expected_first_half, lib.read(sym, date_range=(index[0], mid)).data)
    assert_frame_equal(expected_second_half, lib.read(sym, date_range=(mid, index[-1])).data)


def test_compact_data_single_row(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_compact_data_single_row"
    df = pd.DataFrame({"col": [42]})
    lib.write(sym, df)
    generic_compact_data_test_noop(lib, sym)


def test_compact_data_empty_dataframe(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_compact_data_empty_dataframe"
    df = pd.DataFrame({"col": np.array([], dtype=np.int64)})
    lib.write(sym, df)
    generic_compact_data_test_noop(lib, sym)


@pytest.mark.parametrize("rows_per_segment", [5, 10, 20])
def test_compact_data_total_rows_equals_rows_per_segment(in_memory_store_factory, clear_query_stats, rows_per_segment):
    lib = in_memory_store_factory(segment_row_size=rows_per_segment)
    sym = "test_compact_data_total_rows_equals_rows_per_segment"
    df = pd.DataFrame({"col": np.arange(rows_per_segment)})
    lib.write(sym, df)
    generic_compact_data_test_noop(lib, sym)


def test_compact_data_column_filtered_read(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory(column_group_size=2, segment_row_size=10)
    sym = "test_compact_data_column_filtered_read"
    num_rows = 20
    df = pd.DataFrame(
        {
            "col_a": np.arange(num_rows),
            "col_b": np.arange(num_rows, 2 * num_rows),
            "col_c": np.arange(2 * num_rows, 3 * num_rows),
        }
    )
    for i in range(4):
        lib.append(sym, df[i * 5 : (i + 1) * 5])
    expected_col_a = lib.read(sym, columns=["col_a"]).data
    expected_col_bc = lib.read(sym, columns=["col_b", "col_c"]).data
    generic_compact_data_test(lib, sym)
    assert_frame_equal(expected_col_a, lib.read(sym, columns=["col_a"]).data)
    assert_frame_equal(expected_col_bc, lib.read(sym, columns=["col_b", "col_c"]).data)


def test_compact_data_fixed_width_strings(in_memory_store_factory):
    lib = in_memory_store_factory()
    sym = "test_compact_data_fixed_width_strings"
    assert not lib.lib_cfg().lib_desc.version.write_options.dynamic_strings
    lib.write(sym, pd.DataFrame({"col": ["a", "bb", "ccc"]}))
    lib.append(sym, pd.DataFrame({"col": ["dddd", "eeeee"]}))
    generic_compact_data_test(lib, sym)


@pytest.mark.parametrize("dynamic_strings_first", [True, False])
def test_compact_data_fixed_width_and_dynamic_strings(in_memory_store_factory, dynamic_strings_first):
    lib = in_memory_store_factory()
    sym = "test_compact_data_fixed_width_and_dynamic_strings"
    # Include two segments with different widths of strings
    lib.write(sym, pd.DataFrame({"col": ["a", "bb", "ccc"]}), dynamic_strings=dynamic_strings_first)
    lib.append(sym, pd.DataFrame({"col": ["dddd", "eeeee"]}), dynamic_strings=dynamic_strings_first)
    lib.append(sym, pd.DataFrame({"col": ["f", "gg"]}), dynamic_strings=not dynamic_strings_first)
    lib.append(sym, pd.DataFrame({"col": ["hhhhhhhhhhhhhh", "i"]}), dynamic_strings=not dynamic_strings_first)
    generic_compact_data_test(lib, sym)


@pytest.mark.parametrize("dynamic_strings_first", [True, False])
@pytest.mark.parametrize("operation", ["combine", "split"])
def test_compact_data_blns(in_memory_store_factory, dynamic_strings_first, operation):
    lib = in_memory_store_factory()
    sym = "test_compact_data_blns"
    df = pd.DataFrame({"col": read_big_list_of_naughty_strings()})
    lib.write(sym, df[: len(df) // 2], dynamic_strings=dynamic_strings_first)
    lib.append(sym, df[len(df) // 2 :], dynamic_strings=not dynamic_strings_first)
    generic_compact_data_test(lib, sym, len(df) if operation == "combine" else len(df) // 4)


def test_compact_data_string_none_nan_handling(in_memory_store_factory):
    lib = in_memory_store_factory(dynamic_strings=True)
    sym = "test_compact_data_string_none_nan_handling"
    # Combine string columns with only Nones and NaNs
    lib.write(sym, pd.DataFrame({"col": [None, np.nan, np.nan, None, None]}), coerce_columns={"col": object})
    lib.append(sym, pd.DataFrame({"col": [None, np.nan, np.nan, None, None]}), coerce_columns={"col": object})
    generic_compact_data_test(lib, sym)
    # Split a string column so that one segment gets all the strings, and the other gets only Nones and NaNs
    lib.write(sym, pd.DataFrame({"col": ["a", "b", "c", "d", "e", None, np.nan, np.nan, None, None]}))
    generic_compact_data_test(lib, sym, method_arg=5)


def test_compact_pickled_data(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory(segment_row_size=1000)
    sym = "test_compact_pickled_data"
    data = 100_000 * [0]
    lib.write(sym, data)
    assert lib.is_symbol_pickled(sym)
    generic_compact_data_test(lib, sym, 10_000)


def test_compact_recursively_normalized_data(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    lt = lib.library_tool()
    sym = "test_compact_recursively_normalized_data"
    data = {"a": pd.DataFrame({"col": [42]})}
    lib.write(sym, data, recursive_normalizers=True)
    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 1
    with pytest.raises(SchemaException) as e:
        lib.compact_data_experimental(sym)
    assert "recursive" in str(e.value) and sym in str(e.value)


def test_compact_sparse_data(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "test_compact_sparse_data"
    write_df = pd.DataFrame({"col": [0.5, np.nan]})
    append_df = pd.DataFrame({"col": [1.5]})
    lib.write(sym, write_df, sparsify_floats=True)
    lib.append(sym, append_df)
    with pytest.raises(SchemaException) as e:
        lib.compact_data_experimental(sym)
    assert "sparse" in str(e.value)


def test_compact_data_dynamic_schema_changing_types(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "test_compact_data_dynamic_schema_changing_types"
    write_df = pd.DataFrame({"col": np.arange(10, dtype=np.int32)})
    append_df = pd.DataFrame({"col": np.arange(10, dtype=np.int64)})
    lib.write(sym, write_df)
    lib.append(sym, append_df)
    with pytest.raises(SchemaException) as e:
        lib.compact_data_experimental(sym)
    assert "dynamic" in str(e.value)


def test_compact_data_dynamic_schema_changing_column_names(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "test_compact_data_dynamic_schema_changing_column_names"
    write_df = pd.DataFrame({"col1": np.arange(10, dtype=np.int64)})
    append_df = pd.DataFrame({"col2": np.arange(10, dtype=np.int64)})
    lib.write(sym, write_df)
    lib.append(sym, append_df)
    with pytest.raises(SchemaException) as e:
        lib.compact_data_experimental(sym)
    assert "dynamic" in str(e.value)


# We are more interested in the slicing than the data, so the parameters are for:
# - number of rows and columns
# - library slicing settings
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    # Making these parameters too large results in all the time being spent in numpy generating random numbers
    num_rows=st.integers(1, 2_000),
    num_cols=st.integers(1, 20),
    # The more interesting cases are when num_rows > rows_per_segment
    rows_per_segment=st.integers(1, 100),
    cols_per_segment=st.integers(1, 20),
)
def test_compact_data_hypothesis_general(
    in_memory_store_factory, clear_query_stats, num_rows, num_cols, rows_per_segment, cols_per_segment
):
    rng = np.random.default_rng(42)
    lib_sliced = in_memory_store_factory(
        column_group_size=cols_per_segment, segment_row_size=rows_per_segment, dynamic_strings=True, name="_unique_"
    )
    lib_unsliced = in_memory_store_factory(column_group_size=cols_per_segment, dynamic_strings=True, name="_unique_")
    sym = "test_compact_data_hypothesis_general"
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
        data[col_name] = arr
    df = pd.DataFrame(data)
    try:
        # Do one version where we write with the slicing policy and then compact
        lib_sliced.write(sym, df)
        generic_compact_data_test(lib_sliced, sym)
        # Do another version where we append random numbers of rows between 1 and 2 * rows_per_segment and then compact with
        # an explicit argument
        remaining_rows = num_rows
        while remaining_rows > 0:
            rows_to_take = rng.integers(1, 2 * rows_per_segment)
            lib_unsliced.append(sym, df[:rows_to_take])
            df = df[rows_to_take:]
            remaining_rows -= rows_to_take
        generic_compact_data_test(lib_unsliced, sym, rows_per_segment)
    except DuplicateKeyException:
        # On macOS the low timestamp resolution can cause duplicate keys when
        # compaction creates a new version within the same second as the write.
        # Skip this example instead of failing the whole test suite.
        # TODO: Fix the underlying issue and remove this workaround (monday ticket ref 11777175142)
        if not MACOS:
            raise
        assume(False)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    small_num_rows_0=st.integers(1, 10),
    small_num_rows_1=st.integers(1, 10),
    small_num_rows_2=st.integers(1, 10),
    large_num_rows_0=st.integers(150, 200),
    large_num_rows_1=st.integers(150, 200),
    large_num_rows_2=st.integers(150, 200),
)
def test_compact_data_hypothesis_small_and_large_segments(
    in_memory_store_factory,
    clear_query_stats,
    small_num_rows_0,
    small_num_rows_1,
    small_num_rows_2,
    large_num_rows_0,
    large_num_rows_1,
    large_num_rows_2,
):
    rng = np.random.default_rng(42)
    lib = in_memory_store_factory(segment_row_size=100, name="_unique_")
    sym = "test_compact_data_hypothesis_small_and_large_segments"
    try:
        # We will create small and large segments in the following order: S S L L S L
        lib.write(sym, pd.DataFrame({"col": rng.random(small_num_rows_0)}))
        lib.append(sym, pd.DataFrame({"col": rng.random(small_num_rows_1)}))
        lib.append(sym, pd.DataFrame({"col": rng.random(large_num_rows_0)}))
        lib.append(sym, pd.DataFrame({"col": rng.random(large_num_rows_1)}))
        lib.append(sym, pd.DataFrame({"col": rng.random(small_num_rows_2)}))
        lib.append(sym, pd.DataFrame({"col": rng.random(large_num_rows_2)}))
        generic_compact_data_test(lib, sym)
    except DuplicateKeyException:
        # TODO: Fix the underlying issue and remove this workaround (monday ticket ref 11777175142)
        if not MACOS:
            raise
        assume(False)
