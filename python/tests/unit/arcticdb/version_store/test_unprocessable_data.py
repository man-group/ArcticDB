"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file
licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb import ReadRequest
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.exceptions import ErrorCode, SchemaException

# Pipeline processing needs to reach individual columns. Pickled data, numpy arrays and recursively
# normalized data have no columns to reach, so every kind of processing must be refused for all three,
# with the dedicated error code rather than an incidental complaint from further down the pipeline.

TS = pd.date_range("2024-01-01", periods=6, freq="h")

PICKLED = "pickled"
NUMPY = "numpy"
RECURSIVE = "recursive"
DATA_KINDS = [PICKLED, NUMPY, RECURSIVE]

EXPECTED_CODE = {
    PICKLED: ErrorCode.E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA,
    NUMPY: ErrorCode.E_OPERATION_NOT_SUPPORTED_WITH_NUMPY_ARRAY,
    RECURSIVE: ErrorCode.E_OPERATION_NOT_SUPPORTED_WITH_RECURSIVE_NORMALIZED_DATA,
}

RECURSIVE_DATA = {"a": np.arange(5), "b": np.arange(8)}
MERGE_SOURCE = pd.DataFrame({"a": [99]}, index=[pd.Timestamp("2024-01-02")])


def write_unprocessable(lib, kind, symbol="sym"):
    nvs = lib._nvs
    if kind == PICKLED:
        nvs.write(symbol, pd.DataFrame({"a": [[i, i] for i in range(6)]}, index=TS), pickle_on_failure=True)
    elif kind == NUMPY:
        nvs.write(symbol, np.arange(6))
    elif kind == RECURSIVE:
        nvs.write(symbol, RECURSIVE_DATA, recursive_normalizers=True)
    else:
        raise AssertionError(f"unknown data kind {kind}")
    return symbol


def expect_refusal(kind):
    return pytest.raises(SchemaException, match=EXPECTED_CODE[kind].name)


def filter_query():
    q = QueryBuilder()
    return q[q["a"] == 0]


def projection_query():
    q = QueryBuilder()
    return q.apply("doubled", q["a"] * 2)


def groupby_agg_query():
    return QueryBuilder().groupby("g").agg({"a": "sum"})


def resample_query():
    return QueryBuilder().resample("2h").agg({"a": "sum"})


PROCESSING_KINDS = {
    "filter": filter_query,
    "projection": projection_query,
    "groupby_agg": groupby_agg_query,
    "resample": resample_query,
}

all_data_kinds = pytest.mark.parametrize("kind", DATA_KINDS)
all_processing_kinds = pytest.mark.parametrize("processing", list(PROCESSING_KINDS))


# --- read ---------------------------------------------------------------------------------------


@all_data_kinds
@all_processing_kinds
def test_read_with_query_builder(in_memory_library, kind, processing):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib.read(sym, query_builder=PROCESSING_KINDS[processing]())


@all_data_kinds
def test_read_columns(in_memory_library, kind):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib.read(sym, columns=["a"])


@all_data_kinds
def test_read_date_range(in_memory_library, kind):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib.read(sym, date_range=(TS[0], TS[-1]))


@all_data_kinds
def test_read_row_range(in_memory_library, kind):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib.read(sym, row_range=(0, 2))


@all_data_kinds
def test_head(in_memory_library, kind):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib.head(sym, n=2)


@all_data_kinds
def test_tail(in_memory_library, kind):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib.tail(sym, n=2)


@all_data_kinds
def test_lazy_read_then_collect(in_memory_library, kind):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    lazy_df = lib.read(sym, lazy=True)
    lazy_df = lazy_df[lazy_df["a"] == 0]
    with expect_refusal(kind):
        lazy_df.collect()


# --- batch read ---------------------------------------------------------------------------------


@all_data_kinds
@all_processing_kinds
def test_read_batch_returns_data_error_with_code(in_memory_library, kind, processing):
    """read_batch reports per-symbol failures as DataError rather than raising, so the error code has
    to survive the round trip into DataError.error_code."""
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    result = lib.read_batch([ReadRequest(sym, query_builder=PROCESSING_KINDS[processing]())])[0]
    assert result.error_code == EXPECTED_CODE[kind]


# --- multi-symbol join -------------------------------------------------------------------------


@all_data_kinds
@all_processing_kinds
def test_read_batch_and_join_per_symbol_processing(in_memory_library, kind, processing):
    """Clauses attached to a ReadRequest run per symbol, before the join."""
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib.read_batch_and_join(
            [ReadRequest(sym, query_builder=PROCESSING_KINDS[processing]())], QueryBuilder().concat()
        )


def test_concat_of_numpy_arrays_still_works(in_memory_library):
    """Joining numpy arrays with no processing on top is not processing, and must keep working."""
    lib = in_memory_library
    lib._nvs.write("np1", np.arange(4))
    lib._nvs.write("np2", np.arange(4, 8))
    result = lib.read_batch_and_join([ReadRequest("np1"), ReadRequest("np2")], QueryBuilder().concat())
    assert np.array_equal(result.data, np.arange(8))


# --- read-modify-write -------------------------------------------------------------------------


@all_data_kinds
@all_processing_kinds
def test_read_modify_write(in_memory_library, kind, processing):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib._nvs._read_modify_write(sym, query_builder=PROCESSING_KINDS[processing](), target_symbol="out")


# --- merge -------------------------------------------------------------------------------------


@all_data_kinds
def test_merge(in_memory_library, kind):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib.merge_experimental(sym, MERGE_SOURCE)


def test_merge_on_recursive_data_leaves_symbol_untouched(in_memory_library):
    """Regression: merging into a recursively normalized symbol used to succeed silently and replace
    the symbol contents, because the pipeline context never loaded the index and so reported 0 rows,
    which merge_update_impl read as 'the target is empty'."""
    lib = in_memory_library
    sym = write_unprocessable(lib, RECURSIVE)
    with expect_refusal(RECURSIVE):
        lib.merge_experimental(sym, MERGE_SOURCE)
    after = lib._nvs.read(sym).data
    assert set(after) == set(RECURSIVE_DATA)
    for column, expected in RECURSIVE_DATA.items():
        assert np.array_equal(after[column], expected)


# --- column stats ------------------------------------------------------------------------------


@all_data_kinds
def test_create_column_stats(in_memory_library, kind):
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    with expect_refusal(kind):
        lib._nvs.create_column_stats_experimental(sym)


# --- compaction must stay allowed ---------------------------------------------------------------


@pytest.mark.parametrize("kind", [PICKLED, NUMPY])
def test_compact_data_is_still_allowed(in_memory_library, kind):
    """compact_data goes through the same processing machinery, but must work on pickled data and
    numpy arrays. It is the one clause that is exempt from the unprocessable-data checks."""
    lib = in_memory_library
    sym = write_unprocessable(lib, kind)
    lib.compact_data(sym)
    lib.read(sym)
