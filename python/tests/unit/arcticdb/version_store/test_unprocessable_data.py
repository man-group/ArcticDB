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

from arcticdb import DataError, ReadRequest
from arcticdb.exceptions import ErrorCode, SchemaException
from arcticdb.version_store.library import Library
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import ErrorCategory

# Pipeline processing needs to reach individual columns. Pickled data, numpy arrays and recursively
# normalized data have no columns to reach, so every kind of processing must be refused for all three,
# with the dedicated error code rather than an incidental complaint from further down the pipeline.

pytestmark = pytest.mark.pipeline

ERROR_CODE_FOR = {
    "pickled": ErrorCode.E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA,
    "numpy": ErrorCode.E_OPERATION_NOT_SUPPORTED_WITH_NUMPY_ARRAY,
    "recursive": ErrorCode.E_OPERATION_NOT_SUPPORTED_WITH_RECURSIVE_NORMALIZED_DATA,
}

TS = pd.date_range("2024-01-01", periods=6, freq="h")
PICKLED_DATA = pd.DataFrame({"a": [[i, i] for i in range(6)]}, index=TS)
NUMPY_DATA = np.arange(6)
RECURSIVE_DATA = {"a": np.arange(5), "b": np.arange(8)}
MERGE_SOURCE = pd.DataFrame({"a": [99]}, index=[pd.Timestamp("2024-01-02")])


def write_unprocessable(nvs, kind, symbol="sym"):
    if kind == "pickled":
        nvs.write(symbol, PICKLED_DATA, pickle_on_failure=True)
    elif kind == "numpy":
        nvs.write(symbol, NUMPY_DATA)
    elif kind == "recursive":
        nvs.write(symbol, RECURSIVE_DATA, recursive_normalizers=True)
    else:
        raise AssertionError(f"unknown data kind {kind}")
    return symbol


def assert_read_back(kind, data):
    if kind == "pickled":
        assert data.to_dict() == PICKLED_DATA.to_dict()
    elif kind == "numpy":
        assert np.array_equal(data, NUMPY_DATA)
    elif kind == "recursive":
        assert set(data) == set(RECURSIVE_DATA)
        for column, expected in RECURSIVE_DATA.items():
            assert np.array_equal(data[column], expected)
    else:
        raise AssertionError(f"unknown data kind {kind}")


def expect_refusal(kind):
    return pytest.raises(SchemaException, match=ERROR_CODE_FOR[kind].name)


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

all_data_kinds = pytest.mark.parametrize("kind", list(ERROR_CODE_FOR))
all_processing_kinds = pytest.mark.parametrize("processing", list(PROCESSING_KINDS))


@pytest.fixture
def lib(in_memory_library_static_dynamic, any_output_format) -> Library:
    in_memory_library_static_dynamic._nvs._set_output_format_for_pipeline_tests(any_output_format)
    return in_memory_library_static_dynamic


# --- read ---------------------------------------------------------------------------------------


@all_data_kinds
@all_processing_kinds
def test_read_with_query_builder(lib, kind, processing):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib.read(sym, query_builder=PROCESSING_KINDS[processing]())


@all_processing_kinds
def test_read_with_query_builder_all_recursive_metastructure_versions(
    lib, processing, all_recursive_metastructure_versions
):
    sym = write_unprocessable(lib._nvs, "recursive")
    with expect_refusal("recursive"):
        lib.read(sym, query_builder=PROCESSING_KINDS[processing]())


@all_data_kinds
def test_read_columns(lib, kind):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib.read(sym, columns=["a"])


@all_data_kinds
def test_read_date_range(lib, kind):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib.read(sym, date_range=(TS[0], TS[-1]))


@all_data_kinds
def test_read_row_range(lib, kind):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib.read(sym, row_range=(0, 2))


@all_data_kinds
def test_head(lib, kind):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib.head(sym, n=2)


@all_data_kinds
def test_tail(lib, kind):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib.tail(sym, n=2)


@all_data_kinds
def test_lazy_read_then_collect(lib, kind):
    sym = write_unprocessable(lib._nvs, kind)
    lazy_df = lib.read(sym, lazy=True)
    lazy_df = lazy_df[lazy_df["a"] == 0]
    with expect_refusal(kind):
        lazy_df.collect()


@all_data_kinds
def test_plain_read_still_works(in_memory_library_static_dynamic, kind):
    lib = in_memory_library_static_dynamic
    sym = write_unprocessable(lib._nvs, kind)
    assert_read_back(kind, lib.read(sym).data)


# --- batch read ---------------------------------------------------------------------------------


@all_data_kinds
@all_processing_kinds
def test_read_batch_returns_data_error_with_code(lib, kind, processing):
    sym = write_unprocessable(lib._nvs, kind)
    result = lib.read_batch([ReadRequest(sym, query_builder=PROCESSING_KINDS[processing]())])[0]
    assert isinstance(result, DataError)
    assert result.error_code == ERROR_CODE_FOR[kind]
    assert result.error_category == ErrorCategory.SCHEMA


# --- multi-symbol join -------------------------------------------------------------------------


@all_data_kinds
@all_processing_kinds
def test_read_batch_and_join_per_symbol_processing(lib, kind, processing):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib.read_batch_and_join(
            [ReadRequest(sym, query_builder=PROCESSING_KINDS[processing]())], QueryBuilder().concat()
        )


def test_concat_of_numpy_arrays_still_works(in_memory_library_static_dynamic):
    lib = in_memory_library_static_dynamic
    lib._nvs.write("np1", np.arange(4))
    lib._nvs.write("np2", np.arange(4, 8))
    result = lib.read_batch_and_join([ReadRequest("np1"), ReadRequest("np2")], QueryBuilder().concat())
    assert np.array_equal(result.data, np.arange(8))


# --- read-modify-write -------------------------------------------------------------------------


@all_data_kinds
@all_processing_kinds
def test_read_modify_write(lib, kind, processing):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib._nvs._read_modify_write(sym, PROCESSING_KINDS[processing](), target_symbol="out")


# --- merge -------------------------------------------------------------------------------------


@all_data_kinds
def test_merge(lib, kind):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib.merge(sym, MERGE_SOURCE)


def test_merge_on_recursive_data_leaves_symbol_untouched(in_memory_library_static_dynamic):
    lib = in_memory_library_static_dynamic
    sym = write_unprocessable(lib._nvs, "recursive")
    with expect_refusal("recursive"):
        lib.merge(sym, MERGE_SOURCE)
    assert_read_back("recursive", lib._nvs.read(sym).data)


# --- column stats ------------------------------------------------------------------------------


@all_data_kinds
def test_create_column_stats(lib, kind):
    sym = write_unprocessable(lib._nvs, kind)
    with expect_refusal(kind):
        lib._nvs.create_column_stats_experimental(sym)
