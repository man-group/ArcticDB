"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import sys
import os

import pytz
from arcticdb_ext.exceptions import ErrorCode, ErrorCategory

from arcticdb.version_store import VersionedItem as PythonVersionedItem
from arcticdb_ext.storage import KeyType
from arcticdb_ext.version_store import VersionRequestType

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb import QueryBuilder, DataError

import pytest
import pandas as pd
from datetime import datetime, date, timezone, timedelta
import numpy as np
from numpy import datetime64
from arcticdb.util.test import (
    assert_frame_equal,
    random_strings_of_length,
    random_floats,
)
from arcticdb.util._versions import IS_PANDAS_TWO

import random

from azure.storage.blob import BlobServiceClient
from botocore.client import BaseClient as BotoClient
import time


from arcticdb.version_store.library import (
    WritePayload,
    WriteMetadataPayload,
    ArcticDuplicateSymbolsInBatchException,
    ArcticUnsupportedDataTypeException,
    ReadRequest,
    ReadInfoRequest,
    ArcticInvalidApiUsageException,
)


def generate_dataframe(columns, dt, num_days, num_rows_per_day):
    dataframes = []
    for _ in range(num_days):
        index = pd.Index([dt + timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in columns}
        new_df = pd.DataFrame(data=vals, index=index)
        dataframes.append(new_df)
        dt = dt + timedelta(days=1)
    return pd.concat(dataframes)


def test_write_meta_batch_with_as_ofs(arctic_library):
    lib = arctic_library
    num_symbols = 2
    num_versions = 5

    for sym in range(num_symbols):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        lib.write("sym_" + str(sym), df, metadata={"meta_" + str(sym): 0})

    for version in range(1, num_versions):
        write_requests = []
        for sym in range(num_symbols):
            write_requests.append(WriteMetadataPayload("sym_" + str(sym), {"meta_" + str(sym): version}))
        lib.write_metadata_batch(write_requests)

    read_requests = [
        ReadInfoRequest("sym_" + str(sym), as_of=version)
        for sym in range(num_symbols)
        for version in range(num_versions)
    ]
    results_list = lib.read_metadata_batch(read_requests)
    for sym in range(num_symbols):
        for version in range(num_versions):
            idx = sym * num_versions + version
            assert results_list[idx].metadata == {"meta_" + str(sym): version}


def test_write_metadata_batch_with_none(arctic_library):
    lib = arctic_library
    symbol = "symbol_"
    num_symbols = 2

    write_requests = []
    for sym in range(num_symbols):
        meta = {"meta_" + str(sym): sym}
        write_requests.append(WriteMetadataPayload(symbol + str(sym), meta))
    results_write = lib.write_metadata_batch(write_requests)
    for sym in range(num_symbols):
        assert results_write[sym].version == 0

    read_requests = [ReadInfoRequest(symbol + str(sym)) for sym in range(num_symbols)]
    results_meta_read = lib.read_metadata_batch(read_requests)
    for sym in range(num_symbols):
        assert results_meta_read[sym].data is None
        assert results_meta_read[sym].metadata == {"meta_" + str(sym): sym}
        assert results_meta_read[sym].version == 0

    read_requests = [ReadRequest(symbol + str(sym)) for sym in range(num_symbols)]
    results_read = lib.read_batch(read_requests)
    for sym in range(num_symbols):
        assert results_read[sym].data is None
        assert results_read[sym].metadata == {"meta_" + str(sym): sym}
        assert results_read[sym].version == 0


def test_read_meta_batch_with_as_ofs(arctic_library):
    lib = arctic_library

    # Given
    lib.write_pickle("sym1", 1, {"meta1": 0})
    lib.write_pickle("sym1", 1, {"meta1": 1})
    lib.write_pickle("sym2", 2, {"meta2": 0})
    lib.write_pickle("sym2", 2, {"meta2": 1})

    # When
    batch = lib.read_metadata_batch(
        [
            ReadInfoRequest("sym1", as_of=0),
            "sym1",
            ReadInfoRequest("sym2", as_of=0),
            "sym2",
        ]
    )

    # Then
    assert batch[0].metadata == {"meta1": 0}
    assert batch[1].metadata == {"meta1": 1}
    assert batch[2].metadata == {"meta2": 0}
    assert batch[3].metadata == {"meta2": 1}


def test_read_metadata_batch_with_none(arctic_library):
    lib = arctic_library

    # Given
    df1 = pd.DataFrame({"a": [5, 7, 9]})
    df2 = pd.DataFrame({"a": [7, 9, 11]})
    lib.write("s1", df1)
    lib.write("s2", df2)

    # When
    batch = lib.read_metadata_batch(["s1", "s2"])

    # Then
    assert batch[0].data is None
    assert batch[0].metadata is None
    assert batch[0].version == 0

    assert batch[1].data is None
    assert batch[1].metadata is None
    assert batch[1].version == 0


def test_read_metadata_batch_missing_keys(arctic_library):
    lib = arctic_library

    # Given
    df1 = pd.DataFrame({"a": [3, 5, 7]})
    lib.write("s1", df1, metadata={"meta1": 0})
    # Need two versions for this symbol as we're going to delete a version key, and the optimisation of storing the
    # latest index key in the version ref key means it will still work if we just write one version key and then delete
    # it
    df2 = pd.DataFrame({"a": [5, 7, 9]})
    lib.write("s2", df2, metadata={"meta2": 0})
    lib.write("s2", df2, metadata={"meta2": 1})
    df3 = pd.DataFrame({"a": [7, 9, 11]})
    lib.write("s3", df3, metadata={"meta3": 0})

    lib_tool = lib._nvs.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "s2")
    s2_key_to_delete = [key for key in s2_version_keys if key.version_id == 0][0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_key_to_delete)

    # When
    batch = lib.read_metadata_batch(["s1", ReadInfoRequest("s2", as_of=0), "s3"])

    # Then
    assert isinstance(batch[0], DataError)
    assert batch[0].symbol == "s1"
    assert batch[0].version_request_type == VersionRequestType.LATEST
    assert batch[0].version_request_data is None
    assert batch[0].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[0].error_category == ErrorCategory.STORAGE

    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s2"
    assert batch[1].version_request_type == VersionRequestType.SPECIFIC
    assert batch[1].version_request_data == 0
    assert batch[1].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[1].error_category == ErrorCategory.STORAGE

    assert not isinstance(batch[2], DataError)
    assert batch[2].metadata == {"meta3": 0}


def test_read_metadata_batch_symbol_doesnt_exist(arctic_library):
    lib = arctic_library

    # Given
    df = pd.DataFrame({"a": [3, 5, 7]})
    lib.write("s1", df, {"meta1": 0})

    # When
    batch = lib.read_metadata_batch(["s1", "s2"])

    # Then
    assert not isinstance(batch[0], DataError)
    assert batch[0].metadata == {"meta1": 0}

    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s2"
    assert batch[1].version_request_type == VersionRequestType.LATEST
    assert batch[1].version_request_data == None
    assert batch[1].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[1].error_category == ErrorCategory.MISSING_DATA


def test_read_metadata_batch_version_doesnt_exist(arctic_library):
    lib = arctic_library

    # Given
    df = pd.DataFrame({"a": [3, 5, 7]})
    lib.write("s1", df, {"meta1": 0})

    # When
    batch = lib.read_metadata_batch([ReadInfoRequest("s1", as_of=0), ReadInfoRequest("s1", as_of=1)])

    # Then
    assert not isinstance(batch[0], DataError)
    assert batch[0].metadata == {"meta1": 0}

    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s1"
    assert batch[1].version_request_type == VersionRequestType.SPECIFIC
    assert batch[1].version_request_data == 1
    assert batch[1].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[1].error_category == ErrorCategory.MISSING_DATA


class A:
    """A dummy user defined type that requires pickling to serialize."""

    def __init__(self, id: str):
        self.id = id

    def __eq__(self, other):
        return self.id == other.id


def test_write_batch_with_pickle_mode(arctic_library):
    """Writing in pickle mode should succeed when the user uses the dedicated method."""
    lib = arctic_library
    lib.write_pickle_batch(
        [WritePayload("test_1", A("id_1")), WritePayload("test_2", A("id_2"), metadata="the metadata")]
    )
    assert lib["test_1"].data.id == "id_1"
    assert lib["test_1"].version == 0
    assert lib["test_2"].data.id == "id_2"
    assert lib["test_2"].metadata == "the metadata"


def test_write_object_in_batch_without_pickle_mode(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    with pytest.raises(ArcticUnsupportedDataTypeException) as e:
        lib.write_batch([WritePayload("test_1", A("id_1"))])
    # omit the part with the full class path as that will change in arcticdb
    assert e.value.args[0].startswith(
        "payload contains some data of types that cannot be normalized. Consider using write_pickle_batch instead."
        " symbols with bad datatypes"
    )


def test_write_object_in_batch_without_pickle_mode_many_symbols(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    with pytest.raises(ArcticUnsupportedDataTypeException) as e:
        lib.write_batch([WritePayload(f"test_{i}", A(f"id_{i}")) for i in range(10)])
    message: str = e.value.args[0]
    assert "(and more)... 10 data in total have bad types." in message


def test_write_batch_duplicate_symbols(arctic_library):
    """Should throw and not write if duplicate symbols are provided."""
    lib = arctic_library
    with pytest.raises(ArcticDuplicateSymbolsInBatchException):
        lib.write_batch(
            [
                WritePayload("symbol_1", pd.DataFrame()),
                WritePayload("symbol_1", pd.DataFrame(), metadata="great_metadata"),
            ]
        )

    assert not lib.list_symbols()


def test_write_pickle_batch_duplicate_symbols(arctic_library):
    """Should throw and not write if duplicate symbols are provided."""
    lib = arctic_library
    with pytest.raises(ArcticDuplicateSymbolsInBatchException):
        lib.write_pickle_batch(
            [
                WritePayload("symbol_1", pd.DataFrame()),
                WritePayload("symbol_1", pd.DataFrame(), metadata="great_metadata"),
            ]
        )

    assert not lib.list_symbols()


def test_write_batch(library_factory):
    """Should be able to write different size of batch of data."""
    lib = library_factory(LibraryOptions(rows_per_segment=10))
    assert lib._nvs._lib_cfg.lib_desc.version.write_options.segment_row_size == 10
    num_days = 40
    num_symbols = 2
    dt = datetime(2019, 4, 8, 0, 0, 0)
    column_length = 4
    num_columns = 5
    num_rows_per_day = 1
    write_requests = []
    read_requests = []
    list_dataframes = {}
    columns = random_strings_of_length(num_columns, num_columns, True)
    for sym in range(num_symbols):
        df = generate_dataframe(random.sample(columns, num_columns), dt, num_days, num_rows_per_day)
        write_requests.append(WritePayload("symbol_" + str(sym), df, metadata="great_metadata" + str(sym)))
        read_requests.append("symbol_" + str(sym))
        list_dataframes[sym] = df

    write_batch_result = lib.write_batch(write_requests)
    assert all(type(w) == PythonVersionedItem for w in write_batch_result)

    read_batch_result = lib.read_batch(read_requests)
    for sym in range(num_symbols):
        original_dataframe = list_dataframes[sym]
        assert read_batch_result[sym].metadata == "great_metadata" + str(sym)
        assert_frame_equal(read_batch_result[sym].data, original_dataframe)


def test_write_batch_dedup(library_factory):
    """Should be able to write different size of batch of data reusing deduplicated data from previous versions."""
    lib = library_factory(LibraryOptions(rows_per_segment=10, dedup=True))
    assert lib._nvs._lib_cfg.lib_desc.version.write_options.segment_row_size == 10
    assert lib._nvs._lib_cfg.lib_desc.version.write_options.de_duplication == True
    num_days = 40
    num_symbols = 2
    num_versions = 4
    dt = datetime(2019, 4, 8, 0, 0, 0)
    column_length = 4
    num_columns = 5
    num_rows_per_day = 1
    read_requests = []
    list_dataframes = {}
    columns = random_strings_of_length(num_columns, num_columns, True)
    df = generate_dataframe(random.sample(columns, num_columns), dt, num_days, num_rows_per_day)
    for v in range(num_versions):
        write_requests = []
        for sym in range(num_symbols):
            write_requests.append(WritePayload("symbol_" + str(sym), df, metadata="great_metadata" + str(v)))
            read_requests.append("symbol_" + str(sym))
            list_dataframes[sym] = df
        write_batch_result = lib.write_batch(write_requests)
        assert all(type(w) == PythonVersionedItem for w in write_batch_result)

    read_batch_result = lib.read_batch(read_requests)
    for sym in range(num_symbols):
        original_dataframe = list_dataframes[sym]
        assert read_batch_result[sym].metadata == "great_metadata" + str(num_versions - 1)
        assert_frame_equal(read_batch_result[sym].data, original_dataframe)

    num_segments = int(
        (num_days * num_rows_per_day) / lib._nvs._lib_cfg.lib_desc.version.write_options.segment_row_size
    )
    for sym in range(num_symbols):
        data_key_version = lib._nvs.read_index("symbol_" + str(sym))["version_id"]
        for s in range(num_segments):
            assert data_key_version[s] == 0


def test_write_batch_missing_keys_dedup(library_factory):
    """When there is duplicate data to reuse for the current write, we need to access the index key of the previous
    versions in order to refer to the corresponding keys for the deduplicated data."""
    lib = library_factory(LibraryOptions(dedup=True))
    assert lib._nvs._lib_cfg.lib_desc.version.write_options.de_duplication == True

    num_days = 2
    num_rows_per_day = 1

    # Given
    dt = datetime(2019, 4, 8, 0, 0, 0)
    df1 = generate_dataframe(["a", "b", "c"], dt, num_days, num_rows_per_day)
    df2 = generate_dataframe(["a", "b", "c"], dt, num_days, num_rows_per_day)
    lib.write("s1", df1)
    lib.write("s2", df2)

    lib_tool = lib._nvs.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    lib_tool.remove(s1_index_key)

    # When
    batch = lib.write_batch(
        [
            WritePayload("s1", df1, metadata="great_metadata_s1"),
            WritePayload("s2", df2, metadata="great_metadata_s2"),
        ]
    )

    # Then
    assert isinstance(batch[0], DataError)
    assert batch[0].symbol == "s1"
    assert batch[0].version_request_type is None
    assert batch[0].version_request_data is None
    assert batch[0].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[0].error_category == ErrorCategory.STORAGE

    assert not isinstance(batch[1], DataError)
    read_dataframe = lib.read("s2")
    assert read_dataframe.metadata == "great_metadata_s2"
    assert_frame_equal(read_dataframe.data, df2)


def test_append_batch(library_factory):
    lib = library_factory(LibraryOptions(rows_per_segment=10))
    assert lib._nvs._lib_cfg.lib_desc.version.write_options.segment_row_size == 10
    num_days = 50
    num_symbols = 2
    dt = datetime(2019, 4, 8, 0, 0, 0)
    num_rows_per_day = 1

    # Given
    list_append_requests = []
    list_dataframes = {}
    for sym in range(num_symbols):
        df = generate_dataframe(["a", "b", "c"], dt, num_days, num_rows_per_day)
        list_append_requests.append(WritePayload("symbol_" + str(sym), df, metadata="great_metadata" + str(sym)))
        list_dataframes[sym] = df

    # When a symbol doesn't exist, we expect it to be created. In effect, append_batch functions as write_batch
    batch = lib.append_batch(list_append_requests)
    assert all(type(w) == PythonVersionedItem for w in batch)

    # Then
    for sym in range(num_symbols):
        original_dataframe = list_dataframes[sym]
        read_dataframe = lib.read("symbol_" + str(sym))
        assert read_dataframe.metadata == "great_metadata" + str(sym)
        assert_frame_equal(read_dataframe.data, original_dataframe)

    # Given
    dt = datetime(2020, 4, 8, 0, 0, 0)
    list_append_requests = []
    for sym in range(num_symbols):
        df = generate_dataframe(["a", "b", "c"], dt, num_days, num_rows_per_day)
        list_append_requests.append(WritePayload("symbol_" + str(sym), df, metadata="great_metadata" + str(sym)))
        list_dataframes[sym] = pd.concat([list_dataframes[sym], df])

    # When the symbol already exists, we expect the current dataframe to be appended to the previous dataframe
    batch = lib.append_batch(list_append_requests)
    assert all(type(w) == PythonVersionedItem for w in batch)

    # Then
    for sym in range(num_symbols):
        original_dataframe = list_dataframes[sym]
        read_dataframe = lib.read("symbol_" + str(sym))
        assert read_dataframe.metadata == "great_metadata" + str(sym)
        assert_frame_equal(read_dataframe.data, original_dataframe)


def test_append_batch_missing_keys(arctic_library):
    lib = arctic_library

    num_days = 2
    num_rows_per_day = 1

    # Given
    dt = datetime(2019, 4, 8, 0, 0, 0)
    df1_write = generate_dataframe(["a", "b", "c"], dt, num_days, num_rows_per_day)
    df2_write = generate_dataframe(["a", "b", "c"], dt, num_days, num_rows_per_day)
    lib.write("s1", df1_write)
    lib.write("s2", df2_write)

    lib_tool = lib._nvs.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    lib_tool.remove(s1_index_key)

    # When
    dt = datetime(2020, 4, 8, 0, 0, 0)
    df1_append = generate_dataframe(["a", "b", "c"], dt, num_days, num_rows_per_day)
    df2_append = generate_dataframe(["a", "b", "c"], dt, num_days, num_rows_per_day)
    batch = lib.append_batch(
        [
            WritePayload("s1", df1_append, metadata="great_metadata_s1"),
            WritePayload("s2", df2_append, metadata="great_metadata_s2"),
        ]
    )

    # Then
    assert isinstance(batch[0], DataError)
    assert batch[0].symbol == "s1"
    assert batch[0].version_request_type is None
    assert batch[0].version_request_data is None
    assert batch[0].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[0].error_category == ErrorCategory.STORAGE

    assert not isinstance(batch[1], DataError)
    read_dataframe = lib.read("s2")
    assert read_dataframe.metadata == "great_metadata_s2"
    assert_frame_equal(read_dataframe.data, pd.concat([df2_write, df2_append]))


def test_read_batch_time_stamp(arctic_library):
    """Should be able to read data in batch mode using a timestamp."""
    lib = arctic_library
    sym = "sym_"
    num_versions = 3
    num_symbols = 3
    for v_num in range(num_versions):
        write_requests = [
            WritePayload(sym + str(sym_num), pd.DataFrame({"col": [sym_num + v_num, sym_num * v_num, sym_num - v_num]}))
            for sym_num in range(num_symbols)
        ]
        lib.write_batch(write_requests)

    microsecond_delta = timedelta(microseconds=1)

    requests_batch = [
        ReadRequest(sym + str(sym_num), as_of=version_info.date + microsecond_delta)
        for sym_num in range(num_symbols)
        for key, version_info in lib.list_versions(sym + str(sym_num)).items()
    ]
    original_dataframes = [
        pd.DataFrame(
            {"col": [sym_num + version_info.version, sym_num * version_info.version, sym_num - version_info.version]}
        )
        for sym_num in range(num_symbols)
        for version_info in lib.list_versions(sym + str(sym_num))
    ]

    data_batch = lib.read_batch(requests_batch)
    for d1, d2 in zip(data_batch, original_dataframes):
        assert_frame_equal(d1.data, d2)


def test_read_batch_mixed_request_supported(arctic_library):
    lib = arctic_library

    # Given
    lib.write("s1", pd.DataFrame())
    s2_frame = pd.DataFrame({"col": [1, 2, 3]})
    lib.write("s2", s2_frame)
    s3_frame = pd.DataFrame({"col_2": [4, 5, 6]})
    lib.write("s3", s3_frame)

    # When - note the batch has a mix of symbols and read requests
    batch = lib.read_batch(["s1", "s2", ReadRequest("s3"), "s1"])  # duplicates are fine

    # Then
    assert [vi.symbol for vi in batch] == ["s1", "s2", "s3", "s1"]
    assert batch[0].data.empty
    assert_frame_equal(s2_frame, batch[1].data)
    assert_frame_equal(s3_frame, batch[2].data)
    assert batch[3].data.empty


def test_read_batch_with_columns(arctic_library):
    lib = arctic_library

    # Given
    lib.write("s", pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}))

    # When
    batch = lib.read_batch([ReadRequest("s", columns=["B", "C"])])

    # Then
    assert_frame_equal(pd.DataFrame({"B": [4, 5, 6], "C": [7, 8, 9]}), batch[0].data)


def test_read_batch_overall_query_builder(arctic_library):
    lib = arctic_library

    # Given
    q = QueryBuilder()
    q = q[q["a"] < 5]
    lib.write("s1", pd.DataFrame({"a": [3, 5, 7]}))
    lib.write("s2", pd.DataFrame({"a": [4, 6, 8]}))
    # When
    batch = lib.read_batch(["s1", "s2"], query_builder=q)
    # Then
    assert_frame_equal(batch[0].data, pd.DataFrame({"a": [3]}))
    assert_frame_equal(batch[1].data, pd.DataFrame({"a": [4]}))


def test_read_batch_per_symbol_query_builder(arctic_library):
    lib = arctic_library

    # Given
    q_1 = QueryBuilder()
    q_1 = q_1[q_1["a"] < 5]
    q_2 = QueryBuilder()
    q_2 = q_2[q_2["a"] < 7]
    lib.write("s1", pd.DataFrame({"a": [3, 5, 7]}))
    lib.write("s2", pd.DataFrame({"a": [4, 6, 8]}))
    # When
    batch = lib.read_batch([ReadRequest("s1", query_builder=q_1), ReadRequest("s2", query_builder=q_2)])
    # Then
    assert_frame_equal(batch[0].data, pd.DataFrame({"a": [3]}))
    assert_frame_equal(batch[1].data, pd.DataFrame({"a": [4, 6]}))


def test_read_batch_as_of(arctic_library):
    lib = arctic_library

    # Given
    lib.write("s1", pd.DataFrame({"col": [1, 2, 3]}))  # v0
    lib.write("s1", pd.DataFrame(), prune_previous_versions=False)  # v1

    # When
    batch = lib.read_batch(["s1", ReadRequest("s1", as_of=0)])

    # Then
    assert batch[0].data.empty
    assert not batch[1].data.empty
    assert type(batch[1]) == PythonVersionedItem


def test_batch_methods_with_negative_as_of(arctic_library):
    lib = arctic_library
    sym = "test_batch_methods_with_negative_as_of"
    data_0 = 0
    data_1 = 1
    metadata_0 = {"some": "metadata"}
    metadata_1 = {"more": "metadata"}
    lib.write_pickle(sym, data_0, metadata=metadata_0)
    lib.write_pickle(sym, data_1, metadata=metadata_1)
    res = lib.read_batch([ReadRequest(sym, as_of=-1), ReadRequest(sym, as_of=-2)])
    assert res[0].data == data_1
    assert res[1].data == data_0

    res = lib.read_metadata_batch([ReadInfoRequest(sym, as_of=-1), ReadInfoRequest(sym, as_of=-2)])
    assert res[0].metadata == metadata_1
    assert res[1].metadata == metadata_0

    res = lib.get_description_batch([ReadInfoRequest(sym, as_of=-1), ReadInfoRequest(sym, as_of=-2)])
    assert res[0] == lib.get_description(sym)
    assert res[1] == lib.get_description(sym, as_of=0)


def test_read_batch_date_ranges(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    batch = lib.read_batch(
        [
            ReadRequest("symbol", date_range=(datetime(2018, 1, 1), datetime(2018, 1, 2))),
            ReadRequest("symbol", date_range=(datetime(2018, 1, 1), datetime(2018, 1, 3))),
        ]
    )

    assert_frame_equal(
        batch[0].data, pd.DataFrame({"column": [1, 2]}, index=pd.date_range(start="1/1/2018", end="1/2/2018"))
    )
    assert_frame_equal(
        batch[1].data, pd.DataFrame({"column": [1, 2, 3]}, index=pd.date_range(start="1/1/2018", end="1/3/2018"))
    )


def test_read_batch_date_ranges_dates_not_times(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    batch = lib.read_batch(
        [
            ReadRequest("symbol", date_range=(date(2018, 1, 1), date(2018, 1, 2))),
            ReadRequest("symbol", date_range=(date(2018, 1, 1), date(2018, 1, 3))),
        ]
    )

    assert_frame_equal(
        batch[0].data, pd.DataFrame({"column": [1, 2]}, index=pd.date_range(start="1/1/2018", end="1/2/2018"))
    )
    assert_frame_equal(
        batch[1].data, pd.DataFrame({"column": [1, 2, 3]}, index=pd.date_range(start="1/1/2018", end="1/3/2018"))
    )


def test_read_batch_overall_query_builder_and_per_request_query_builder_raises(arctic_library):
    lib = arctic_library

    # Given
    q_1 = QueryBuilder()
    q_1 = q_1[q_1["a"] < 5]
    q_2 = QueryBuilder()
    q_2 = q_2[q_2["a"] < 7]
    lib.write("s", pd.DataFrame({"a": [3, 5, 7]}))
    # When & Then
    with pytest.raises(ArcticInvalidApiUsageException):
        lib.read_batch([ReadRequest("s", query_builder=q_1)], query_builder=q_2)


def test_read_batch_unhandled_type(arctic_library):
    """Only str and ReadRequest are supported."""
    lib = arctic_library
    lib.write("1", pd.DataFrame())
    with pytest.raises(ArcticInvalidApiUsageException):
        lib.read_batch([1])


def test_read_batch_symbol_doesnt_exist(arctic_library):
    lib = arctic_library

    # Given
    df = pd.DataFrame({"a": [3, 5, 7]})
    lib.write("s1", df)
    # When
    batch = lib.read_batch(["s1", "s2"])
    # Then
    assert_frame_equal(batch[0].data, df)
    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s2"
    assert batch[1].version_request_type == VersionRequestType.LATEST
    assert batch[1].version_request_data == None
    assert batch[1].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[1].error_category == ErrorCategory.MISSING_DATA


def test_read_batch_version_doesnt_exist(arctic_library):
    lib = arctic_library

    # Given
    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    lib.write("s1", df1)
    lib.write("s2", df2)
    # When
    batch = lib.read_batch([ReadRequest("s1", as_of=0), ReadRequest("s1", as_of=1), ReadRequest("s2", as_of=1)])
    # Then
    assert_frame_equal(batch[0].data, df1)
    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s1"
    assert batch[1].version_request_type == VersionRequestType.SPECIFIC
    assert batch[1].version_request_data == 1
    assert batch[1].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[1].error_category == ErrorCategory.MISSING_DATA

    assert isinstance(batch[2], DataError)
    assert batch[2].symbol == "s2"
    assert batch[2].version_request_type == VersionRequestType.SPECIFIC
    assert batch[2].version_request_data == 1
    assert batch[2].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[2].error_category == ErrorCategory.MISSING_DATA


def test_read_batch_missing_keys(arctic_library):
    lib = arctic_library

    # Given
    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    df3 = pd.DataFrame({"a": [5, 7, 9]})
    lib.write("s1", df1)
    lib.write("s2", df2)
    # Need two versions for this symbol as we're going to delete a version key, and the optimisation of storing the
    # latest index key in the version ref key means it will still work if we just write one version key and then delete
    # it
    lib.write("s3", df3)
    lib.write("s3", df3)
    lib_tool = lib._nvs.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_data_key = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, "s2")[0]
    s3_version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "s3")
    s3_key_to_delete = [key for key in s3_version_keys if key.version_id == 0][0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_data_key)
    lib_tool.remove(s3_key_to_delete)
    # When
    batch = lib.read_batch(["s1", "s2", ReadRequest("s3", as_of=0)])
    # Then
    assert isinstance(batch[0], DataError)
    assert batch[0].symbol == "s1"
    assert batch[0].version_request_type == VersionRequestType.LATEST
    assert batch[0].version_request_data is None
    assert batch[0].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[0].error_category == ErrorCategory.STORAGE

    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s2"
    assert batch[1].version_request_type == VersionRequestType.LATEST
    assert batch[1].version_request_data is None
    assert batch[1].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[1].error_category == ErrorCategory.STORAGE

    assert isinstance(batch[2], DataError)
    assert batch[2].symbol == "s3"
    assert batch[2].version_request_type == VersionRequestType.SPECIFIC
    assert batch[2].version_request_data == 0
    assert batch[2].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[2].error_category == ErrorCategory.STORAGE


def test_write_metadata_batch_missing_keys(arctic_library):
    lib = arctic_library

    # Given
    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    lib.write("s1", df1)
    lib.write("s2", df2)

    lib_tool = lib._nvs.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s2")[0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_index_key)
    # When
    batch = lib.write_metadata_batch(
        [WriteMetadataPayload("s1", {"s1_meta": 1}), WriteMetadataPayload("s2", {"s2_meta": 1})]
    )
    # Then
    assert isinstance(batch[0], DataError)
    assert batch[0].symbol == "s1"
    assert batch[0].version_request_type is None
    assert batch[0].version_request_data is None
    assert batch[0].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[0].error_category == ErrorCategory.STORAGE

    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s2"
    assert batch[1].version_request_type is None
    assert batch[1].version_request_data is None
    assert batch[1].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[1].error_category == ErrorCategory.STORAGE


def test_read_batch_query_builder_missing_keys(arctic_library):
    lib = arctic_library

    # Given
    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    df3 = pd.DataFrame({"a": [5, 7, 9]})
    lib.write("s1", df1)
    lib.write("s2", df2)
    # Need two versions for this symbol as we're going to delete a version key, and the optimisation of storing the
    # latest index key in the version ref key means it will still work if we just write one version key and then delete
    # it
    lib.write("s3", df3)
    lib.write("s3", df3)
    lib_tool = lib._nvs.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_data_key = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, "s2")[0]
    s3_version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "s3")
    s3_key_to_delete = [key for key in s3_version_keys if key.version_id == 0][0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_data_key)
    lib_tool.remove(s3_key_to_delete)
    q = QueryBuilder()
    q = q[q["a"] < 5]
    # When
    batch = lib.read_batch(["s1", "s2", ReadRequest("s3", as_of=0)], query_builder=q)
    # Then
    assert isinstance(batch[0], DataError)
    assert batch[0].symbol == "s1"
    assert batch[0].version_request_type == VersionRequestType.LATEST
    assert batch[0].version_request_data is None
    assert batch[0].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[0].error_category == ErrorCategory.STORAGE

    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s2"
    assert batch[1].version_request_type == VersionRequestType.LATEST
    assert batch[1].version_request_data is None
    assert batch[1].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[1].error_category == ErrorCategory.STORAGE

    assert isinstance(batch[2], DataError)
    assert batch[2].symbol == "s3"
    assert batch[2].version_request_type == VersionRequestType.SPECIFIC
    assert batch[2].version_request_data == 0
    assert batch[2].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[2].error_category == ErrorCategory.STORAGE


def test_get_description_batch_missing_keys(arctic_library):
    lib = arctic_library

    # Given
    df1 = pd.DataFrame({"a": [3, 5, 7]}, index=pd.date_range(start="1/1/2018", end="1/3/2018"))
    df2 = pd.DataFrame({"a": [5, 7, 9]}, index=pd.date_range(start="1/1/2018", end="1/3/2018"))
    df3 = pd.DataFrame({"a": [7, 9, 11]}, index=pd.date_range(start="1/1/2018", end="1/3/2018"))
    df3.index.rename("named_index", inplace=True)

    lib.write("s1", df1)
    # Need two versions for this symbol as we're going to delete a version key, and the optimisation of storing the
    # latest index key in the version ref key means it will still work if we just write one version key and then delete
    # it
    lib.write("s2", df2)
    lib.write("s2", df2)
    lib.write("s3", df3)
    lib_tool = lib._nvs.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "s2")
    s2_key_to_delete = [key for key in s2_version_keys if key.version_id == 0][0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_key_to_delete)

    # When
    batch = lib.get_description_batch(["s1", ReadInfoRequest("s2", as_of=0), "s3"])

    # Then
    assert isinstance(batch[0], DataError)
    assert batch[0].symbol == "s1"
    assert batch[0].version_request_type == VersionRequestType.LATEST
    assert batch[0].version_request_data is None
    assert batch[0].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[0].error_category == ErrorCategory.STORAGE

    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s2"
    assert batch[1].version_request_type == VersionRequestType.SPECIFIC
    assert batch[1].version_request_data == 0
    assert batch[1].error_code == ErrorCode.E_KEY_NOT_FOUND
    assert batch[1].error_category == ErrorCategory.STORAGE

    assert not isinstance(batch[2], DataError)
    assert batch[2].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2018, 1, 1), datetime(2018, 1, 3)),
        )
    )
    assert [c[0] for c in batch[2].columns] == ["a"]
    assert batch[2].index[0] == ["named_index"]
    assert batch[2].index_type == "index"
    assert batch[2].row_count == 3


def test_get_description_batch_symbol_doesnt_exist(arctic_library):
    lib = arctic_library

    # Given
    df = pd.DataFrame({"a": [3, 5, 7, 9]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    df.index.rename("named_index", inplace=True)
    lib.write("s1", df)

    # When
    batch = lib.get_description_batch(["s1", "s2"])

    # Then
    assert not isinstance(batch[0], DataError)
    assert batch[0].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2018, 1, 1), datetime(2018, 1, 4)),
        )
    )
    assert [c[0] for c in batch[0].columns] == ["a"]
    assert batch[0].index[0] == ["named_index"]
    assert batch[0].index_type == "index"
    assert batch[0].row_count == 4

    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s2"
    assert batch[1].version_request_type == VersionRequestType.LATEST
    assert batch[1].version_request_data == None
    assert batch[1].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[1].error_category == ErrorCategory.MISSING_DATA


def test_get_description_batch_version_doesnt_exist(arctic_library):
    lib = arctic_library

    # Given
    df1 = pd.DataFrame({"a": [3, 5, 7, 9]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    df1.index.rename("named_index", inplace=True)
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    lib.write("s1", df1)
    lib.write("s2", df2)

    # When
    batch = lib.get_description_batch(
        [ReadInfoRequest("s1", as_of=0), ReadInfoRequest("s1", as_of=1), ReadInfoRequest("s2", as_of=1)]
    )

    # Then
    assert not isinstance(batch[0], DataError)
    assert batch[0].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2018, 1, 1), datetime(2018, 1, 4)),
        )
    )
    assert [c[0] for c in batch[0].columns] == ["a"]
    assert batch[0].index[0] == ["named_index"]
    assert batch[0].index_type == "index"
    assert batch[0].row_count == 4

    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s1"
    assert batch[1].version_request_type == VersionRequestType.SPECIFIC
    assert batch[1].version_request_data == 1
    assert batch[1].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[1].error_category == ErrorCategory.MISSING_DATA

    assert isinstance(batch[2], DataError)
    assert batch[2].symbol == "s2"
    assert batch[2].version_request_type == VersionRequestType.SPECIFIC
    assert batch[2].version_request_data == 1
    assert batch[2].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[2].error_category == ErrorCategory.MISSING_DATA


def test_read_batch_query_builder_symbol_doesnt_exist(arctic_library):
    lib = arctic_library

    # Given
    q = QueryBuilder()
    q = q[q["a"] < 5]
    lib.write("s1", pd.DataFrame({"a": [3, 5, 7]}))
    # When
    batch = lib.read_batch(["s1", "s2"], query_builder=q)
    # Then
    assert_frame_equal(batch[0].data, pd.DataFrame({"a": [3]}))
    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s2"
    assert batch[1].version_request_type == VersionRequestType.LATEST
    assert batch[1].version_request_data == None
    assert batch[1].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[1].error_category == ErrorCategory.MISSING_DATA


def test_read_batch_query_builder_version_doesnt_exist(arctic_library):
    lib = arctic_library

    # Given
    q = QueryBuilder()
    q = q[q["a"] < 5]
    lib.write("s1", pd.DataFrame({"a": [3, 5, 7]}))
    lib.write("s2", pd.DataFrame({"a": [4, 6, 8]}))
    # When
    batch = lib.read_batch(
        [ReadRequest("s1", as_of=0), ReadRequest("s1", as_of=1), ReadRequest("s2", as_of=1)], query_builder=q
    )
    # Then
    assert_frame_equal(batch[0].data, pd.DataFrame({"a": [3]}))
    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == "s1"
    assert batch[1].version_request_type == VersionRequestType.SPECIFIC
    assert batch[1].version_request_data == 1
    assert batch[1].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[1].error_category == ErrorCategory.MISSING_DATA

    assert isinstance(batch[2], DataError)
    assert batch[2].symbol == "s2"
    assert batch[2].version_request_type == VersionRequestType.SPECIFIC
    assert batch[2].version_request_data == 1
    assert batch[2].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert batch[2].error_category == ErrorCategory.MISSING_DATA


def test_get_description_batch(arctic_library):
    lib = arctic_library

    # given
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    df.index.rename("named_index", inplace=True)
    lib.write("symbol1", df)
    to_append_df = pd.DataFrame({"column": [5, 6]}, index=pd.date_range(start="1/5/2018", end="1/6/2018"))
    to_append_df.index.rename("named_index", inplace=True)
    lib.append("symbol1", to_append_df)

    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2019", end="1/4/2019"))
    df.index.rename("named_index", inplace=True)
    lib.write("symbol2", df)
    to_append_df = pd.DataFrame({"column": [5, 6]}, index=pd.date_range(start="1/5/2019", end="1/6/2019"))
    to_append_df.index.rename("named_index", inplace=True)
    lib.append("symbol2", to_append_df)

    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2020", end="1/4/2020"))
    df.index.rename("named_index", inplace=True)
    lib.write("symbol3", df)
    to_append_df = pd.DataFrame({"column": [5, 6]}, index=pd.date_range(start="1/5/2020", end="1/6/2020"))
    to_append_df.index.rename("named_index", inplace=True)
    lib.append("symbol3", to_append_df)
    # when
    infos = lib.get_description_batch(["symbol1", "symbol2", "symbol3"])
    original_infos = lib.get_description_batch(
        [ReadInfoRequest("symbol1", as_of=0), ReadInfoRequest("symbol2", as_of=0), ReadInfoRequest("symbol3", as_of=0)]
    )

    assert infos[0].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2018, 1, 1), datetime(2018, 1, 6)),
        )
    )
    assert infos[1].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2019, 1, 1), datetime(2019, 1, 6)),
        )
    )
    assert infos[2].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2020, 1, 1), datetime(2020, 1, 6)),
        )
    )

    assert original_infos[0].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2018, 1, 1), datetime(2018, 1, 4)),
        )
    )
    assert original_infos[1].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2019, 1, 1), datetime(2019, 1, 4)),
        )
    )
    assert original_infos[2].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2020, 1, 1), datetime(2020, 1, 4)),
        )
    )

    list_infos = list(zip(infos, original_infos))
    # then
    for info, original_info in list_infos:
        assert [c[0] for c in info.columns] == ["column"]
        assert info.index[0] == ["named_index"]
        assert info.index_type == "index"
        assert info.row_count == 6
        assert original_info.row_count == 4
        assert info.last_update_time > original_info.last_update_time


def test_get_description_batch_multiple_versions(arctic_library):
    lib = arctic_library

    # given
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    df.index.rename("named_index", inplace=True)
    lib.write("symbol1", df)
    to_append_df = pd.DataFrame({"column": [5, 6]}, index=pd.date_range(start="1/5/2018", end="1/6/2018"))
    to_append_df.index.rename("named_index", inplace=True)
    lib.append("symbol1", to_append_df)

    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2019", end="1/4/2019"))
    df.index.rename("named_index", inplace=True)
    lib.write("symbol2", df)
    to_append_df = pd.DataFrame({"column": [5, 6]}, index=pd.date_range(start="1/5/2019", end="1/6/2019"))
    to_append_df.index.rename("named_index", inplace=True)
    lib.append("symbol2", to_append_df)

    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2020", end="1/4/2020"))
    df.index.rename("named_index", inplace=True)
    lib.write("symbol3", df)
    to_append_df = pd.DataFrame({"column": [5, 6]}, index=pd.date_range(start="1/5/2020", end="1/6/2020"))
    to_append_df.index.rename("named_index", inplace=True)
    lib.append("symbol3", to_append_df)

    infos_multiple_version = lib.get_description_batch(
        [
            ReadInfoRequest("symbol1", as_of=0),
            ReadInfoRequest("symbol2", as_of=0),
            ReadInfoRequest("symbol3", as_of=0),
            ReadInfoRequest("symbol1", as_of=1),
            ReadInfoRequest("symbol2", as_of=1),
            ReadInfoRequest("symbol3", as_of=1),
        ]
    )

    infos = infos_multiple_version[3:6]
    original_infos = infos_multiple_version[0:3]

    assert infos[0].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2018, 1, 1), datetime(2018, 1, 6)),
        )
    )
    assert infos[1].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2019, 1, 1), datetime(2019, 1, 6)),
        )
    )
    assert infos[2].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2020, 1, 1), datetime(2020, 1, 6)),
        )
    )

    assert original_infos[0].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2018, 1, 1), datetime(2018, 1, 4)),
        )
    )
    assert original_infos[1].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2019, 1, 1), datetime(2019, 1, 4)),
        )
    )
    assert original_infos[2].date_range == tuple(
        map(
            lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x,
            (datetime(2020, 1, 1), datetime(2020, 1, 4)),
        )
    )

    list_infos = list(zip(infos, original_infos))
    # then
    for info, original_info in list_infos:
        assert [c[0] for c in info.columns] == ["column"]
        assert info.index[0] == ["named_index"]
        assert info.index_type == "index"
        assert info.row_count == 6
        assert original_info.row_count == 4
        assert info.last_update_time > original_info.last_update_time


def test_read_description_batch_high_amount(arctic_library):
    lib = arctic_library
    num_symbols = 10
    num_versions = 4
    start_year = 2000
    start_day = 1
    for version in range(num_versions):
        write_requests = []
        for sym in range(num_symbols):
            start_date = pd.Timestamp(str("{}/1/{}".format(start_year + sym, start_day + version)))
            end_date = pd.Timestamp(str("{}/1/{}".format(start_year + sym, start_day + version + 3)))
            df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start=start_date, end=end_date))
            write_requests.append(WritePayload("sym_" + str(sym), df))
        lib.write_batch(write_requests, prune_previous_versions=False)

    description_requests = [
        ReadInfoRequest("sym_" + str(sym), as_of=version)
        for sym in range(num_symbols)
        for version in range(num_versions)
    ]
    results_list = lib.get_description_batch(description_requests)
    for sym in range(num_symbols):
        for version in range(num_versions):
            idx = sym * num_versions + version
            date_ramge_comp = (
                datetime(start_year + sym, 1, start_day + version),
                datetime(start_year + sym, 1, start_day + version + 3),
            )
            date_range_comp_with_utc = tuple(
                map(lambda x: x.replace(tzinfo=timezone.utc) if not np.isnat(np.datetime64(x)) else x, date_ramge_comp)
            )
            assert results_list[idx].date_range == date_range_comp_with_utc
            if version > 0:
                assert results_list[idx].last_update_time > results_list[idx - 1].last_update_time

                result_last_update_time = results_list[idx].last_update_time
                tz = result_last_update_time.tz

                if IS_PANDAS_TWO:
                    # Pandas 2.0.0 now uses `datetime.timezone.utc` instead of `pytz.UTC`.
                    # See: https://github.com/pandas-dev/pandas/issues/34916
                    # TODO: is there a better way to handle this edge case?
                    assert tz == timezone.utc
                else:
                    assert isinstance(tz, pytz.BaseTzInfo)
                    assert tz == pytz.UTC


def test_read_description_batch_empty_nat(arctic_library):
    lib = arctic_library
    num_symbols = 10
    for sym in range(num_symbols):
        lib.write("sym_" + str(sym), pd.DataFrame())
    requests = [ReadInfoRequest("sym_" + str(sym)) for sym in range(num_symbols)]
    results_list = lib.get_description_batch(requests)
    for sym in range(num_symbols):
        assert np.isnat(results_list[sym].date_range[0]) == True
        assert np.isnat(results_list[sym].date_range[1]) == True
