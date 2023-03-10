"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""

from arcticdb_ext.exceptions import ArcticNativeCxxException

try:
    from arcticdb.version_store import VersionedItem as PythonVersionedItem
except ImportError:
    # arcticdb squashes the packages
    from arcticdb._store import VersionedItem as PythonVersionedItem
from arcticdb_ext.storage import NoDataFoundException

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb import QueryBuilder
import math
import re
import time
import pytest
import pandas as pd
from datetime import datetime, date
import numpy as np


try:
    from arcticdb.version_store.library import (
        WritePayload,
        ArcticDuplicateSymbolsInBatchException,
        ArcticUnsupportedDataTypeException,
        ReadRequest,
        ArcticInvalidApiUsageException,
        StagedDataFinalizeMethod,
    )
except ImportError:
    # arcticdb squashes the packages
    from arcticdb.library import (
        WritePayload,
        ArcticDuplicateSymbolsInBatchException,
        ArcticUnsupportedDataTypeException,
        ReadRequest,
        ArcticInvalidApiUsageException,
        StagedDataFinalizeMethod,
    )


def test_library_creation_deletion(moto_s3_uri_incl_bucket):
    ac = Arctic(moto_s3_uri_incl_bucket)
    assert ac.list_libraries() == []

    ac.create_library("pytest_test_lib")
    with pytest.raises(ValueError):
        ac.create_library("pytest_test_lib")

    assert ac.list_libraries() == ["pytest_test_lib"]
    assert ac["pytest_test_lib"].name == "pytest_test_lib"

    ac.delete_library("pytest_test_lib")
    # Want this to be silent.
    ac.delete_library("library_that_does_not_exist")

    assert not ac.list_libraries()
    with pytest.raises(Exception):  # TODO: Nicely wrap?
        _lib = ac["pytest_test_lib"]


def test_library_options(moto_s3_uri_incl_bucket):
    ac = Arctic(moto_s3_uri_incl_bucket)
    assert ac.list_libraries() == []
    ac.create_library("pytest_default_options")
    lib = ac["pytest_default_options"]
    write_options = lib._nvs._lib_cfg.lib_desc.version.write_options
    assert not write_options.dynamic_schema
    assert not write_options.de_duplication
    assert write_options.segment_row_size == 100_000
    assert write_options.column_group_size == 127

    ac.create_library(
        "pytest_explicit_options",
        LibraryOptions(dynamic_schema=True, dedup=True, rows_per_segment=20, columns_per_segment=3),
    )
    lib = ac["pytest_explicit_options"]
    write_options = lib._nvs._lib_cfg.lib_desc.version.write_options
    assert write_options.dynamic_schema
    assert write_options.de_duplication
    assert write_options.segment_row_size == 20
    assert write_options.column_group_size == 3


def test_separation_between_libraries(moto_s3_uri_incl_bucket):
    """Validate that symbols in one library are not exposed in another."""
    ac = Arctic(moto_s3_uri_incl_bucket)
    assert ac.list_libraries() == []

    ac.create_library("pytest_test_lib")
    ac.create_library("pytest_test_lib_2")

    assert ac.list_libraries() == ["pytest_test_lib", "pytest_test_lib_2"]

    ac["pytest_test_lib"].write("test_1", pd.DataFrame())
    ac["pytest_test_lib_2"].write("test_2", pd.DataFrame())
    assert ac["pytest_test_lib"].list_symbols() == ["test_1"]
    assert ac["pytest_test_lib_2"].list_symbols() == ["test_2"]


def test_separation_between_libraries_with_prefixes(moto_s3_uri_incl_bucket):
    """The motivation for the prefix feature is that separate users want to be able to create libraries
    with the same name in the same bucket without over-writing each other's work. This can be useful when
    creating a new bucket is time-consuming, for example due to organisational issues.

    See AN-566.
    """
    mercury_uri = moto_s3_uri_incl_bucket + "&path_prefix=/planet/mercury"
    ac_mercury = Arctic(mercury_uri)
    assert ac_mercury.list_libraries() == []

    mars_uri = moto_s3_uri_incl_bucket + "&path_prefix=/planet/mars"
    ac_mars = Arctic(mars_uri)
    assert ac_mars.list_libraries() == []

    ac_mercury.create_library("pytest_test_lib")
    ac_mars.create_library("pytest_test_lib")
    assert ac_mercury.list_libraries() == ["pytest_test_lib"]
    assert ac_mars.list_libraries() == ["pytest_test_lib"]

    ac_mercury["pytest_test_lib"].write("test_1", pd.DataFrame())
    ac_mars["pytest_test_lib"].write("test_2", pd.DataFrame())

    assert ac_mercury["pytest_test_lib"].list_symbols() == ["test_1"]
    assert ac_mars["pytest_test_lib"].list_symbols() == ["test_2"]


def test_library_management_path_prefix(moto_s3_uri_incl_bucket, boto_client):
    test_bucket = sorted(boto_client.list_buckets()["Buckets"], key=lambda bucket_meta: bucket_meta["CreationDate"])[
        -1
    ]["Name"]

    URI = moto_s3_uri_incl_bucket + "&path_prefix=hello/world"
    ac = Arctic(URI)
    assert ac.list_libraries() == []

    ac.create_library("pytest_test_lib")

    ac["pytest_test_lib"].write("test_1", pd.DataFrame())
    ac["pytest_test_lib"].write("test_2", pd.DataFrame())

    assert sorted(ac["pytest_test_lib"].list_symbols()) == ["test_1", "test_2"]

    ac["pytest_test_lib"].snapshot("test_snapshot")
    assert ac["pytest_test_lib"].list_snapshots() == {"test_snapshot": None}

    keys = [d["Key"] for d in boto_client.list_objects(Bucket=test_bucket)["Contents"]]
    assert all(k.startswith("hello/world") for k in keys)
    assert any(k.startswith("hello/world/_arctic_cfg") for k in keys)
    assert any(k.startswith("hello/world/pytest_test_lib") for k in keys)

    ac.delete_library("pytest_test_lib")

    assert not ac.list_libraries()
    with pytest.raises(Exception):  # TODO: Nicely wrap?
        _lib = ac["pytest_test_lib"]


def test_basic_write_read_update_and_append(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    written_vi = lib.write("my_symbol", df)
    assert re.match(r"S3\(endpoint=localhost:\d+, bucket=test_bucket_\d+\)", written_vi.host)

    assert lib.list_symbols() == ["my_symbol"]
    pd.testing.assert_frame_equal(lib.read("my_symbol").data, df)
    pd.testing.assert_frame_equal(lib.read("my_symbol", columns=["col1"]).data, df[["col1"]])

    pd.testing.assert_frame_equal(lib.head("my_symbol", n=1).data, df.head(n=1))
    pd.testing.assert_frame_equal(lib.tail("my_symbol", n=1).data, df.tail(n=1).reset_index(drop=True))

    lib.append("my_symbol", pd.DataFrame({"col1": [4, 5, 6], "col2": [7, 8, 9]}))
    assert lib["my_symbol"].version == 1
    pd.testing.assert_frame_equal(
        lib.read("my_symbol").data, pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6], "col2": [4, 5, 6, 7, 8, 9]})
    )

    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    df.index = pd.date_range("2018-01-01", periods=3, freq="H")
    lib.write("timeseries", df, metadata={"hello": "world"})
    assert lib["timeseries"].version == 0

    df = pd.DataFrame({"col1": [4, 5, 6], "col2": [7, 8, 9]})
    df.index = pd.date_range("2018-01-01", periods=3, freq="H")
    lib.update("timeseries", df)
    assert lib["timeseries"].version == 1
    df.index.freq = None
    pd.testing.assert_frame_equal(lib.read("timeseries").data, df)

    lib.write("meta", df, metadata={"hello": "world"})
    assert lib["meta"].version == 0

    read_metadata = lib.read_metadata("meta")
    assert read_metadata.metadata == {"hello": "world"}
    assert read_metadata.data is None
    assert read_metadata.version == 0

    lib.write("meta", df, metadata={"goodbye": "cruel world"})
    read_metadata = lib.read_metadata("meta")
    assert read_metadata.version == 1


def staged_write(sym, arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    df.index = pd.date_range("2018-01-01", periods=3, freq="H")
    lib.write(sym, df, staged=True)

    df = pd.DataFrame({"col1": [4, 5, 6], "col2": [7, 8, 9]})
    df.index = pd.date_range("2018-01-01 03:00:00", periods=3, freq="H")
    lib.write(sym, df, staged=True)


def test_parallel_writes_and_appends(arctic_library):
    lib = arctic_library
    staged_write("my_symbol", lib)
    staged_write("my_other_symbol", lib)
    staged_write("yet_another_symbol", lib)

    lib.finalize_staged_data("my_symbol", StagedDataFinalizeMethod.WRITE)
    lib.finalize_staged_data("my_other_symbol", StagedDataFinalizeMethod.WRITE)

    assert set(lib.list_symbols()) == {"my_symbol", "my_other_symbol"}

    comp_df = pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6], "col2": [4, 5, 6, 7, 8, 9]})
    comp_df.index = pd.date_range("2018-01-01", periods=6, freq="H")
    pd.testing.assert_frame_equal(lib.read("my_symbol").data, comp_df)
    pd.testing.assert_frame_equal(lib.read("my_other_symbol").data, comp_df)

    df = pd.DataFrame({"col1": [7, 8, 9], "col2": [10, 11, 12]})
    df.index = pd.date_range("2018-01-01 06:00:00", periods=3, freq="H")
    lib.write("my_symbol", df, staged=True)

    lib.finalize_staged_data("my_symbol", StagedDataFinalizeMethod.APPEND)

    comp_df = pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6, 7, 8, 9], "col2": [4, 5, 6, 7, 8, 9, 10, 11, 12]})
    comp_df.index = pd.date_range("2018-01-01", periods=9, freq="H")
    pd.testing.assert_frame_equal(lib.read("my_symbol").data, comp_df)


def test_snapshots_and_deletes(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("my_symbol", df)
    lib.write("my_symbol2", df)

    lib.snapshot("test1")

    assert lib.list_snapshots() == {"test1": None}

    pd.testing.assert_frame_equal(lib.read("my_symbol", as_of="test1").data, df)

    lib.delete("my_symbol")
    lib.snapshot("snap_after_delete")
    assert sorted(lib.list_symbols("test1")) == ["my_symbol", "my_symbol2"]
    assert lib.list_symbols("snap_after_delete") == ["my_symbol2"]

    lib.delete_snapshot("test1")
    assert lib.list_snapshots() == {"snap_after_delete": None}
    assert lib.list_symbols() == ["my_symbol2"]


def test_delete_non_existent_snapshot(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("my_symbol", df)
    with pytest.raises(NoDataFoundException):
        lib.delete_snapshot("test")


def test_prune_previous_versions(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df, metadata={"very": "interesting"})
    lib.write("symbol", df, metadata={"muy": "interesante"}, prune_previous_versions=False)
    lib.write("symbol", df, metadata={"tres": "interessant"}, prune_previous_versions=False)
    lib.prune_previous_versions("symbol")
    assert len(lib.list_versions("symbol")) == 1
    assert ("symbol", 2) in lib.list_versions("symbol")
    assert lib["symbol"].metadata == {"tres": "interessant"}


def test_delete_version(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df, metadata={"very": "interesting"})
    lib.write("symbol", df, metadata={"muy": "interesante"}, prune_previous_versions=False)
    lib.write("symbol", df, metadata={"tres": "interessant"}, prune_previous_versions=False)
    lib.delete("symbol", versions=(1, 2))
    assert lib["symbol"].version == 0
    assert lib["symbol"].metadata == {"very": "interesting"}


def test_delete_version_with_snapshot(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df, metadata={"very": "interesting"})
    lib.write("symbol", df, metadata={"muy": "interesante"}, prune_previous_versions=False)
    lib.snapshot("my_snap")
    lib.delete("symbol", versions=1)
    assert lib["symbol"].version == 0
    assert lib["symbol"].metadata == {"very": "interesting"}
    assert lib.read("symbol", as_of=1).version == 1
    assert lib.read("symbol", as_of=1).metadata == {"muy": "interesante"}
    assert lib.read("symbol", as_of="my_snap").version == 1
    assert lib.read("symbol", as_of="my_snap").metadata == {"muy": "interesante"}


def test_list_versions_with_snapshot(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df, metadata={"very": "interesting"})
    lib.write("symbol", df, metadata={"muy": "interesante"}, prune_previous_versions=False)
    lib.snapshot("my_snap")
    lib.write("symbol", df, metadata={"tres": "interessant"}, prune_previous_versions=False)
    lib.delete("symbol", versions=(1, 2))

    versions = lib.list_versions("symbol")

    assert not versions["symbol", 0].deleted
    assert not versions["symbol", 0].snapshots
    assert versions["symbol", 1].deleted
    assert versions["symbol", 1].snapshots == ["my_snap"]
    assert versions["symbol", 1].date > versions["symbol", 0].date


def test_list_versions_without_snapshot(arctic_library):
    lib = arctic_library
    lib.write("symbol", pd.DataFrame())

    versions = lib.list_versions("symbol")

    assert len(versions) == 1
    assert versions["symbol", 0].snapshots == []


def test_delete_version_that_does_not_exist(arctic_library):
    lib = arctic_library

    # symbol does not exist
    with pytest.raises(ArcticNativeCxxException):
        lib.delete("symbol", versions=0)

    # version does not exist
    lib.write("symbol", pd.DataFrame())
    with pytest.raises(ArcticNativeCxxException):
        lib.delete("symbol", versions=1)


def test_delete_date_range(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [5, 6, 7, 8]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    # when
    lib.delete_data_in_range("symbol", date_range=(datetime(2018, 1, 1), datetime(2018, 1, 2)))

    # then
    pd.testing.assert_frame_equal(
        lib["symbol"].data, pd.DataFrame({"column": [7, 8]}, index=pd.date_range(start="1/3/2018", end="1/4/2018"))
    )
    assert lib["symbol"].version == 1


def test_repr(moto_s3_uri_incl_bucket):
    ac = Arctic(moto_s3_uri_incl_bucket)

    assert ac.list_libraries() == []
    ac.create_library("pytest_test_lib")

    lib = ac["pytest_test_lib"]
    s3_endpoint = moto_s3_uri_incl_bucket.split("//")[1].split(":")[0]
    port = moto_s3_uri_incl_bucket.split("port=")[1]
    s3_endpoint += f":{port}"
    bucket = moto_s3_uri_incl_bucket.split(":")[-1].split("?")[0]
    assert (
        repr(lib) == "Library("
        "Arctic("
        "config=S3("
        f"endpoint={s3_endpoint}, bucket={bucket})), path=pytest_test_lib, storage=s3_storage)"
    )


class A:
    """A dummy user defined type that requires pickling to serialize."""

    def __init__(self, id: str):
        self.id = id

    def __eq__(self, other):
        return self.id == other.id


def test_write_object_with_pickle_mode(arctic_library):
    """Writing in pickle mode should succeed when the user uses the dedicated method."""
    lib = arctic_library
    lib.write_pickle("test_1", A("id_1"))
    assert lib["test_1"].data.id == "id_1"


def test_write_batch_with_pickle_mode(arctic_library):
    """Writing in pickle mode should succeed when the user uses the dedicated method."""
    lib = arctic_library
    lib.write_batch_pickle(
        [WritePayload("test_1", A("id_1")), WritePayload("test_2", A("id_2"), metadata="the metadata")]
    )
    assert lib["test_1"].data.id == "id_1"
    assert lib["test_1"].version == 0
    assert lib["test_2"].data.id == "id_2"
    assert lib["test_2"].metadata == "the metadata"


def test_write_object_without_pickle_mode(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    with pytest.raises(ArcticUnsupportedDataTypeException):
        lib.write("test_1", A("id_1"))


def test_write_object_in_batch_without_pickle_mode(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    with pytest.raises(ArcticUnsupportedDataTypeException) as e:
        lib.write_batch([WritePayload("test_1", A("id_1"))])
    # omit the part with the full class path as that will change in arcticdb
    assert e.value.args[0].startswith(
        "payload contains some data of types that cannot be normalized. Consider using write_batch_pickle instead. symbols with bad datatypes"
    )


def test_write_object_in_batch_without_pickle_mode_many_symbols(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    with pytest.raises(ArcticUnsupportedDataTypeException) as e:
        lib.write_batch([WritePayload(f"test_{i}", A(f"id_{i}")) for i in range(10)])
    message: str = e.value.args[0]
    assert "(and more)... 10 data in total have bad types." in message


def test_write_list_without_pickle_mode(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    with pytest.raises(ArcticUnsupportedDataTypeException):
        lib.write("test_1", [1, 2, 3])


def test_write_non_native_frame_with_pickle_mode(arctic_library):
    """Writing with pickle mode should work when the user calls the dedicated method."""
    lib = arctic_library
    df = pd.DataFrame({"col1": [A("id_1")]})
    lib.write_pickle("test_1", df)
    loaded: pd.DataFrame = lib["test_1"].data
    pd.testing.assert_frame_equal(loaded, df[["col1"]])


def test_write_non_native_frame_without_pickle_mode(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    df = pd.DataFrame({"col1": [A("id_1")]})
    with pytest.raises(Exception):
        lib.write("test_1", df)


def test_write_batch(arctic_library):
    """Should be able to write a batch of data."""
    lib = arctic_library
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    df_2 = pd.DataFrame({"col1": [-1, -2, -3], "col2": [-4, -5, -6], "anothercol": [0, 0, 0]})

    batch = lib.write_batch([WritePayload("symbol_1", df_1), WritePayload("symbol_2", df_2, metadata="great_metadata")])

    pd.testing.assert_frame_equal(lib.read("symbol_1", columns=["col1"]).data, df_1[["col1"]])
    pd.testing.assert_frame_equal(lib.read("symbol_2", columns=["col1"]).data, df_2[["col1"]])
    pd.testing.assert_frame_equal(lib.read("symbol_2", columns=["anothercol"]).data, df_2[["anothercol"]])

    symbol_2_loaded = lib.read("symbol_2")
    assert symbol_2_loaded.metadata == "great_metadata"
    assert all(type(w) == PythonVersionedItem for w in batch)


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


def test_write_batch_pickle_duplicate_symbols(arctic_library):
    """Should throw and not write if duplicate symbols are provided."""
    lib = arctic_library
    with pytest.raises(ArcticDuplicateSymbolsInBatchException):
        lib.write_batch_pickle(
            [
                WritePayload("symbol_1", pd.DataFrame()),
                WritePayload("symbol_1", pd.DataFrame(), metadata="great_metadata"),
            ]
        )

    assert not lib.list_symbols()


def test_write_with_unpacking(arctic_library):
    """Check the syntactic sugar that lets us unpack WritePayload in `write` calls using *."""
    lib = arctic_library
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    df_2 = pd.DataFrame({"col1": [-1, -2, -3], "col2": [-4, -5, -6], "anothercol": [0, 0, 0]})

    payload_1 = WritePayload("symbol_1", df_1)
    payload_2 = WritePayload("symbol_2", df_2, metadata="great_metadata")

    lib.write(*payload_1)
    lib.write(*payload_2)

    pd.testing.assert_frame_equal(lib.read("symbol_1", columns=["col1"]).data, df_1[["col1"]])
    pd.testing.assert_frame_equal(lib.read("symbol_2", columns=["col1"]).data, df_2[["col1"]])
    pd.testing.assert_frame_equal(lib.read("symbol_2", columns=["anothercol"]).data, df_2[["anothercol"]])

    symbol_2_loaded = lib.read("symbol_2")
    assert symbol_2_loaded.metadata == "great_metadata"


def test_prune_previous_versions_with_write(arctic_library):
    lib = arctic_library
    # When
    lib.write("sym", pd.DataFrame())
    lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}), prune_previous_versions=False)

    # Then
    v0 = lib.read("sym", as_of=0).data
    assert v0.empty

    v1 = lib.read("sym", as_of=1).data
    assert not v1.empty

    # We prune by default
    lib.write("sym", pd.DataFrame())
    with pytest.raises(NoDataFoundException):
        lib.read("sym", as_of=0)
    with pytest.raises(NoDataFoundException):
        lib.read("sym", as_of=1)

    v3 = lib.read("sym", as_of=2).data
    assert v3.empty


def test_append_documented_example(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3]}, index=pd.date_range(start="1/1/2018", end="1/3/2018"))
    lib.write("symbol", df)
    to_append_df = pd.DataFrame({"column": [4, 5, 6]}, index=pd.date_range(start="1/4/2018", end="1/6/2018"))
    lib.append("symbol", to_append_df, prune_previous_versions=False)

    expected = pd.DataFrame({"column": [1, 2, 3, 4, 5, 6]}, index=pd.date_range(start="1/1/2018", end="1/6/2018"))
    pd.testing.assert_frame_equal(lib["symbol"].data, expected)
    # Check that old versions were not pruned
    symbols = lib.list_versions("symbol")
    assert len(symbols) == 2
    assert ("symbol", 0) in symbols
    assert ("symbol", 1) in symbols
    pd.testing.assert_frame_equal(lib.read("symbol", as_of=0).data, df)


def test_append_prune_previous_versions(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3]}, index=pd.date_range(start="1/1/2018", end="1/3/2018"))
    lib.write("symbol", df)
    to_append_df = pd.DataFrame({"column": [4, 5, 6]}, index=pd.date_range(start="1/4/2018", end="1/6/2018"))
    lib.append("symbol", to_append_df, prune_previous_versions=True)

    expected = pd.DataFrame({"column": [1, 2, 3, 4, 5, 6]}, index=pd.date_range(start="1/1/2018", end="1/6/2018"))
    pd.testing.assert_frame_equal(lib["symbol"].data, expected)
    # Check that old versions were pruned
    symbols = lib.list_versions("symbol")
    assert len(symbols) == 1
    assert ("symbol", 1) in symbols


def test_update_documented_example(arctic_library):
    """Test the example given on the `update` docstring."""
    lib = arctic_library
    # Given
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": [400, 40]}, index=pd.date_range(start="1/1/2018", end="1/3/2018", freq="2D"))

    # When
    lib.update("symbol", update_df, prune_previous_versions=False)

    # Then
    result = lib.read("symbol").data
    expected = pd.DataFrame({"column": [400, 40, 4]}, index=pd.to_datetime(["1/1/2018", "1/3/2018", "1/4/2018"]))
    pd.testing.assert_frame_equal(result, expected)
    # Check that old versions were not pruned
    symbols = lib.list_versions("symbol")
    assert len(symbols) == 2
    assert ("symbol", 0) in symbols
    assert ("symbol", 1) in symbols
    pd.testing.assert_frame_equal(lib.read("symbol", as_of=0).data, df)


def test_update_prune_previous_versions(arctic_library):
    """Test that updating and pruning previous versions does indeed clear previous versions."""
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": [400, 40]}, index=pd.date_range(start="1/1/2018", end="1/3/2018", freq="2D"))

    lib.update("symbol", update_df, prune_previous_versions=True)

    result = lib.read("symbol").data
    expected = pd.DataFrame({"column": [400, 40, 4]}, index=pd.to_datetime(["1/1/2018", "1/3/2018", "1/4/2018"]))
    pd.testing.assert_frame_equal(result, expected)
    symbols = lib.list_versions("symbol")
    assert len(symbols) == 1
    assert ("symbol", 1) in symbols


def test_update_with_daterange(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": [400]}, index=pd.to_datetime(["1/2/2018"]))

    lib.update("symbol", update_df, date_range=(datetime(2010, 1, 1), datetime(2020, 1, 1)))

    result = lib["symbol"].data
    pd.testing.assert_frame_equal(result, update_df)


def test_update_with_daterange_no_width(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": [400, 500]}, index=pd.date_range(start="1/2/2018", end="1/3/2018"))

    lib.update("symbol", update_df, date_range=(datetime(2018, 1, 2), datetime(2018, 1, 2)))

    result = lib["symbol"].data
    pd.testing.assert_frame_equal(
        result, pd.DataFrame({"column": [1, 400, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    )


def test_update_with_daterange_multi_index(arctic_library):
    lib = arctic_library
    # Given
    index = pd.MultiIndex.from_tuples(
        list(
            zip(
                [
                    datetime(2018, 1, 1),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 3),
                    datetime(2018, 1, 3),
                    datetime(2018, 1, 4),
                ],
                ["A", "B", "C", "D", "E", "F"],
            )
        )
    )
    df = pd.DataFrame({"column": [1, 2, 3, 4, 5, 6]}, index=index)
    lib.write("symbol", df)

    # When
    update_index = pd.MultiIndex.from_tuples(
        list(zip([datetime(2018, 1, 2), datetime(2018, 1, 2), datetime(2018, 1, 3)], ["B", "C", "D"]))
    )
    update_df = pd.DataFrame({"column": [100, 200, 300]}, index=update_index)
    lib.update("symbol", update_df, date_range=(datetime(2018, 1, 2), datetime(2018, 1, 3)))

    # Then
    result = lib["symbol"].data
    # note that 2018-1-3 "E" has been removed as it is within date_range even though it is outside update_index
    expected_index = pd.MultiIndex.from_tuples(
        list(
            zip(
                [
                    datetime(2018, 1, 1),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 3),
                    datetime(2018, 1, 4),
                ],
                ["A", "B", "C", "D", "F"],
            )
        )
    )
    pd.testing.assert_frame_equal(result, pd.DataFrame({"column": [1, 100, 200, 300, 6]}, index=expected_index))


def test_update_with_daterange_multi_index_no_width(arctic_library):
    lib = arctic_library

    # Given
    index = pd.MultiIndex.from_tuples(
        list(
            zip(
                [
                    datetime(2018, 1, 1),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 3),
                    datetime(2018, 1, 3),
                ],
                ["A", "B", "C", "D", "E"],
            )
        )
    )
    df = pd.DataFrame({"column": [1, 2, 3, 4, 5]}, index=index)
    lib.write("symbol", df)

    # When
    update_index = pd.MultiIndex.from_tuples(
        list(zip([datetime(2018, 1, 2), datetime(2018, 1, 2), datetime(2018, 1, 3)], ["B", "C", "D"]))
    )
    update_df = pd.DataFrame({"column": [100, 200, 300]}, index=update_index)
    lib.update("symbol", update_df, date_range=(datetime(2018, 1, 2), datetime(2018, 1, 2)))

    # Then
    result = lib["symbol"].data
    pd.testing.assert_frame_equal(result, pd.DataFrame({"column": [1, 100, 200, 4, 5]}, index=index))


def test_update_with_daterange_restrictive(arctic_library):
    """Here the update_df cover more dates than date_range. We should only touch data that lies within date_range."""
    lib = arctic_library
    df = pd.DataFrame({"column": np.arange(30)}, index=pd.date_range(start="1/1/2022", periods=30))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": np.arange(30, 60)}, index=pd.date_range(start="1/1/2022", periods=30))
    lib.update("symbol", update_df, date_range=(datetime(2022, 1, 10), datetime(2022, 1, 20)))

    result = lib["symbol"].data
    assert result.size == 30
    expected = pd.DataFrame(
        {"column": np.concatenate((np.arange(0, 9), np.arange(39, 50), np.arange(20, 30)))},
        index=pd.date_range(start="1/1/2022", periods=30),
    )
    pd.testing.assert_frame_equal(expected, result)


def test_update_with_upsert(arctic_library):
    lib = arctic_library
    with pytest.raises(Exception):
        lib.update("symbol", pd.DataFrame())
    assert not lib.list_symbols()
    lib.update("symbol", pd.DataFrame(), upsert=True)
    assert "symbol" in lib.list_symbols()


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
    pd.testing.assert_frame_equal(s2_frame, batch[1].data)
    pd.testing.assert_frame_equal(s3_frame, batch[2].data)
    assert batch[3].data.empty


def test_read_with_read_request_form(arctic_library):
    lib = arctic_library

    # Given
    q = QueryBuilder()
    q = q[q["A"] < 3]
    lib.write("s", pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))

    # When
    result = lib.read(*ReadRequest("s", as_of=0, columns=["A"], query_builder=q))

    # Then
    pd.testing.assert_frame_equal(result.data, pd.DataFrame({"A": [1, 2]}))


def test_read_batch_with_columns(arctic_library):
    lib = arctic_library

    # Given
    lib.write("s", pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}))

    # When
    batch = lib.read_batch([ReadRequest("s", columns=["B", "C"])])

    # Then
    pd.testing.assert_frame_equal(pd.DataFrame({"B": [4, 5, 6], "C": [7, 8, 9]}), batch[0].data)


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
    pd.testing.assert_frame_equal(batch[0].data, pd.DataFrame({"a": [3]}))
    pd.testing.assert_frame_equal(batch[1].data, pd.DataFrame({"a": [4]}))


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
    pd.testing.assert_frame_equal(batch[0].data, pd.DataFrame({"a": [3]}))
    pd.testing.assert_frame_equal(batch[1].data, pd.DataFrame({"a": [4, 6]}))


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

    pd.testing.assert_frame_equal(
        batch[0].data, pd.DataFrame({"column": [1, 2]}, index=pd.date_range(start="1/1/2018", end="1/2/2018"))
    )
    pd.testing.assert_frame_equal(
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

    pd.testing.assert_frame_equal(
        batch[0].data, pd.DataFrame({"column": [1, 2]}, index=pd.date_range(start="1/1/2018", end="1/2/2018"))
    )
    pd.testing.assert_frame_equal(
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


def test_has_symbol(arctic_library):
    lib = arctic_library
    lib.write("symbol", pd.DataFrame())
    lib.write("symbol", pd.DataFrame(), prune_previous_versions=False)
    assert lib.has_symbol("symbol")
    assert "symbol" in lib
    lib.snapshot("snapshot")
    lib.delete("symbol")
    assert "symbol" not in lib
    assert lib.has_symbol("symbol", as_of="snapshot")


def test_get_description(arctic_library):
    lib = arctic_library

    # given
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    df.index.rename("named_index", inplace=True)
    lib.write("symbol", df)
    to_append_df = pd.DataFrame({"column": [5, 6]}, index=pd.date_range(start="1/5/2018", end="1/6/2018"))
    to_append_df.index.rename("named_index", inplace=True)
    lib.append("symbol", to_append_df)
    # when
    info = lib.get_description("symbol")
    original_info = lib.get_description("symbol", as_of=0)
    # then
    assert [c[0] for c in info.columns] == ["column"]
    assert info.date_range == (datetime(2018, 1, 1), datetime(2018, 1, 6))
    assert info.index[0] == ["named_index"]
    assert info.index_type == "index"
    assert info.row_count == 6
    assert original_info.row_count == 4
    assert info.last_update_time > original_info.last_update_time


def test_tail(arctic_library):
    lib = arctic_library

    # given
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("sym", df)
    df = pd.DataFrame({"column": [5, 6, 7, 8]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("sym", df, prune_previous_versions=False)
    df = pd.DataFrame({"column": [9]}, index=pd.date_range(start="1/1/2018", end="1/1/2018"))
    lib.write("sym", df, prune_previous_versions=False)

    # when
    first_version = lib.tail("sym", 2, as_of=0)
    second_version = lib.tail("sym", 2, as_of=1)
    third_version = lib.tail("sym", 2, as_of=2)

    # then
    assert first_version.version == 0
    pd.testing.assert_frame_equal(
        first_version.data, pd.DataFrame({"column": [3, 4]}, index=pd.date_range(start="1/3/2018", end="1/4/2018"))
    )
    assert second_version.version == 1
    pd.testing.assert_frame_equal(
        second_version.data, pd.DataFrame({"column": [7, 8]}, index=pd.date_range(start="1/3/2018", end="1/4/2018"))
    )
    assert third_version.version == 2
    pd.testing.assert_frame_equal(
        third_version.data, pd.DataFrame({"column": [9]}, index=pd.date_range(start="1/1/2018", end="1/1/2018"))
    )


@pytest.mark.parametrize("dedup", [True, False])
def test_dedup(moto_s3_uri_incl_bucket, dedup):
    ac = Arctic(moto_s3_uri_incl_bucket)
    assert ac.list_libraries() == []
    ac.create_library("pytest_test_library", LibraryOptions(dedup=dedup))
    lib = ac["pytest_test_library"]
    symbol = "test_dedup"
    lib.write_pickle(symbol, 1)
    lib.write_pickle(symbol, 1, prune_previous_versions=False)
    data_key_version = lib._nvs.read_index(symbol)["version_id"][0]
    assert data_key_version == 0 if dedup else 1


def test_segment_slicing(moto_s3_uri_incl_bucket):
    ac = Arctic(moto_s3_uri_incl_bucket)
    assert ac.list_libraries() == []
    rows_per_segment = 5
    columns_per_segment = 2
    ac.create_library("pytest_test_library", LibraryOptions(rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment))
    lib = ac["pytest_test_library"]
    symbol = "test_segment_slicing"
    rows = 12
    columns = 3
    data = {}
    for col in range(columns):
        data[f"col{col}"] = np.arange(100*col, (100*col) + rows)
    lib.write(symbol, pd.DataFrame(data))
    num_data_segments = len(lib._nvs.read_index(symbol))
    assert num_data_segments == math.ceil(rows / rows_per_segment) * math.ceil(columns / columns_per_segment)


def test_reload_symbol_list(moto_s3_uri_incl_bucket, boto_client):
    def get_symbol_list_keys():
        keys = [d["Key"] for d in boto_client.list_objects(Bucket=test_bucket)["Contents"] if d["Key"].startswith(lib_name)]
        symbol_list_keys = []
        for key in keys:
            path_components = key.split("/")
            if path_components[1] == "sl":
                symbol_list_keys.append(path_components[2])
        return symbol_list_keys

    test_bucket = sorted(boto_client.list_buckets()["Buckets"], key=lambda bucket_meta: bucket_meta["CreationDate"])[
        -1
    ]["Name"]
    ac = Arctic(moto_s3_uri_incl_bucket)
    assert ac.list_libraries() == []

    lib_name = "pytest_test_lib"

    ac.create_library(lib_name)
    lib = ac[lib_name]

    lib.write_pickle("symbol_1", 1)
    lib.write_pickle("symbol_2", 2)
    assert set(lib.list_symbols()) == {"symbol_1", "symbol_2"}
    lib.delete("symbol_1")
    assert set(lib.list_symbols()) == {"symbol_2"}
    assert len(get_symbol_list_keys()) == 2

    lib.reload_symbol_list()
    assert len(get_symbol_list_keys()) == 1


if __name__ == "__main__":
    # Cheap and dirty test harness to make testing in various different Python installs easier
    uri = (
        "s3://s3.gdc.res.m:arctic-native-tests?access="
        "PSFBSAZRHGPOEGGMMPGCEGDHOLPCOHMLKPBKDBDIOH&secret=23A013A1Fabc7e2fb/f48eF20BB093d7259150BOPA"
    )

    ac = Arctic(uri)
    if "pytest_test_lib" in ac.list_libraries():
        ac.delete_library("pytest_test_lib")

        time.sleep(5)

    test_basic_write_read_update_and_append(uri)

    ac.delete_library("pytest_test_lib")
    time.sleep(5)

    test_snapshots_and_deletes(uri)
