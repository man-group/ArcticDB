"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import multiprocessing as mp
import pytest
import numpy as np
import pandas as pd
import os
import sys

from arcticdb import Arctic
from arcticdb.util.test import assert_frame_equal
from arcticdb.exceptions import LmdbMapFullError, StorageException
from arcticdb.util.test import get_wide_dataframe
import arcticdb.adapters.lmdb_library_adapter as la
from arcticdb.exceptions import LmdbOptionsError


def test_batch_read_only_segfault_regression(tmpdir):
    # See Github issue #520
    # This segfaults with arcticdb==1.5.0
    ac = Arctic(f"lmdb://{tmpdir}/lmdb_instance")
    ac.create_library("test_lib")
    lib = ac["test_lib"]
    df = pd.DataFrame({"a": list(range(100))}, index=list(range(100)))
    for i in range(100):
        lib.write(str(i), df, prune_previous_versions=True)

    # New Arctic instance is essential to repro the bug
    fresh_lib = Arctic(f"lmdb://{tmpdir}/lmdb_instance")["test_lib"]
    vis = fresh_lib.read_batch([str(i) for i in range(100)])  # used to crash
    assert len(vis) == 100
    assert_frame_equal(vis[0].data, df)


def test_library_deletion(tmpdir):
    # See Github issue #517
    # Given
    ac = Arctic(f"lmdb://{tmpdir}/lmdb_instance")
    path = os.path.join(tmpdir, "lmdb_instance", "test_lib")
    ac.create_library("test_lib")
    assert os.path.exists(path)

    ac.create_library("test_lib2")

    # When
    ac.delete_library("test_lib")

    # Then
    assert not os.path.exists(path)
    assert ac.list_libraries() == ["test_lib2"]


def test_library_deletion_leave_non_lmdb_files_alone(tmpdir):
    # See Github issue #517
    # Given
    ac = Arctic(f"lmdb://{tmpdir}/lmdb_instance")
    path = os.path.join(tmpdir, "lmdb_instance", "test_lib")
    ac.create_library("test_lib")
    assert os.path.exists(path)
    with open(os.path.join(path, "another"), "w") as f:
        f.write("blah")
    os.makedirs(os.path.join(path, "dir"))

    ac.create_library("test_lib2")

    # When
    ac.delete_library("test_lib")

    # Then
    assert os.path.exists(path)
    files = set(os.listdir(path))
    assert files == {"dir", "another"}
    assert ac.list_libraries() == ["test_lib2"]


def test_lmdb(tmpdir):
    # Github Issue #520 #889 - this used to segfault
    d = {
        "test1": pd.Timestamp("1979-01-18 00:00:00"),
        "test2": pd.Timestamp("1979-01-19 00:00:00"),
    }

    ac = Arctic(f"lmdb://{tmpdir}")
    ac.create_library("model")
    lib = ac.get_library("model")

    for i in range(50):
        lib.write_pickle("test", d)
        lib = ac.get_library("model")
        lib.read("test").data


def test_lmdb_malloc_trim(tmpdir):
    # Make sure that the bindings for calling malloc_trim have been setup correctly
    ac = Arctic(f"lmdb://{tmpdir}")
    ac.create_library("test_lmdb_malloc_trim")
    lib = ac["test_lmdb_malloc_trim"]
    lib._nvs.trim()


def test_lmdb_mapsize(tmpdir):
    # Given - tiny map size
    ac = Arctic(f"lmdb://{tmpdir}?map_size=1KB")

    # When
    with pytest.raises(LmdbMapFullError) as e:
        ac.create_library("test")
    # Then - even library creation fails so map size having an effect
    assert "MDB_MAP_FULL" in str(e.value)
    assert "E5003" in str(e.value)
    assert issubclass(e.type, LmdbMapFullError)

    # Given - larger map size
    ac = Arctic(f"lmdb://{tmpdir}?map_size=1MB")

    # When
    lib = ac["test"]
    df = get_wide_dataframe(size=1_000)
    lib.write("sym", df)

    # Then - operations succeed as usual


def test_lmdb_mapsize_write(tmpdir):
    ac = Arctic(f"lmdb://{tmpdir}?map_size=1MB")
    df = pd.DataFrame(np.random.randint(0, 100, size=(int(1e6), 4)), columns=list('ABCD'))
    lib = ac.create_library("test")

    with pytest.raises(LmdbMapFullError) as e:
        lib.write("sym", df)

    assert "MDB_MAP_FULL" in str(e.value)
    assert "E5003" in str(e.value)
    assert "mdb_put" in str(e.value)
    assert "-30792" in str(e.value)
    assert issubclass(e.type, StorageException)


@pytest.mark.parametrize(
    "options, expected",
    [
        ("map_size=1KB", int(1e3)),
        ("?map_size=20MB", int(20e6)),
        ("map_size=100GB", int(100e9)),
        ("map_size=3TB", int(3e12)),
    ],
)
def test_map_size_parsing(options, expected):
    result = la.parse_query(options)
    assert result.map_size == expected


@pytest.mark.parametrize(
    "options",
    ["map_size=1kb", "map_size=-3MB", "map_size=0GB", "map_size=", "map_size=", "map_size=@", "map_size=oStRiCh"],
)
def test_map_size_bad_input(options):
    with pytest.raises(LmdbOptionsError) as e:
        la.parse_query(options)

    assert "Incorrect format for map_size" in str(e.value)


@pytest.mark.parametrize("options", ["MAP_SIZE=1GB", "atlas_shape=1GB"])
def test_lmdb_options_unknown_option(options):
    with pytest.raises(LmdbOptionsError) as e:
        la.parse_query(options)

    assert "Invalid LMDB URI" in str(e.value)


def create_arctic_instance(td, i):
    ac = Arctic(f"lmdb://{td}")
    lib = ac["test"]
    assert lib.read("a")
    lib.write(f"{i}", pd.DataFrame())
    assert lib.read(f"{i}")


def test_warnings_arctic_instance(tmpdir):
    ac = Arctic(f"lmdb://{tmpdir}")
    # should warn
    ac = Arctic(f"lmdb://{tmpdir}")

    del ac
    # should not warn
    ac = Arctic(f"lmdb://{tmpdir}")


def test_warnings_library(tmpdir):
    """Should not warn - library caching prevents us opening LMDB env twice."""
    ac = Arctic(f"lmdb://{tmpdir}")
    lib = ac.get_library("lib", create_if_missing=True)
    lib2 = ac.get_library("lib")
    lib3 = ac.get_library("lib")


@pytest.mark.skipif(sys.platform == "win32", reason="Windows pessimistic file-locking")
def test_arctic_instances_across_same_lmdb_multiprocessing(tmpdir):
    """Should not warn when across multiple processes."""
    ac = Arctic(f"lmdb://{tmpdir}")
    ac.create_library("test")
    ac["test"].write("a", pd.DataFrame())
    with mp.Pool(5) as p:
        p.starmap(create_arctic_instance, [(tmpdir, i) for i in range(20)])
