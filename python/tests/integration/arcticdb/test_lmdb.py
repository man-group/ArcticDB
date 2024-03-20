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
import re
import sys
from pathlib import Path

from arcticdb import Arctic
from arcticdb.util.test import assert_frame_equal
from arcticdb.exceptions import LmdbMapFullError, StorageException
from arcticdb.util.test import get_wide_dataframe
import arcticdb.adapters.lmdb_library_adapter as la
from arcticdb.exceptions import LmdbOptionsError


def test_batch_read_only_segfault_regression(lmdb_storage):
    # See Github issue #520
    # This segfaults with arcticdb==1.5.0
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library("test_lib")
    df = pd.DataFrame({"a": list(range(100))}, index=list(range(100)))
    for i in range(100):
        lib.write(str(i), df, prune_previous_versions=True)

    # New Arctic instance is essential to repro the bug
    fresh_lib = lmdb_storage.create_arctic()["test_lib"]
    vis = fresh_lib.read_batch([str(i) for i in range(100)])  # used to crash
    assert len(vis) == 100
    assert_frame_equal(vis[0].data, df)


def test_library_deletion(tmp_path: Path):
    # See Github issue #517
    # Given
    ac = Arctic(f"lmdb://{tmp_path}/lmdb_instance")
    path = tmp_path / "lmdb_instance" / "test_lib"
    ac.create_library("test_lib")
    assert path.exists()

    ac.create_library("test_lib2")

    # When
    ac.delete_library("test_lib")

    # Then
    assert not path.exists()
    assert ac.list_libraries() == ["test_lib2"]


def test_library_deletion_leave_non_lmdb_files_alone(tmp_path: Path):
    # See Github issue #517
    # Given
    ac = Arctic(f"lmdb://{tmp_path}/lmdb_instance")
    path = tmp_path / "lmdb_instance" / "test_lib"
    ac.create_library("test_lib")
    assert path.exists()
    with open(os.path.join(path, "another"), "w") as f:
        f.write("blah")
    (path / "dir").mkdir()

    ac.create_library("test_lib2")

    # When
    ac.delete_library("test_lib")

    # Then
    assert path.exists()
    files = set(os.listdir(path))
    assert files == {"dir", "another"}
    assert ac.list_libraries() == ["test_lib2"]


def test_lmdb(lmdb_storage):
    # Github Issue #520 #889 - this used to segfault
    d = {
        "test1": pd.Timestamp("1979-01-18 00:00:00"),
        "test2": pd.Timestamp("1979-01-19 00:00:00"),
    }

    ac = lmdb_storage.create_arctic()
    lib = ac.create_library("model")

    for i in range(50):
        lib.write_pickle("test", d)
        lib = ac.get_library("model")
        lib.read("test").data


def test_lmdb_malloc_trim(lmdb_storage):
    # Make sure that the bindings for calling malloc_trim have been setup correctly
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library("test_lmdb_malloc_trim")
    lib._nvs.trim()


def test_lmdb_mapsize(tmp_path):
    # Given - tiny map size
    ac = Arctic(f"lmdb://{tmp_path}?map_size=1KB")

    # When
    with pytest.raises(LmdbMapFullError) as e:
        ac.create_library("test")
    # Then - even library creation fails so map size having an effect
    assert "MDB_MAP_FULL" in str(e.value)
    assert "E5003" in str(e.value)
    assert issubclass(e.type, LmdbMapFullError)

    # Given - larger map size
    ac = Arctic(f"lmdb://{tmp_path}?map_size=1MB")

    # When
    lib = ac["test"]
    df = get_wide_dataframe(size=1_000)
    lib.write("sym", df)

    # Then - operations succeed as usual


def test_lmdb_mapsize_write(version_store_factory):
    df = pd.DataFrame(np.random.randint(0, 100, size=(int(1e6), 4)), columns=list("ABCD"))
    lib = version_store_factory(lmdb_config={"map_size": 4096000})

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

def test_delete_library(lmdb_storage):
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library("library")
    ac.delete_library("library")
    with pytest.raises(StorageException) as e:
        lib.write("sym1", pd.DataFrame())

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


def test_warnings_arctic_instance(tmp_path, get_stderr):
    pytest.skip("This test is flaky due to trying to retrieve the log messages")
    ac = Arctic(f"lmdb://{tmp_path}")
    get_stderr()  # Clear buffer

    # should warn
    ac = Arctic(f"lmdb://{tmp_path}")
    assert re.search(r"W .*LMDB path at.*has already been opened in this process", get_stderr())

    del ac
    # should not warn
    ac = Arctic(f"lmdb://{tmp_path}")
    assert not re.search(r"W .*LMDB path at.*has already been opened in this process", get_stderr())


def test_warnings_library(lmdb_storage, get_stderr):
    """Should not warn - library caching prevents us opening LMDB env twice."""
    ac = lmdb_storage.create_arctic()
    lib = ac.get_library("lib", create_if_missing=True)
    lib2 = ac.get_library("lib")
    lib3 = ac.get_library("lib")
    assert " W " not in get_stderr()


@pytest.mark.skipif(sys.platform == "win32", reason="Windows pessimistic file-locking")
def test_arctic_instances_across_same_lmdb_multiprocessing(tmp_path):
    """Should not warn when across multiple processes."""
    ac = Arctic(f"lmdb://{tmp_path}")
    ac.create_library("test")
    ac["test"].write("a", pd.DataFrame())
    with mp.Pool(5) as p:
        p.starmap(create_arctic_instance, [(tmp_path, i) for i in range(20)])
