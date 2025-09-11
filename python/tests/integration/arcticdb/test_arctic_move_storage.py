import shutil
import sys
import pytest
from pathlib import Path

from arcticdb import Arctic
from arcticdb.storage_fixtures.lmdb import LmdbStorageFixture
from arcticdb.util.test import get_wide_dataframe
from arcticdb.util.test import assert_frame_equal
from arcticdb.exceptions import LmdbMapFullError

from tests.util.mark import AZURE_TESTS_MARK


@pytest.fixture()
def lmdb_storage_factory():  # LmdbStorageFixtures aren't produced by a factory, however to fit the pattern:
    class Dummy:
        create_fixture = LmdbStorageFixture

    return Dummy()


@pytest.mark.parametrize(
    "storage_type, host_attr",
    [("lmdb", "db_dir"), ("s3", "bucket"), pytest.param("azurite", "container", marks=AZURE_TESTS_MARK)],
)
def test_move_storage(storage_type, host_attr, request):
    storage_factory = request.getfixturevalue(storage_type + "_storage_factory")

    with storage_factory.create_fixture() as dest_storage:
        with storage_factory.create_fixture() as source_storage:
            # Given - a library
            ac = source_storage.create_arctic()
            lib = ac.create_library("lib")

            df = get_wide_dataframe(size=100)
            lib.write("sym", df)

            dest_host = getattr(dest_storage, host_attr)
            assert dest_host not in lib.read("sym").host

            # When - we copy the underlying objects without using the Arctic API
            source_storage.copy_underlying_objects_to(dest_storage)

        # When - we leave the source_storage context, the storage should have been cleaned up
        assert not source_storage.create_arctic().list_libraries()

        # Then - should be readable at new location
        new_ac = dest_storage.create_arctic()
        assert new_ac.list_libraries() == ["lib"]
        lib = new_ac["lib"]
        assert lib.list_symbols() == ["sym"]
        assert_frame_equal(df, lib.read("sym").data)
        assert dest_host in lib.read("sym").host


def test_move_lmdb_library_map_size_reduction(tmp_path: Path):
    # Given - any LMDB library
    original = tmp_path / "original"
    original.mkdir()
    ac = Arctic(f"lmdb://{original}?map_size=1MB")
    ac.create_library("lib")
    lib = ac["lib"]

    df = get_wide_dataframe(size=100)
    lib.write("sym", df)

    # Free resources to release the filesystem lock held by Windows
    del lib
    del ac

    # When - we move the data
    dest = tmp_path / "dest"
    dest.mkdir()
    shutil.move(str(original / "_arctic_cfg"), dest)
    shutil.move(str(original / "lib"), dest)

    # Then - should be readable at new location as long as map size still big enough
    ac = Arctic(f"lmdb://{dest}?map_size=500KB")
    assert ac.list_libraries() == ["lib"]
    lib = ac["lib"]
    assert lib.list_symbols() == ["sym"]
    assert_frame_equal(df, lib.read("sym").data)
    assert "dest" in lib.read("sym").host
    assert "original" not in lib.read("sym").host
    lib.write("another_sym", df)

    del lib
    del ac

    # Then - read with new tiny map size
    # Current data should still be readable, expect modifications to fail
    ac = Arctic(f"lmdb://{dest}?map_size=1KB")
    try:
        assert ac.list_libraries() == ["lib"]
        lib = ac["lib"]
        assert set(lib.list_symbols()) == {"sym", "another_sym"}
        assert_frame_equal(df, lib.read("sym").data)

        with pytest.raises(LmdbMapFullError) as e:
            lib.write("another_sym", df)

        assert "MDB_MAP_FULL" in str(e.value)
        assert "E5003" in str(e.value)
        assert "-30792" in str(e.value)

        # stuff should still be readable despite the error
        assert_frame_equal(df, lib.read("sym").data)
    finally:
        ac.delete_library("lib")


def test_move_lmdb_library_map_size_increase(tmp_path: Path):
    # Given - any LMDB library
    original = tmp_path / "original"
    original.mkdir()
    ac = Arctic(f"lmdb://{original}?map_size=2MB")
    ac.create_library("lib")
    lib = ac["lib"]

    for i in range(10):
        df = get_wide_dataframe(size=100)
        lib.write(f"sym_{i}", df)

    # Free resources to release the filesystem lock held by Windows
    del lib
    del ac

    # When - we move the data
    dest = tmp_path / "dest"
    dest.mkdir()
    shutil.move(str(original / "_arctic_cfg"), dest)
    shutil.move(str(original / "lib"), dest)

    # Then - should be readable at new location as long as map size made big enough
    # 20 writes of this size would fail with the old 2MB map size
    ac = Arctic(f"lmdb://{dest}?map_size=10MB")
    lib = ac["lib"]
    for i in range(20):
        df = get_wide_dataframe(size=100)
        lib.write(f"more_sym_{i}", df)
    assert len(lib.list_symbols()) == 30
    ac.delete_library("lib")
