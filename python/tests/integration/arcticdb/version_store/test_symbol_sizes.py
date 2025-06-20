from multiprocessing import Queue, Process

import pytest
from arcticdb import LibraryOptions
from arcticdb.encoding_version import EncodingVersion
from arcticdb.util.test import sample_dataframe, config_context_multi
from arcticdb_ext.storage import KeyType
import arcticdb_ext.cpp_async as adb_async

from arcticdb.util.utils import delete_nvs
from tests.util.mark import REAL_S3_TESTS_MARK


@pytest.mark.storage
def test_symbol_sizes(basic_store):
    sym_names = []
    for i in range(5):
        df = sample_dataframe(100, i)
        sym = "sym_{}".format(i)
        sym_names.append(sym)
        basic_store.write(sym, df)

    sizes = basic_store.version_store.scan_object_sizes_by_stream()

    for s in sym_names:
        assert s in sizes

    assert sizes["sym_0"][KeyType.VERSION].compressed_size < 1000
    assert sizes["sym_0"][KeyType.TABLE_INDEX].compressed_size < 5000
    assert sizes["sym_0"][KeyType.TABLE_DATA].compressed_size < 15000


@pytest.mark.storage
def test_symbol_sizes_for_stream(basic_store):
    sym_names = []
    for i in range(5):
        df = sample_dataframe(100, i)
        sym = "sym_{}".format(i)
        sym_names.append(sym)
        basic_store.write(sym, df)

    last_data_size = -1
    for s in sym_names:
        sizes = basic_store.version_store.scan_object_sizes_for_stream(s)
        res = dict()
        for size in sizes:
            res[size.key_type] = (size.count, size.compressed_size)

        assert res[KeyType.VERSION][0] == 1
        assert res[KeyType.VERSION][1] < 1000
        assert res[KeyType.TABLE_INDEX][1] < 5000
        last_data_size = res[KeyType.TABLE_DATA][1]
        assert last_data_size < 15000

    # Write a symbol 10 times bigger than the ones above. Check that the size of its data key is plausible: roughly
    # 10x larger than those of the smaller writes.
    big_sym = "big_sym"
    df = sample_dataframe(1000, 4)
    basic_store.write(big_sym, df)
    sizes = basic_store.version_store.scan_object_sizes_for_stream(big_sym)
    big_data_sizes = [s.compressed_size for s in sizes if s.key_type == KeyType.TABLE_DATA]
    assert len(big_data_sizes) == 1
    big_data_size = big_data_sizes[0]
    assert 0.8 * 10 * last_data_size < big_data_size < 1.2 * 10 * last_data_size


@pytest.mark.storage
def test_symbol_sizes_big(basic_store):
    """
    Manual testing lines up well:

    In [11]: lib._nvs.version_store.scan_object_sizes_by_stream()
    Out[11]:
    {'sym': {<KeyType.VERSION: 4>: 1160,
      <KeyType.TABLE_INDEX: 3>: 2506,
      <KeyType.TABLE_DATA: 2>: 5553859}}

    In [12]: lib
    Out[12]: Library(Arctic(config=LMDB(path=/home/alex/source/ArcticDB/python/blah)), path=tst3, storage=lmdb_storage)

    (310) ➜  tst3 git:(size-by-symbol) ✗ du -h .
    5.5M    .
    (310) ➜  tst3 git:(size-by-symbol) ✗ pwd
    /home/alex/source/ArcticDB/python/blah/tst3
    """

    basic_store.write("sym", sample_dataframe(1000))

    sizes = basic_store.version_store.scan_object_sizes_by_stream()

    assert sizes["sym"][KeyType.VERSION].compressed_size < 1000
    assert sizes["sym"][KeyType.VERSION].count == 1

    assert sizes["sym"][KeyType.TABLE_INDEX].compressed_size < 5000
    assert sizes["sym"][KeyType.TABLE_INDEX].count == 1

    assert 50_000 < sizes["sym"][KeyType.TABLE_DATA].compressed_size < 85_000
    assert sizes["sym"][KeyType.TABLE_DATA].count == 1


@pytest.mark.storage
def test_symbol_sizes_multiple_versions(basic_store):
    basic_store.write("sym", sample_dataframe(1000))
    basic_store.write("sym", sample_dataframe(1000))

    sizes = basic_store.version_store.scan_object_sizes_by_stream()

    assert sizes
    assert sizes["sym"][KeyType.VERSION].count == 2
    assert sizes["sym"][KeyType.TABLE_INDEX].count == 2
    assert sizes["sym"][KeyType.TABLE_DATA].count == 2


@pytest.mark.storage
def test_scan_object_sizes(arctic_client, lib_name):
    try:
        lib = arctic_client.create_library(lib_name)
        basic_store = lib._nvs

        df = sample_dataframe(1000)
        basic_store.write("sym", df)
        basic_store.write("sym", df)

        sizes = basic_store.version_store.scan_object_sizes()

        res = dict()
        for s in sizes:
            res[s.key_type] = (s.count, s.compressed_size)

        assert KeyType.VERSION in res
        assert KeyType.TABLE_INDEX in res
        assert KeyType.TABLE_DATA in res
        assert KeyType.VERSION_REF in res

        assert res[KeyType.VERSION][0] == 2
        assert 1000 < res[KeyType.VERSION][1] < 2000
        assert res[KeyType.TABLE_INDEX][0] == 2
        assert 2000 < res[KeyType.TABLE_INDEX][1] < 4000
        assert res[KeyType.TABLE_DATA][0] == 2
        assert 100_000 < res[KeyType.TABLE_DATA][1] < 200_000
        assert res[KeyType.VERSION_REF][0] == 1
        assert 500 < res[KeyType.VERSION_REF][1] < 1500
    finally:
        arctic_client.delete_library(lib_name)


@pytest.mark.parametrize("storage, encoding_version_, num_io_threads, num_cpu_threads", [
    ("s3", EncodingVersion.V1, 1, 1),
    ("s3", EncodingVersion.V1, 10, 1),
    ("s3", EncodingVersion.V1, 1, 10),
])
def test_scan_object_sizes_threading(request, storage, encoding_version_, lib_name, num_io_threads, num_cpu_threads):
    """Some stress testing for scan_object_sizes, particularly against deadlocks. Use a small segment size so that
    there is some work to be done in parallel."""
    storage_fixture = request.getfixturevalue(storage + "_storage")
    arctic_client = storage_fixture.create_arctic(encoding_version=encoding_version_)
    try:
        with config_context_multi({"VersionStore.NumIOThreads": num_io_threads, "VersionStore.NumCPUThreads": num_cpu_threads}):
            adb_async.reinit_task_scheduler()
            if num_io_threads:
                assert adb_async.io_thread_count() == num_io_threads
            if num_cpu_threads:
                assert adb_async.cpu_thread_count() == num_cpu_threads

            lib = arctic_client.create_library(lib_name, library_options=LibraryOptions(rows_per_segment=5))
            try:
                basic_store = lib._nvs

                df = sample_dataframe(100)
                basic_store.write("sym", df)
                basic_store.write("sym", df)

                sizes = basic_store.version_store.scan_object_sizes()

                res = dict()
                for s in sizes:
                    res[s.key_type] = (s.count, s.compressed_size)

                assert KeyType.VERSION in res
                assert KeyType.TABLE_INDEX in res
                assert KeyType.TABLE_DATA in res
                assert KeyType.VERSION_REF in res
            finally:
                arctic_client.delete_library(lib_name)
    finally:
        adb_async.reinit_task_scheduler()


@pytest.mark.parametrize("storage, encoding_version_, num_io_threads, num_cpu_threads", [
    ("s3", EncodingVersion.V1, 1, 1),
    ("s3", EncodingVersion.V1, 10, 1),
    ("s3", EncodingVersion.V1, 1, 10),
])
def test_scan_object_sizes_by_stream_threading(request, storage, encoding_version_, lib_name, num_io_threads, num_cpu_threads):
    """Some stress testing for scan_object_sizes, particularly against deadlocks. Use a small segment size so that
    there is some work to be done in parallel."""
    storage_fixture = request.getfixturevalue(storage + "_storage")
    arctic_client = storage_fixture.create_arctic(encoding_version=encoding_version_)
    try:
        with config_context_multi({"VersionStore.NumIOThreads": num_io_threads, "VersionStore.NumCPUThreads": num_cpu_threads}):
            adb_async.reinit_task_scheduler()
            if num_io_threads:
                assert adb_async.io_thread_count() == num_io_threads
            if num_cpu_threads:
                assert adb_async.cpu_thread_count() == num_cpu_threads

            lib = arctic_client.create_library(lib_name, library_options=LibraryOptions(rows_per_segment=5))
            basic_store = lib._nvs

            df = sample_dataframe(100)
            basic_store.write("sym", df)
            basic_store.write("sym", df)

            sizes = basic_store.version_store.scan_object_sizes_by_stream()

            assert sizes["sym"][KeyType.VERSION].compressed_size < 2000
            assert sizes["sym"][KeyType.TABLE_INDEX].compressed_size < 5000
            assert sizes["sym"][KeyType.TABLE_DATA].compressed_size < 50_000
    finally:
        adb_async.reinit_task_scheduler()


@pytest.fixture
def reader_store(basic_store):
    return basic_store


@pytest.fixture
def writer_store(basic_store):
    return basic_store


def read_repeatedly(version_store, queue: Queue):
    while True:
        try:
            version_store.version_store.scan_object_sizes_by_stream()
            version_store.version_store.scan_object_sizes()
        except Exception as e:
            queue.put(e)
            raise  # don't get stuck in the while loop when we already know there's an issue


def write_repeatedly(version_store):
    while True:
        version_store.write("sym", [1, 2, 3], prune_previous_version=True)


def test_symbol_sizes_concurrent(reader_store, writer_store):
    """We should still return (possibly approximate) symbol sizes even when the keys we scan are being deleted by
    another process."""
    writer_store.write("sym", [1, 2, 3], prune_previous_version=True)
    exceptions_in_reader = Queue()
    reader = Process(target=read_repeatedly, args=(reader_store, exceptions_in_reader))
    writer = Process(target=write_repeatedly, args=(writer_store,))

    try:
        reader.start()
        writer.start()
        reader.join(2)
        writer.join(0.001)
    finally:
        writer.terminate()
        reader.terminate()

    assert exceptions_in_reader.empty()


@pytest.mark.parametrize("storage", ["s3_storage", "gcp_storage", "nfs_backed_s3_storage"])
def test_symbol_sizes_matches_boto(request, storage, lib_name):
    s3_storage = request.getfixturevalue(storage)
    bucket = s3_storage.get_boto_bucket()
    ac = s3_storage.create_arctic(encoding_version=EncodingVersion.V1)
    lib = ac.create_library(lib_name)._nvs

    try:
        df = sample_dataframe(100, 0)

        lib.write("s", df)

        sizes = lib.version_store.scan_object_sizes()
        assert len(sizes) == 10
        key_types = {s.key_type for s in sizes}
        assert key_types == {KeyType.TABLE_DATA, KeyType.TABLE_INDEX, KeyType.VERSION, KeyType.VERSION_REF, KeyType.APPEND_DATA,
                             KeyType.MULTI_KEY, KeyType.SNAPSHOT_REF, KeyType.LOG, KeyType.LOG_COMPACTED, KeyType.SYMBOL_LIST}

        data_size = [s for s in sizes if s.key_type == KeyType.TABLE_DATA][0]
        data_keys = [o for o in bucket.objects.all() if "test_symbol_sizes_matches_boto" in o.key and "/tdata/" in o.key]
        assert len(data_keys) == 1
        assert len(data_keys) == data_size.count
        assert data_keys[0].size == data_size.compressed_size
    finally:
        lib.version_store.clear()
        assert lib.version_store.empty()


def test_symbol_sizes_matches_azurite(azurite_storage, lib_name):
    factory = azurite_storage.create_version_store_factory(lib_name)
    df = sample_dataframe(100, 0)
    lib = factory()
    try:
        lib.write("s", df)

        blobs = azurite_storage.client.list_blobs()
        total_size = 0
        total_count = 0
        for blob in blobs:
            if lib_name.replace(".", "/") in blob.name and blob.container == azurite_storage.container and "/tdata/" in blob.name:
                total_size += blob.size
                total_count += 1

        sizes = lib.version_store.scan_object_sizes()

        data_size = [s for s in sizes if s.key_type == KeyType.TABLE_DATA][0]
        assert total_count == 1
        assert data_size.count == total_count
        assert data_size.compressed_size == total_size
    finally:
        delete_nvs(lib)
