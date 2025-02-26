from multiprocessing import Queue, Process

import pytest
from arcticdb.util.test import sample_dataframe
from arcticdb_ext.storage import KeyType


def test_symbol_sizes(basic_store):
    sizes = basic_store.version_store.scan_object_sizes_by_stream()
    assert len(sizes) == 1
    assert "__symbols__" in sizes

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
    assert sizes["sym"][KeyType.VERSION].uncompressed_size < 200
    assert sizes["sym"][KeyType.VERSION].count == 1

    assert sizes["sym"][KeyType.TABLE_INDEX].compressed_size < 5000
    assert sizes["sym"][KeyType.TABLE_INDEX].uncompressed_size < 2500
    assert sizes["sym"][KeyType.TABLE_INDEX].count == 1

    assert 50_000 < sizes["sym"][KeyType.TABLE_DATA].compressed_size < 85_000
    assert 60_000 < sizes["sym"][KeyType.TABLE_DATA].uncompressed_size < 150_000
    assert sizes["sym"][KeyType.TABLE_DATA].count == 1


def test_symbol_sizes_multiple_versions(basic_store):
    basic_store.write("sym", sample_dataframe(1000))
    basic_store.write("sym", sample_dataframe(1000))

    sizes = basic_store.version_store.scan_object_sizes_by_stream()

    assert sizes
    assert sizes["sym"][KeyType.VERSION].count == 2
    assert sizes["sym"][KeyType.TABLE_INDEX].count == 2
    assert sizes["sym"][KeyType.TABLE_DATA].count == 2
    assert 100_000 < sizes["sym"][KeyType.TABLE_DATA].uncompressed_size < 250_000


def test_scan_object_sizes(basic_store):
    df = sample_dataframe(1000)
    basic_store.write("sym", df)

    sizes = basic_store.version_store.scan_object_sizes()

    assert len(sizes) == 5
    assert sizes[KeyType.VERSION].compressed_size < 1000
    assert sizes[KeyType.VERSION].uncompressed_size < 200

    assert sizes[KeyType.TABLE_INDEX].compressed_size < 2500
    assert sizes[KeyType.TABLE_INDEX].uncompressed_size < 1000

    assert 15_000 < sizes[KeyType.TABLE_DATA].compressed_size < 100_000
    assert 50_000 < sizes[KeyType.TABLE_DATA].uncompressed_size < 150_000

    assert sizes[KeyType.VERSION].count == 1
    assert sizes[KeyType.TABLE_INDEX].count == 1
    assert sizes[KeyType.TABLE_DATA].count == 1


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
        reader.join(1)
        writer.join(0.001)
    finally:
        writer.terminate()
        reader.terminate()

    assert exceptions_in_reader.empty()
