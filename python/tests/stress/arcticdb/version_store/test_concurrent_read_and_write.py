import time
import pytest
from multiprocessing import Process, Queue

from arcticdb.util.test import config_context


@pytest.fixture
def writer_store(lmdb_version_store_delayed_deletes_v2):
    return lmdb_version_store_delayed_deletes_v2


@pytest.fixture
def reader_store(lmdb_version_store_delayed_deletes_v2):
    return lmdb_version_store_delayed_deletes_v2


@pytest.fixture
def eager_writer_store(lmdb_version_store_v2):
    return lmdb_version_store_v2


@pytest.fixture
def eager_reader_store(lmdb_version_store_v2):
    return lmdb_version_store_v2


def read_repeatedly(version_store, queue: Queue):
    while True:
        try:
            version_store.read("sym")
        except Exception as e:
            queue.put(e)
            raise  # don't get stuck in the while loop when we already know there's an issue
        time.sleep(0.1)


def write_repeatedly(version_store):
    while True:
        version_store.write("sym", [1, 2, 3], prune_previous_version=True)
        time.sleep(0.1)


def test_concurrent_read_write(writer_store, reader_store):
    """When using delayed deletes, a reader should always be able to read a symbol even if it is being modified
    and pruned by another process."""
    writer_store.write("sym", [1, 2, 3], prune_previous_version=True)
    exceptions_in_reader = Queue()
    reader = Process(target=read_repeatedly, args=(reader_store, exceptions_in_reader))
    writer = Process(target=write_repeatedly, args=(writer_store,))

    try:
        reader.start()
        writer.start()
        reader.join(5)
        writer.join(0.001)
    finally:
        writer.terminate()
        reader.terminate()

    assert exceptions_in_reader.empty()


def test_concurrent_read_write_eager_prune(eager_writer_store, eager_reader_store):
    """Without delayed deletes, prune_previous physically deletes superseded versions. The
    PrunePreviousProtectionSecs window still keeps recently superseded versions in storage long
    enough that a concurrent reader, which may have resolved a version just before it was pruned,
    can always read its data before it is reclaimed.

    The window must be enabled before the child processes are forked so they inherit it (the test
    session otherwise disables it globally via the _disable_prune_protection_window fixture)."""
    with config_context("VersionStore.PrunePreviousProtectionSecs", 600):
        eager_writer_store.write("sym", [1, 2, 3], prune_previous_version=True)
        exceptions_in_reader = Queue()
        reader = Process(target=read_repeatedly, args=(eager_reader_store, exceptions_in_reader))
        writer = Process(target=write_repeatedly, args=(eager_writer_store,))

        try:
            reader.start()
            writer.start()
            reader.join(5)
            writer.join(0.001)
        finally:
            writer.terminate()
            reader.terminate()

        assert exceptions_in_reader.empty()
