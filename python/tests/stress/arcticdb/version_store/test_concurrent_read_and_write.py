import time
import pytest
from multiprocessing import Process, Queue


@pytest.fixture
def writer_store(lmdb_version_store_delayed_deletes_v2):
    return lmdb_version_store_delayed_deletes_v2


@pytest.fixture
def reader_store(lmdb_version_store_delayed_deletes_v2):
    return lmdb_version_store_delayed_deletes_v2


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

    reader.start()
    writer.start()
    reader.join(5)
    writer.join(0.001)

    assert exceptions_in_reader.empty()
