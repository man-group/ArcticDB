"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

"""
This test demonstrates an issue where the symbol/version cache can become stale
in multi-process scenarios. When Process A has a cached version of the version
chain, and Process B writes a new version, Process A may fail to see the new
version (or raise an error) because it relies on stale cached data instead of
going to storage to resolve the current version.

The default cache reload interval is 2 seconds (VersionMap.ReloadInterval).
"""

import time
from multiprocessing import Process, Queue, Event

import pandas as pd
import pytest
from arcticdb import Arctic
from arcticdb_ext import set_config_int

from tests.util.mark import SLOW_TESTS_MARK


def create_test_df(version: int) -> pd.DataFrame:
    """Create a DataFrame with an identifiable version marker."""
    return pd.DataFrame({"version": [version], "data": [f"version_{version}"]})


def writer_process(arctic_uri: str, lib_name: str, symbol: str, version_to_write: int, start_event, done_queue: Queue):
    """
    Process that writes a new version of a symbol.
    Waits for start_event before writing to ensure proper sequencing.
    """
    try:
        # Create a fresh Arctic connection in this process
        ac = Arctic(arctic_uri)
        lib = ac[lib_name]

        # Wait for signal to write
        start_event.wait(timeout=30)

        # Write a new version
        df = create_test_df(version_to_write)
        lib.write(symbol, df)

        done_queue.put(("success", version_to_write))
    except Exception as e:
        done_queue.put(("error", str(e)))


def reader_process_with_warm_cache(
    arctic_uri: str,
    lib_name: str,
    symbol: str,
    cache_reload_interval_ns: int,
    reader_ready_event,
    writer_done_event,
    result_queue: Queue,
):
    """
    Process that:
    1. Sets a long cache reload interval
    2. Reads the symbol to warm the cache
    3. Signals that it's ready
    4. Waits for writer to complete
    5. Reads again (should see stale data due to cache)
    6. Reports what version it sees
    """
    try:
        # Set a very long cache reload interval to ensure cache staleness
        set_config_int("VersionMap.ReloadInterval", cache_reload_interval_ns)

        # Create a fresh Arctic connection in this process
        ac = Arctic(arctic_uri)
        lib = ac[lib_name]

        # First read - warms the cache
        first_read = lib.read(symbol)
        first_version = first_read.data["version"].iloc[0]

        # Signal that we're ready (cache is warm)
        reader_ready_event.set()

        # Wait for writer to complete
        writer_done_event.wait(timeout=30)

        # Small delay to ensure write is fully committed
        time.sleep(0.1)

        # Second read - should use cached data (showing the bug)
        # With a long cache interval, we should see the stale version
        second_read = lib.read(symbol)
        second_version = second_read.data["version"].iloc[0]

        result_queue.put({
            "status": "success",
            "first_version": first_version,
            "second_version": second_version,
            "cache_was_stale": first_version == second_version,
        })
    except Exception as e:
        result_queue.put({"status": "error", "error": str(e)})


@SLOW_TESTS_MARK
def test_read_latest_with_stale_cache_returns_cached_version(lmdb_storage, lib_name):
    """
    Documents that reading "latest" with a stale cache returns the cached latest version.

    This is expected behavior with caching - "latest" means "latest according to the cache".
    If you need to see the absolute latest version written by another process,
    either wait for the cache to expire (default 2 seconds) or request a specific version.

    Test scenario:
    1. Main process writes version 0
    2. Reader process reads (warms cache with version 0 as "latest")
    3. Writer process writes version 1
    4. Reader process reads "latest" again - sees cached version 0

    This is NOT a bug - it's the expected behavior of caching. The cache will eventually
    expire and the next read will see version 1.
    """
    symbol = "test_symbol"
    arctic_uri = lmdb_storage.arctic_uri

    # Create the library and write initial data
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    lib.write(symbol, create_test_df(0))

    # Set up synchronization
    reader_ready_event = Event()
    writer_done_event = Event()
    writer_result_queue = Queue()
    reader_result_queue = Queue()

    # Use a very long cache interval (10 seconds in nanoseconds) to demonstrate cache behavior
    cache_interval_ns = 10 * 1_000_000_000

    # Start the reader process first - it will warm its cache
    reader = Process(
        target=reader_process_with_warm_cache,
        args=(
            arctic_uri,
            lib_name,
            symbol,
            cache_interval_ns,
            reader_ready_event,
            writer_done_event,
            reader_result_queue,
        ),
    )
    reader.start()

    # Wait for reader to warm its cache
    assert reader_ready_event.wait(timeout=30), "Reader did not become ready in time"

    # Now start the writer - it will write a new version
    start_write_event = Event()
    start_write_event.set()  # Signal immediately
    writer = Process(
        target=writer_process,
        args=(arctic_uri, lib_name, symbol, 1, start_write_event, writer_result_queue),
    )
    writer.start()
    writer.join(timeout=30)

    # Get writer result
    writer_result = writer_result_queue.get(timeout=5)
    assert writer_result[0] == "success", f"Writer failed: {writer_result}"

    # Signal reader that writer is done
    writer_done_event.set()

    # Wait for reader to complete
    reader.join(timeout=30)

    # Get reader result
    reader_result = reader_result_queue.get(timeout=5)
    assert reader_result["status"] == "success", f"Reader failed: {reader_result}"

    print(f"Reader first version: {reader_result['first_version']}")
    print(f"Reader second version: {reader_result['second_version']}")
    print(f"Cache was stale: {reader_result['cache_was_stale']}")

    # Verify that the new version IS in storage
    fresh_ac = Arctic(arctic_uri)
    fresh_lib = fresh_ac[lib_name]
    latest = fresh_lib.read(symbol)
    assert latest.data["version"].iloc[0] == 1, "Version 1 should be in storage"

    # Expected behavior: Reading "latest" with a stale cache returns the cached version.
    # This is correct caching behavior - the cache will eventually expire.
    assert reader_result["cache_was_stale"], (
        "Expected reader to see cached version 0 when reading 'latest' "
        "(the cache hasn't expired yet), demonstrating caching behavior."
    )


@SLOW_TESTS_MARK
def test_specific_version_read_bypasses_stale_cache(lmdb_storage, lib_name):
    """
    Verifies that reading a specific version works even when the cache is stale.

    When reading a specific version that was written after the cache was populated,
    the code now automatically retries with cache bypass if the version is not found.

    Test scenario:
    1. Process A reads latest version (caches version chain ending at v0)
    2. Process B writes v1
    3. Process A tries to read v1 specifically
    4. First lookup fails (cache doesn't have v1), but retry with cache bypass finds it
    """
    symbol = "test_symbol"
    arctic_uri = lmdb_storage.arctic_uri

    # Create the library and write initial data
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    lib.write(symbol, create_test_df(0))

    def reader_tries_specific_version(
        arctic_uri: str,
        lib_name: str,
        symbol: str,
        cache_reload_interval_ns: int,
        reader_ready_event,
        writer_done_event,
        result_queue: Queue,
    ):
        """Reader that tries to read a specific version that was written after cache warm."""
        try:
            set_config_int("VersionMap.ReloadInterval", cache_reload_interval_ns)

            ac = Arctic(arctic_uri)
            lib = ac[lib_name]

            # Warm the cache by reading
            first_read = lib.read(symbol)
            first_version_id = first_read.version

            reader_ready_event.set()
            writer_done_event.wait(timeout=30)
            time.sleep(0.1)

            # Try to read version 1 (the one just written by writer)
            # This should fail if cache is stale
            try:
                version_1_read = lib.read(symbol, as_of=1)
                result_queue.put({
                    "status": "success",
                    "found_version_1": True,
                    "version_1_data": version_1_read.data["version"].iloc[0],
                })
            except Exception as e:
                result_queue.put({
                    "status": "success",
                    "found_version_1": False,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                })
        except Exception as e:
            result_queue.put({"status": "error", "error": str(e)})

    reader_ready_event = Event()
    writer_done_event = Event()
    writer_result_queue = Queue()
    reader_result_queue = Queue()

    cache_interval_ns = 10 * 1_000_000_000

    reader = Process(
        target=reader_tries_specific_version,
        args=(
            arctic_uri,
            lib_name,
            symbol,
            cache_interval_ns,
            reader_ready_event,
            writer_done_event,
            reader_result_queue,
        ),
    )
    reader.start()

    assert reader_ready_event.wait(timeout=30), "Reader did not become ready"

    start_write_event = Event()
    start_write_event.set()
    writer = Process(
        target=writer_process,
        args=(arctic_uri, lib_name, symbol, 1, start_write_event, writer_result_queue),
    )
    writer.start()
    writer.join(timeout=30)

    writer_result = writer_result_queue.get(timeout=5)
    assert writer_result[0] == "success", f"Writer failed: {writer_result}"

    writer_done_event.set()
    reader.join(timeout=30)

    reader_result = reader_result_queue.get(timeout=5)
    assert reader_result["status"] == "success", f"Reader process failed: {reader_result}"

    print(f"Reader found version 1: {reader_result.get('found_version_1')}")
    if not reader_result.get("found_version_1"):
        print(f"Error type: {reader_result.get('error_type')}")
        print(f"Error message: {reader_result.get('error_message')}")

    # The fix: When a specific version is not found in the cache, the code retries
    # with cache bypass, finding the version in storage
    assert reader_result.get("found_version_1"), (
        f"Reader should be able to read version 1 after writer process wrote it. "
        f"The cache bypass retry mechanism should find versions written by other processes. "
        f"Error: {reader_result.get('error_type')}: {reader_result.get('error_message')}."
    )


@SLOW_TESTS_MARK
@pytest.mark.parametrize("cache_interval_ns", [
    0,  # No caching - should always see latest
    10 * 1_000_000_000,  # 10 second cache - should see stale data
])
def test_cache_interval_affects_staleness(lmdb_storage, lib_name, cache_interval_ns):
    """
    Demonstrates how the cache reload interval setting affects visibility
    of cross-process writes:
    - With interval=0: Always sees latest version
    - With interval=10s: Sees stale version due to caching
    """
    symbol = "test_symbol"
    arctic_uri = lmdb_storage.arctic_uri

    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    lib.write(symbol, create_test_df(0))

    reader_ready_event = Event()
    writer_done_event = Event()
    writer_result_queue = Queue()
    reader_result_queue = Queue()

    reader = Process(
        target=reader_process_with_warm_cache,
        args=(
            arctic_uri,
            lib_name,
            symbol,
            cache_interval_ns,
            reader_ready_event,
            writer_done_event,
            reader_result_queue,
        ),
    )
    reader.start()

    assert reader_ready_event.wait(timeout=30)

    start_write_event = Event()
    start_write_event.set()
    writer = Process(
        target=writer_process,
        args=(arctic_uri, lib_name, symbol, 1, start_write_event, writer_result_queue),
    )
    writer.start()
    writer.join(timeout=30)

    writer_result = writer_result_queue.get(timeout=5)
    assert writer_result[0] == "success"

    writer_done_event.set()
    reader.join(timeout=30)

    reader_result = reader_result_queue.get(timeout=5)
    assert reader_result["status"] == "success"

    if cache_interval_ns == 0:
        # With no caching, reader should see the new version
        assert not reader_result["cache_was_stale"], (
            "With cache interval=0, reader should see the new version written by another process"
        )
    else:
        # With long cache interval, reader should see stale data (the bug)
        assert reader_result["cache_was_stale"], (
            "With cache interval=10s, reader should see stale data due to cached version chain"
        )
