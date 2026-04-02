import pandas as pd
import pytest

from arcticdb.util.test import config_context_multi
from arcticdb_ext.storage import KeyType
import arcticdb_ext.cpp_async as adb_async


@pytest.fixture
def missing_tdata_lib(s3_store_factory, sym):
    try:
        with config_context_multi(
            {"VersionStore.NumIOThreads": 100, "VersionStore.NumCPUThreads": 100}
        ):
            adb_async.reinit_task_scheduler()
            assert adb_async.io_thread_count() == 100
            assert adb_async.cpu_thread_count() == 100

            # dynamic_strings registers a PythonStringHandler, which causes handler_data
            # to be accessed during segment decoding on IO threads.
            lib = s3_store_factory(segment_row_size=1, dynamic_strings=True)
            num_rows = 100

            df = pd.DataFrame({"a": ["b"] * num_rows}, index=pd.date_range("2026-01-01", periods=num_rows, freq="s"))
            lib.write(sym, df)

            lib_tool = lib.library_tool()
            data_keys = lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, sym)
            assert len(data_keys) == num_rows

            # Triggers an IO exception in a folly task to verify proper exception handling.
            # folly::collect fails fast when a task raises an exception. If the caller's stack
            # unwinds while other folly tasks are still running, they may access destroyed
            # stack variables, causing a segfault
            lib_tool.remove(data_keys[0])

            yield lib, sym
    finally:
        adb_async.reinit_task_scheduler()


def test_read_with_missing_tdata_raises(missing_tdata_lib):
    lib, sym = missing_tdata_lib
    with pytest.raises(Exception):
        lib.read(sym)


def test_batch_read_with_missing_tdata_raises(missing_tdata_lib):
    lib, sym = missing_tdata_lib
    with pytest.raises(Exception):
        lib.batch_read([sym])
