from datetime import datetime, timedelta
import random
import pytest
import pandas as pd

from arcticdb.exceptions import LibraryNotFound

from arcticdb.options import LibraryOptions
from arcticdb.util.test import random_strings_of_length
from arcticdb.version_store.library import WritePayload
from benchmarks.bi_benchmarks import assert_frame_equal
from arcticdb.version_store import VersionedItem as PythonVersionedItem

from arcticdb.util.test import random_floats


def generate_dataframe(columns, dt, num_days, num_rows_per_day):
    dataframes = []
    for _ in range(num_days):
        index = pd.Index([dt + timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in columns}
        new_df = pd.DataFrame(data=vals, index=index)
        dataframes.append(new_df)
        dt = dt + timedelta(days=1)
    return pd.concat(dataframes)


def execute_test_library_creation_deletion(arctic_client, lib_name):
    ac = arctic_client
    ac.create_library(lib_name)
    try:
        with pytest.raises(ValueError):
            ac.create_library(lib_name)

        assert lib_name in ac.list_libraries()
        assert ac.has_library(lib_name)
        assert lib_name in ac
        if "mongo" in arctic_client.get_uri():
            # The mongo fixture uses PrefixingLibraryAdapterDecorator which leaks in this one case
            assert ac[lib_name].name.endswith(lib_name)
        else:
            assert ac[lib_name].name == lib_name

        ac.delete_library(lib_name)
        # Want this to be silent.
        ac.delete_library("library_that_does_not_exist")

        assert lib_name not in ac.list_libraries()
        with pytest.raises(LibraryNotFound):
            _lib = ac[lib_name]
        assert not ac.has_library(lib_name)
        assert lib_name not in ac
    finally:
        ac.delete_library(lib_name)

def execute_test_write_batch(library_factory):
    """Should be able to write different size of batch of data."""
    lib = library_factory(LibraryOptions(rows_per_segment=10))
    assert lib._nvs._lib_cfg.lib_desc.version.write_options.segment_row_size == 10
    num_days = 40
    num_symbols = 2
    dt = datetime(2019, 4, 8, 0, 0, 0)
    column_length = 4
    num_columns = 5
    num_rows_per_day = 1
    write_requests = []
    read_requests = []
    list_dataframes = {}
    columns = random_strings_of_length(num_columns, num_columns, True)
    for sym in range(num_symbols):
        df = generate_dataframe(random.sample(columns, num_columns), dt, num_days, num_rows_per_day)
        write_requests.append(WritePayload("symbol_" + str(sym), df, metadata="great_metadata" + str(sym)))
        read_requests.append("symbol_" + str(sym))
        list_dataframes[sym] = df

    write_batch_result = lib.write_batch(write_requests)
    assert all(type(w) == PythonVersionedItem for w in write_batch_result)

    read_batch_result = lib.read_batch(read_requests)
    for sym in range(num_symbols):
        original_dataframe = list_dataframes[sym]
        assert read_batch_result[sym].metadata == "great_metadata" + str(sym)
        assert_frame_equal(read_batch_result[sym].data, original_dataframe)

def execute_test_snapshots_and_deletes(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("my_symbol", df)
    lib.write("my_symbol2", df)

    lib.snapshot("test1")

    assert lib.list_snapshots() == {"test1": None}

    assert_frame_equal(lib.read("my_symbol", as_of="test1").data, df)

    lib.delete("my_symbol")
    lib.snapshot("snap_after_delete")
    assert sorted(lib.list_symbols("test1")) == ["my_symbol", "my_symbol2"]
    assert lib.list_symbols("snap_after_delete") == ["my_symbol2"]

    lib.delete_snapshot("test1")
    assert lib.list_snapshots() == {"snap_after_delete": None}
    assert lib.list_symbols() == ["my_symbol2"]        
       