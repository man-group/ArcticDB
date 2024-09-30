import numpy as np
import pandas as pd
import pytest
from arcticdb.arctic import Arctic
from arcticdb.util.test import assert_frame_equal
from arcticdb_ext import set_config_int
from arcticdb.options import ModifiableEnterpriseLibraryOption

class CurrentVersion:
    """
    For many of the compatibility tests we need to maintain a single open connection to the library.
    For example LMDB on Windows starts to fail if we at the same time we use an old_venv and current connection.

    So we use `with CurrentVersion` construct to ensure we delete all our outstanding references to the library.
    """
    def __init__(self, uri, lib_name):
        self.uri = uri
        self.lib_name = lib_name

    def __enter__(self):
        self.ac = Arctic(self.uri)
        self.lib = self.ac.get_library(self.lib_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.lib
        del self.ac

def test_compat_write_read(old_venv_and_arctic_uri, lib_name):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    sym = "sym"
    df = pd.DataFrame({"col": [1, 2, 3]})
    df_2 = pd.DataFrame({"col": [4, 5, 6]})

    # Create library using old version
    old_ac = old_venv.create_arctic(arctic_uri)
    old_lib = old_ac.create_library(lib_name)

    # Write to library using current version
    set_config_int("VersionMap.ReloadInterval", 0) # We disable the cache to be able to read the data written from old_venv
    with CurrentVersion(arctic_uri, lib_name) as curr:
        curr.lib.write(sym, df)

    # Check that dataframe is readable in old version and is unchanged
    old_lib.assert_read(sym, df)

    # Write new version with old library
    old_lib.write(sym, df_2)

    # Check that latest version is readable in current version
    with CurrentVersion(arctic_uri, lib_name) as curr:
        read_df = curr.lib.read(sym).data
        assert_frame_equal(read_df, df_2)

def test_modify_old_library_option_with_current(old_venv_and_arctic_uri, lib_name):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    sym = "sym"
    df = pd.DataFrame({"col": [1, 2, 3]})
    df_2 = pd.DataFrame({"col": [4, 5, 6]})

    # Create library using old version and write to it
    old_ac = old_venv.create_arctic(arctic_uri)
    old_lib = old_ac.create_library(lib_name)
    old_lib.write(sym, df)
    old_lib.assert_read(sym, df)

    # Enable replication and background_deletion with current version
    with CurrentVersion(arctic_uri, lib_name) as curr:
        curr.ac.modify_library_option(curr.lib, ModifiableEnterpriseLibraryOption.REPLICATION, True)
        curr.ac.modify_library_option(curr.lib, ModifiableEnterpriseLibraryOption.BACKGROUND_DELETION, True)

    # We should still be able to read and write with the old version
    old_lib.assert_read(sym, df)
    old_lib.write(sym, df_2)
    old_lib.assert_read(sym, df_2)
