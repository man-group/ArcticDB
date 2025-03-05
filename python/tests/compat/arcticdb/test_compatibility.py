import pytest
from packaging import version
import pandas as pd
import numpy as np
from arcticdb.util.test import assert_frame_equal
from arcticdb.options import ModifiableEnterpriseLibraryOption
from arcticdb.toolbox.library_tool import LibraryTool
from tests.util.mark import ARCTICDB_USING_CONDA

from arcticdb.util.venv import CurrentVersion

if ARCTICDB_USING_CONDA:
    pytest.skip("These tests rely on pip based environments", allow_module_level=True)


def test_compat_write_read(old_venv_and_arctic_uri, lib_name):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    sym = "sym"
    df = pd.DataFrame({"col": [1, 2, 3]})
    df_2 = pd.DataFrame({"col": [4, 5, 6]})

    # Create library using old version
    old_ac = old_venv.create_arctic(arctic_uri)
    old_lib = old_ac.create_library(lib_name)

    # Write to library using current version
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
        expected_cfg = LibraryTool.read_unaltered_lib_cfg(curr.ac._library_manager, lib_name)
        expected_cfg.lib_desc.version.write_options.delayed_deletes = True
        expected_cfg.lib_desc.version.write_options.sync_passive.enabled = True

        curr.ac.modify_library_option(curr.lib, ModifiableEnterpriseLibraryOption.REPLICATION, True)
        curr.ac.modify_library_option(curr.lib, ModifiableEnterpriseLibraryOption.BACKGROUND_DELETION, True)

        cfg_after_modification = LibraryTool.read_unaltered_lib_cfg(curr.ac._library_manager, lib_name)
        assert(cfg_after_modification == expected_cfg)

    # We should still be able to read and write with the old version
    old_lib.assert_read(sym, df)
    old_lib.write(sym, df_2)
    old_lib.assert_read(sym, df_2)

    # We verify that cfg is still what we expect after operations from old_venv
    with CurrentVersion(arctic_uri, lib_name) as curr:
        cfg_after_use = LibraryTool.read_unaltered_lib_cfg(curr.ac._library_manager, lib_name)
        assert(cfg_after_use == expected_cfg)


def test_pandas_pickling(pandas_v1_venv, s3_ssl_disabled_storage, lib_name):
    arctic_uri = s3_ssl_disabled_storage.arctic_uri

    # Create library using old version and write pickled Pandas 1 metadata
    old_ac = pandas_v1_venv.create_arctic(arctic_uri)
    old_ac.create_library(lib_name)
    old_ac.execute([f"""
from packaging import version
pandas_version = version.parse(pd.__version__)
assert pandas_version < version.Version("2.0.0")
df = pd.DataFrame({{"a": [1, 2, 3]}})
idx = pd.Int64Index([1, 2, 3])
df.index = idx
lib = ac.get_library("{lib_name}")
lib.write("sym", df, metadata={{"abc": df}})
"""])

    pandas_version = version.parse(pd.__version__)
    assert pandas_version >= version.Version("2.0.0")
    # Check we can read with Pandas 2
    with CurrentVersion(arctic_uri, lib_name) as curr:
        sym = "sym"
        read_df = curr.lib.read(sym).metadata["abc"]
        expected_df = pd.DataFrame({"a": [1, 2, 3]})
        idx = pd.Index([1, 2, 3], dtype="int64")
        expected_df.index = idx
        assert_frame_equal(read_df, expected_df)


def test_compat_snapshot_metadata_read_write(old_venv_and_arctic_uri, lib_name):
    # Before v4.5.0 and after v5.2.1 we save metadata directly on the snapshot's segment header and we need to make
    # sure we can read snapshot metadata written by those versions, and that those versions can read snapshot metadata
    # written by the latest version.
    old_venv, arctic_uri = old_venv_and_arctic_uri
    old_ac = old_venv.create_arctic(arctic_uri)

    adb_version = version.Version(old_venv.version)
    if version.Version("4.5.0") <= adb_version <= version.Version("5.2.1"):
        pytest.skip(reason="Versions between 4.5.0 and 5.2.1 store snapshot metadata in timeseries descriptor which is incompatible with any other versions")

    sym = "sym"
    df = pd.DataFrame({"col": [1, 2, 3]})
    snap_meta = {"key": "value"}
    snap = "snap"

    old_lib = old_ac.create_library(lib_name)

    # Write snapshot metadata using current version
    with CurrentVersion(arctic_uri, lib_name) as curr:
        curr.lib.write(sym, df)
        curr.lib.snapshot(snap, metadata=snap_meta)

    # Check we can read the snapshot metadata with an old client, and write snapshot metadata with the old client
    old_lib.execute([
        """
snaps = lib.list_snapshots()
meta = snaps["snap"]
assert meta is not None
assert meta == {"key": "value"}
lib.snapshot("old_snap", metadata={"old_key": "old_value"})
        """
    ])

    # Check the modern client can read the snapshot metadata written by the old client
    with CurrentVersion(arctic_uri, lib_name) as curr:
        snaps = curr.lib.list_snapshots()
        meta = snaps["old_snap"]
        assert meta == {"old_key": "old_value"}


def test_compat_snapshot_metadata_read(old_venv_and_arctic_uri, lib_name):
    # Between v4.5.0 and v5.2.1 we saved this metadata on the timeseries_descriptor user_metadata field
    # and we need to keep support for reading data serialized like that.
    # It was not possible to add support for those versions reading snapshot metadata written by current versions.
    old_venv, arctic_uri = old_venv_and_arctic_uri
    sym = "sym"
    df = pd.DataFrame({"col": [1, 2, 3]})

    old_ac = old_venv.create_arctic(arctic_uri)
    old_lib = old_ac.create_library(lib_name)

    # Write snapshot metadata using current version
    with CurrentVersion(arctic_uri, lib_name) as curr:
        curr.lib.write(sym, df)

    # Check we can read the snapshot metadata with an old client, and write snapshot metadata with the old client
    old_lib.execute([
        """
snaps = lib.list_snapshots()
lib.snapshot("old_snap", metadata={"old_key": "old_value"})
        """
    ])

    # Check the modern client can read the snapshot metadata written by the old client
    with CurrentVersion(arctic_uri, lib_name) as curr:
        snaps = curr.lib.list_snapshots()
        meta = snaps["old_snap"]
        assert meta == {"old_key": "old_value"}


def test_compat_read_incomplete(old_venv_and_arctic_uri, lib_name):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    sym = "sym"
    df = pd.DataFrame({"col": np.arange(10), "float_col": np.arange(10, dtype=np.float64)}, pd.date_range("2024-01-01", periods=10))
    df_1 = df.iloc[:8]
    df_2 = df.iloc[8:]

    old_ac = old_venv.create_arctic(arctic_uri)
    old_lib = old_ac.create_library(lib_name)

    if version.Version(old_venv.version) >= version.Version("5.1.0"):
        # In version 5.1.0 (with commit a3b7545) we moved the streaming incomplete python API to the library tool.
        old_lib.execute([
            """
lib_tool = lib.library_tool()
lib_tool.append_incomplete("sym", df_1)
lib_tool.append_incomplete("sym", df_2)
            """
        ], dfs={"df_1": df_1, "df_2": df_2})
    else:
        old_lib.execute([
            """
lib._nvs.append("sym", df_1, incomplete=True)
lib._nvs.append("sym", df_2, incomplete=True)
            """
        ], dfs={"df_1": df_1, "df_2": df_2})


    with CurrentVersion(arctic_uri, lib_name) as curr:
        read_df = curr.lib._nvs.read(sym, date_range=(None, None), incomplete=True).data
        assert_frame_equal(read_df, df)

        read_df = curr.lib._nvs.read(sym, date_range=(None, None), incomplete=True, columns=["float_col"]).data
        assert_frame_equal(read_df, df[["float_col"]])
