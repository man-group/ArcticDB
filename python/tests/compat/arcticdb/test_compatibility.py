import sys
import pytest
import pytz
import os

if sys.version_info >= (3, 9):
    import zoneinfo
from packaging import version
import pandas as pd
import numpy as np
from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal, assert_frame_equal_with_arrow
from arcticdb.options import ModifiableEnterpriseLibraryOption, OutputFormat
from arcticdb.toolbox.library_tool import LibraryTool
from tests.util.mark import ARCTICDB_USING_CONDA, MACOS_WHEEL_BUILD, ZONE_INFO_MARK
from arcticdb_ext.tools import StorageMover

from arcticdb.util.venv import CompatLibrary

if ARCTICDB_USING_CONDA:
    pytest.skip("These tests rely on pip based environments", allow_module_level=True)

if MACOS_WHEEL_BUILD:
    pytest.skip("We don't have previous versions of arcticdb pypi released for MacOS", allow_module_level=True)


@pytest.mark.parametrize("old_venv", ["latest"], indirect=True)  # adb version doesn't matter here
def test_comp_venv_loading_path(old_venv, tmp_path_factory):
    dir = tmp_path_factory.mktemp("comp_venv_loading_path")
    python_path = os.path.join(dir, "run.py")
    python_commands = [
        f"import sys",
        f"import os",
        f"from pathlib import Path",
        f"assert Path(os.getcwd()) == Path('{old_venv.path}')",
        f"assert Path(sys.path[1]) == Path('{old_venv.path}')",
        f"assert Path(sys.path[0]) == Path('{dir}')",
    ]
    with open(python_path, "w") as python_file:
        python_file.write("\n".join(python_commands))
    old_venv.execute_python_file(python_path)


def test_compat_write_read(old_venv_and_arctic_uri, lib_name, any_output_format):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        sym = "sym"
        df = pd.DataFrame({"col": [1, 2, 3]})
        df_2 = pd.DataFrame({"col": [4, 5, 6]})

        # Write to library using current version
        with compat.current_version() as curr:
            curr.lib.write(sym, df)

        # Check that dataframe is readable in old version and is unchanged
        compat.old_lib.assert_read(sym, df)

        # Write new version with old library
        compat.old_lib.write(sym, df_2)

        # Check that latest version is readable in current version
        with compat.current_version() as curr:
            read_df = curr.lib.read(sym, output_format=any_output_format).data
            assert_frame_equal_with_arrow(read_df, df_2)


def test_modify_old_library_option_with_current(old_venv_and_arctic_uri, lib_name):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        sym = "sym"
        df = pd.DataFrame({"col": [1, 2, 3]})
        df_2 = pd.DataFrame({"col": [4, 5, 6]})

        # Create library using old version and write to it
        compat.old_lib.write(sym, df)
        compat.old_lib.assert_read(sym, df)

        # Enable replication and background_deletion with current version
        with compat.current_version() as curr:
            expected_cfg = LibraryTool.read_unaltered_lib_cfg(curr.ac._library_manager, lib_name)
            expected_cfg.lib_desc.version.write_options.delayed_deletes = True
            expected_cfg.lib_desc.version.write_options.sync_passive.enabled = True

            curr.ac.modify_library_option(curr.lib, ModifiableEnterpriseLibraryOption.REPLICATION, True)
            curr.ac.modify_library_option(curr.lib, ModifiableEnterpriseLibraryOption.BACKGROUND_DELETION, True)

            cfg_after_modification = LibraryTool.read_unaltered_lib_cfg(curr.ac._library_manager, lib_name)
            assert cfg_after_modification == expected_cfg

        # We should still be able to read and write with the old version
        compat.old_lib.assert_read(sym, df)
        compat.old_lib.write(sym, df_2)
        compat.old_lib.assert_read(sym, df_2)

        # We verify that cfg is still what we expect after operations from old_venv
        with compat.current_version() as curr:
            cfg_after_use = LibraryTool.read_unaltered_lib_cfg(curr.ac._library_manager, lib_name)
            assert cfg_after_use == expected_cfg


def test_pandas_pickling(pandas_v1_venv, s3_ssl_disabled_storage, lib_name, any_output_format):
    arctic_uri = s3_ssl_disabled_storage.arctic_uri
    with CompatLibrary(pandas_v1_venv, arctic_uri, lib_name) as compat:
        # Create library using old version and write pickled Pandas 1 metadata
        compat.old_ac.execute(
            [
                f"""
from packaging import version
pandas_version = version.parse(pd.__version__)
assert pandas_version < version.Version("2.0.0")
df = pd.DataFrame({{"a": [1, 2, 3]}})
idx = pd.Int64Index([1, 2, 3])
df.index = idx
lib = ac.get_library("{lib_name}")
lib.write("sym", df, metadata={{"abc": df}})
"""
            ]
        )

        pandas_version = version.parse(pd.__version__)
        assert pandas_version >= version.Version("2.0.0")
        # Check we can read with Pandas 2
        with compat.current_version() as curr:
            sym = "sym"
            read_df = curr.lib.read(sym, output_format=any_output_format).metadata["abc"]
            expected_df = pd.DataFrame({"a": [1, 2, 3]})
            idx = pd.Index([1, 2, 3], dtype="int64")
            expected_df.index = idx
            assert_frame_equal(read_df, expected_df)


def test_compat_snapshot_metadata_read_write(old_venv_and_arctic_uri, lib_name):
    # Before v4.5.0 and after v5.2.1 we save metadata directly on the snapshot's segment header and we need to make
    # sure we can read snapshot metadata written by those versions, and that those versions can read snapshot metadata
    # written by the latest version.
    old_venv, arctic_uri = old_venv_and_arctic_uri

    adb_version = version.Version(old_venv.version)
    if version.Version("4.5.0") <= adb_version <= version.Version("5.2.1"):
        pytest.skip(
            reason="Versions between 4.5.0 and 5.2.1 store snapshot metadata in timeseries descriptor which is incompatible with any other versions"
        )
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        sym = "sym"
        df = pd.DataFrame({"col": [1, 2, 3]})
        snap_meta = {"key": "value"}
        snap = "snap"

        # Write snapshot metadata using current version
        with compat.current_version() as curr:
            curr.lib.write(sym, df)
            curr.lib.snapshot(snap, metadata=snap_meta)

        # Check we can read the snapshot metadata with an old client, and write snapshot metadata with the old client
        compat.old_lib.execute(
            [
                """
snaps = lib.list_snapshots()
meta = snaps["snap"]
assert meta is not None
assert meta == {"key": "value"}
lib.snapshot("old_snap", metadata={"old_key": "old_value"})
"""
            ]
        )

        # Check the modern client can read the snapshot metadata written by the old client
        with compat.current_version() as curr:
            snaps = curr.lib.list_snapshots()
            meta = snaps["old_snap"]
            assert meta == {"old_key": "old_value"}


def test_compat_snapshot_metadata_read(old_venv_and_arctic_uri, lib_name):
    # Between v4.5.0 and v5.2.1 we saved this metadata on the timeseries_descriptor user_metadata field
    # and we need to keep support for reading data serialized like that.
    # It was not possible to add support for those versions reading snapshot metadata written by current versions.
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        sym = "sym"
        df = pd.DataFrame({"col": [1, 2, 3]})

        # Write snapshot metadata using current version
        with compat.current_version() as curr:
            curr.lib.write(sym, df)

        # Check we can read the snapshot metadata with an old client, and write snapshot metadata with the old client
        compat.old_lib.execute(
            [
                """
snaps = lib.list_snapshots()
lib.snapshot("old_snap", metadata={"old_key": "old_value"})
"""
            ]
        )

        # Check the modern client can read the snapshot metadata written by the old client
        with compat.current_version() as curr:
            snaps = curr.lib.list_snapshots()
            meta = snaps["old_snap"]
            assert meta == {"old_key": "old_value"}


@ZONE_INFO_MARK
@pytest.mark.parametrize("zone_name", ["UTC", "America/New_York"])
def test_compat_timestamp_metadata(old_venv_and_arctic_uri, lib_name, zone_name):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        sym = "sym"
        df = pd.DataFrame({"col": [1]})
        timestamp_no_tz = pd.Timestamp(2025, 1, 1)
        timestamp_pytz = pd.Timestamp(2025, 1, 1, tz=pytz.timezone(zone_name))
        timestamp_zoneinfo = pd.Timestamp(2025, 1, 1, tz=zoneinfo.ZoneInfo(zone_name))

        # Write two versions with old arctic:
        # v0 - no timezone
        # v1 - pytz
        compat.old_lib.execute(
            [
                f"""
import pytz
timestamp_no_tz = pd.Timestamp(year=2025, month=1, day=1)
timestamp_pytz = pd.Timestamp(year=2025, month=1, day=1, tz=pytz.timezone({repr(zone_name)}))
lib.write({repr(sym)}, df, metadata=timestamp_no_tz)
lib.write_metadata({repr(sym)}, timestamp_pytz)
assert lib.read_metadata({repr(sym)}, as_of=0).metadata == timestamp_no_tz
assert lib.read_metadata({repr(sym)}, as_of=1).metadata == timestamp_pytz
"""
            ],
            dfs={"df": df},
        )

        # Write 3 more versions with current arctic:
        # v2 - no timezone
        # v3 - pytz
        # v4 - zoneinfo
        with compat.current_version() as curr:
            lib = curr.lib
            assert lib.read_metadata(sym, as_of=0).metadata == timestamp_no_tz
            assert lib.read_metadata(sym, as_of=1).metadata == timestamp_pytz
            lib.write_metadata(sym, timestamp_no_tz)  # v2
            lib.write_metadata(sym, timestamp_pytz)  # v3
            lib.write_metadata(sym, timestamp_zoneinfo)  # v4
            assert lib.read_metadata(sym, as_of=2).metadata == timestamp_no_tz
            assert lib.read_metadata(sym, as_of=3).metadata == timestamp_pytz
            assert lib.read_metadata(sym, as_of=4).metadata == timestamp_pytz

        compat.old_lib.execute(
            [
                f"""
import pytz
timestamp_no_tz = pd.Timestamp(year=2025, month=1, day=1)
timestamp_pytz = pd.Timestamp(year=2025, month=1, day=1, tz=pytz.timezone({repr(zone_name)}))
assert lib.read_metadata({repr(sym)}, as_of=2).metadata == timestamp_no_tz
assert lib.read_metadata({repr(sym)}, as_of=3).metadata == timestamp_pytz
assert lib.read_metadata({repr(sym)}, as_of=4).metadata == timestamp_pytz
"""
            ]
        )


def test_compat_read_incomplete(old_venv_and_arctic_uri, lib_name, any_output_format):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        sym = "sym"
        df = pd.DataFrame(
            {
                "col": np.arange(10),
                "float_col": np.arange(10, dtype=np.float64),
                "str_col": [f"str_{i}" for i in range(10)],
            },
            pd.date_range("2024-01-01", periods=10),
        )
        df_1 = df.iloc[:8]
        df_2 = df.iloc[8:]

        if version.Version(old_venv.version) >= version.Version("5.1.0"):
            # In version 5.1.0 (with commit a3b7545) we moved the streaming incomplete python API to the library tool.
            compat.old_lib.execute(
                [
                    """
lib_tool = lib.library_tool()
lib_tool.append_incomplete("sym", df_1)
lib_tool.append_incomplete("sym", df_2)
"""
                ],
                dfs={"df_1": df_1, "df_2": df_2},
            )
        else:
            compat.old_lib.execute(
                [
                    """
lib._nvs.append("sym", df_1, incomplete=True)
lib._nvs.append("sym", df_2, incomplete=True)
"""
                ],
                dfs={"df_1": df_1, "df_2": df_2},
            )

        with compat.current_version() as curr:
            curr.lib._nvs.set_output_format(any_output_format)
            read_df = curr.lib._nvs.read(sym, date_range=(None, None), incomplete=True).data
            assert_frame_equal_with_arrow(read_df, df)

            read_df = curr.lib._nvs.read(sym, date_range=(None, None), incomplete=True, columns=["float_col"]).data
            assert_frame_equal_with_arrow(read_df, df[["float_col"]])

            read_df = curr.lib._nvs.read(
                sym, date_range=(None, None), incomplete=True, columns=["float_col", "str_col"]
            ).data
            assert_frame_equal_with_arrow(read_df, df[["float_col", "str_col"]])

            read_df = curr.lib._nvs.read(
                sym,
                date_range=(pd.Timestamp(2024, 1, 5), pd.Timestamp(2024, 1, 9)),
                incomplete=True,
                columns=["float_col", "str_col"],
            ).data
            assert_frame_equal_with_arrow(read_df, df[["float_col", "str_col"]].iloc[4:9])


def test_storage_mover_clone_old_library(old_venv_and_arctic_uri, lib_name):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    dst_lib_name = "local.extra"
    with CompatLibrary(old_venv, arctic_uri, [lib_name, dst_lib_name]) as compat:
        df = pd.DataFrame({"col": [1, 2, 3]})
        df_2 = pd.DataFrame({"col": [4, 5, 6]})
        sym = "a"

        compat.old_libs[lib_name].write(sym, df)
        compat.old_libs[lib_name].write(sym, df_2)

        with compat.current_version() as curr:
            src_lib = curr.ac.get_library(lib_name)
            dst_lib = curr.ac.get_library(dst_lib_name)
            s = StorageMover(src_lib._nvs._library, dst_lib._nvs._library)
            s.clone_all_keys_for_symbol(sym, 1000)
            assert_frame_equal(src_lib.read(sym).data, dst_lib.read(sym).data)

        if (arctic_uri.startswith("s3") or arctic_uri.startswith("azure")) and "1.6.2" in old_venv.version:
            pytest.skip("Reading the new library on s3 or azure with 1.6.2 requires some work arounds")

        # Make sure that we can read the new lib with the old version
        compat.old_libs[dst_lib_name].assert_read(sym, df_2)


def test_compat_resample_updated_data(old_venv_and_arctic_uri, lib_name):
    # There was a bug where data written using update and old versions of ArcticDB produced data keys where the
    # end_index value was not 1 nanosecond larger than the last index value in the segment (as it should be), but
    # instead contained the start of the date_range passed into the update call. This violated an assumption in the
    # ResampleClause that all calls to ResampleClause::process would have at least one bucket to process, as
    # structure_for_processing assumed that the end_index value in a data key was accurate. This is a nonreg test to
    # prove this is fixed
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        sym = "sym"
        df_0 = pd.DataFrame(
            {"col": [0, 0]}, index=[pd.Timestamp("2025-01-02 00:02:00"), pd.Timestamp("2025-01-03 00:01:00")]
        )
        df_1 = pd.DataFrame(
            {"col": [1, 1]}, index=[pd.Timestamp("2025-01-03 00:04:00"), pd.Timestamp("2025-01-04 00:01:00")]
        )
        df_2 = pd.DataFrame(
            {"col": [2, 2]}, index=[pd.Timestamp("2025-01-05 22:00:00"), pd.Timestamp("2025-01-05 23:00:00")]
        )
        # Write to library using old version
        compat.old_lib.write(sym, df_0)
        compat.old_lib.update(sym, df_1, '(pd.Timestamp("2025-01-03 00:01:00"), None)')
        compat.old_lib.update(sym, df_2, '(pd.Timestamp("2025-01-04 00:01:00"), None)')

        # Resample using current version
        with compat.current_version() as curr:
            q = (
                QueryBuilder()
                .date_range((pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-04 23:59")))
                .resample("1D")
                .agg({"col": "sum"})
            )
            received_df = curr.lib.read(sym, query_builder=q).data
            expected_df = (
                curr.lib.read(sym, date_range=(pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-04 23:59")))
                .data.resample("1D")
                .agg({"col": "sum"})
            )
            assert_frame_equal(expected_df, received_df)


def test_compat_update_old_updated_data(pandas_v1_venv, s3_ssl_disabled_storage, lib_name):
    # There was a bug where data written using update and old versions of ArcticDB produced data keys where the
    # end_index value was not 1 nanosecond larger than the last index value in the segment (as it should be), but
    # instead contained the start of the date_range passed into the update call.
    # We want to verify updating such data works fine.
    arctic_uri = s3_ssl_disabled_storage.arctic_uri
    with CompatLibrary(pandas_v1_venv, arctic_uri, lib_name) as compat:
        sym = "sym"
        df_0 = pd.DataFrame(
            {"col": [0, 0]}, index=[pd.Timestamp("2025-01-02 00:02:00"), pd.Timestamp("2025-01-03 00:01:00")]
        )
        df_1 = pd.DataFrame(
            {"col": [1, 1]}, index=[pd.Timestamp("2025-01-03 00:04:00"), pd.Timestamp("2025-01-04 00:01:00")]
        )
        df_2 = pd.DataFrame(
            {"col": [2, 2]}, index=[pd.Timestamp("2025-01-05 22:00:00"), pd.Timestamp("2025-01-05 23:00:00")]
        )
        # Write to library using old version
        compat.old_lib.write(sym, df_0)
        compat.old_lib.update(sym, df_1, '(pd.Timestamp("2025-01-03 00:00:00"), None)')
        compat.old_lib.update(sym, df_2, '(pd.Timestamp("2025-01-04 00:00:00"), None)')

        # Resample using current version
        with compat.current_version() as curr:
            index_df = curr.lib._nvs.read_index(sym)
            # We verify the first two rows in the latest index have the broken behavior from the old version.
            assert len(index_df) == 3
            assert index_df["end_index"].iloc[0] == pd.Timestamp("2025-01-03 00:00:00")
            assert index_df["end_index"].iloc[1] == pd.Timestamp("2025-01-04 00:00:00")
            assert index_df["end_index"].iloc[2] == pd.Timestamp("2025-01-05 23:00:00") + pd.Timedelta(1, unit="ns")

            df_update = pd.DataFrame(
                {"col": [3, 3]}, index=[pd.Timestamp("2025-01-02 00:14:00"), pd.Timestamp("2025-01-04 00:00:00")]
            )
            curr.lib.update(sym, df_update)

            index_df = curr.lib._nvs.read_index(sym)
            # After the update intersecting problematic range update ranges are correct
            assert len(index_df) == 3
            assert index_df["end_index"].iloc[0] == pd.Timestamp("2025-01-02 00:02:00") + pd.Timedelta(1, unit="ns")
            assert index_df["end_index"].iloc[1] == pd.Timestamp("2025-01-04 00:00:00") + pd.Timedelta(1, unit="ns")
            assert index_df["end_index"].iloc[2] == pd.Timestamp("2025-01-05 23:00:00") + pd.Timedelta(1, unit="ns")

            result_df = curr.lib.read(sym).data
            expected_df = pd.DataFrame(
                {"col": [0, 3, 3, 2, 2]},
                index=[
                    pd.Timestamp("2025-01-02 00:02:00"),
                    pd.Timestamp("2025-01-02 00:14:00"),
                    pd.Timestamp("2025-01-04 00:00:00"),
                    pd.Timestamp("2025-01-05 22:00:00"),
                    pd.Timestamp("2025-01-05 23:00:00"),
                ],
            )
            assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "date_range",
    [
        (
            pd.Timestamp("2025-01-02 10:00:00"),
            pd.Timestamp("2025-01-02 12:00:00"),
        ),  # Empty result within problematic range
        (pd.Timestamp("2025-01-02 10:00:00"), None),  # Intersects problematic range at beginning
        (None, pd.Timestamp("2025-01-03 10:00:00")),  # Intersects with problematic range at end
    ],
)
@pytest.mark.parametrize("use_query_builder", [True, False])
def test_compat_date_range_old_updated_data(
    pandas_v1_venv, s3_ssl_disabled_storage, lib_name, date_range, use_query_builder, any_output_format
):
    # There was a bug where data written using update and old versions of ArcticDB produced data keys where the
    # end_index value was not 1 nanosecond larger than the last index value in the segment (as it should be), but
    # instead contained the start of the date_range passed into the update call.
    # We want to verify reading date range of the old broken end index values works.
    arctic_uri = s3_ssl_disabled_storage.arctic_uri
    with CompatLibrary(pandas_v1_venv, arctic_uri, lib_name) as compat:
        sym = "sym"
        df_0 = pd.DataFrame(
            {"col": [0, 0]}, index=[pd.Timestamp("2025-01-02 00:02:00"), pd.Timestamp("2025-01-03 00:01:00")]
        )
        df_1 = pd.DataFrame(
            {"col": [1, 1]}, index=[pd.Timestamp("2025-01-03 00:04:00"), pd.Timestamp("2025-01-04 00:01:00")]
        )
        df_2 = pd.DataFrame(
            {"col": [2, 2]}, index=[pd.Timestamp("2025-01-05 22:00:00"), pd.Timestamp("2025-01-05 23:00:00")]
        )
        # Write to library using old version
        compat.old_lib.write(sym, df_0)
        compat.old_lib.update(sym, df_1, '(pd.Timestamp("2025-01-03 00:00:00"), None)')
        compat.old_lib.update(sym, df_2, '(pd.Timestamp("2025-01-04 00:00:00"), None)')

        expected_df = pd.concat([df_0.iloc[:1], df_1.iloc[:1], df_2])
        filter_after_start = expected_df.index >= date_range[0] if date_range[0] else True
        filter_before_end = expected_df.index <= date_range[1] if date_range[1] else True
        expected_df = expected_df[filter_after_start & filter_before_end]

        # Resample using current version
        with compat.current_version() as curr:
            index_df = curr.lib._nvs.read_index(sym)
            # We verify the first two rows in the latest index have the broken behavior from the old version.
            assert len(index_df) == 3
            assert index_df["end_index"].iloc[0] == pd.Timestamp("2025-01-03 00:00:00")
            assert index_df["end_index"].iloc[1] == pd.Timestamp("2025-01-04 00:00:00")
            assert index_df["end_index"].iloc[2] == pd.Timestamp("2025-01-05 23:00:00") + pd.Timedelta(1, unit="ns")

            if use_query_builder:
                q = QueryBuilder().date_range(date_range)
                result = curr.lib.read(sym, query_builder=q, output_format=any_output_format).data
            else:
                result = curr.lib.read(sym, date_range=date_range, output_format=any_output_format).data
            assert_frame_equal_with_arrow(result, expected_df)


def test_norm_meta_column_and_index_names_write_old_read_new(old_venv_and_arctic_uri, lib_name):
    """Can a new venv read column and index names serialized by an old client?"""
    old_venv, arctic_uri = old_venv_and_arctic_uri

    sym = "sym"

    df = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02 00:01:00"), pd.Timestamp("2018-01-02 00:02:00")],
        data={"col_one": ["a", "b"], "col_two": ["c", "d"]},
    )
    df.index.set_names(
        ["col_one"], inplace=True
    )  # specifically testing an odd behaviour when an index name matches a column name

    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        compat.old_lib.execute(
            [
                'df = pd.DataFrame(index=[pd.Timestamp("2018-01-02 00:01:00"), pd.Timestamp("2018-01-02 00:02:00")], data={"col_one": ["a", "b"], "col_two": ["c", "d"]})',
                'df.index.set_names(["col_one"], inplace=True)',
                'lib.write("sym", df)',
            ]
        )

        with compat.current_version() as curr:
            res = curr.lib.get_description(sym)
            assert ["col_one", "col_two"] == [c.name for c in res.columns]
            assert res.index[0][0] == "col_one"
            assert_frame_equal(curr.lib.read(sym).data, df)


def test_norm_meta_column_and_index_names_write_new_read_old(old_venv_and_arctic_uri, lib_name):
    """Can an old venv read column and index names serialized by a new client?"""
    old_venv, arctic_uri = old_venv_and_arctic_uri

    start = pd.Timestamp("2018-01-02")
    index = pd.date_range(start=start, periods=4)

    df = pd.DataFrame(index=index, data={"col_one": [1, 2, 3, 4], "col_two": [1, 2, 3, 4]}, dtype=np.uint64)
    df.index.set_names(
        ["col_one"], inplace=True
    )  # specifically testing an odd behaviour when an index name matches a column name

    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        with compat.current_version() as curr:
            curr.lib.write("sym", df)

        compat.old_lib.execute(
            [
                f"desc = lib.get_description('sym')",
                "actual_desc_cols = [c.name for c in desc.columns]",
                "assert ['__col_col_one__0', 'col_two'] == actual_desc_cols, f'Actual columns were {actual_desc_cols}'",
                "actual_desc_index_name = desc.index[0][0]",
                "assert actual_desc_index_name == 'col_one', f'Actual index name was {actual_desc_index_name}'",
                "actual_df = lib.read('sym').data",
                "assert actual_df.index.name == 'col_one', f'Actual index name was {actual_df.index.name}'",
                "actual_col_names = list(actual_df.columns.values)",
                "assert actual_col_names == ['col_one', 'col_two'], f'Actual col names were {actual_col_names}'",
            ]
        )
