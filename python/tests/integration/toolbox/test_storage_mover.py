import numpy as np
import pytest

from hypothesis import given, strategies as st, settings
from arcticdb.config import Defaults
from arcticdb.version_store.helper import ArcticMemoryConfig, get_lib_cfg, add_lmdb_library_to_env
from arcticdb.toolbox.library_tool import KeyType
from arcticdb.toolbox.storage import SymbolVersionsPair
from arcticdb_ext.tools import StorageMover
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from arcticdb.util.test import sample_dataframe
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
import hypothesis
import sys

from arcticdb_ext import (
    set_config_int,
    unset_config_int,
)
from arcticdb_ext.cpp_async import reinit_task_scheduler

# configure_test_logger("DEBUG")


def create_local_lmdb_cfg(lib_name=Defaults.LIB, db_dir=Defaults.DATA_DIR, description=None):
    cfg = EnvironmentConfigsMap()
    add_lmdb_library_to_env(cfg, lib_name=lib_name, env_name=Defaults.ENV, db_dir=db_dir, description=description)
    return cfg


@pytest.fixture
def arctidb_native_local_lib_cfg_extra(tmpdir):
    def create():
        return create_local_lmdb_cfg(lib_name="local.extra", db_dir=str(tmpdir))

    return create


@pytest.fixture
def arctidb_native_local_lib_cfg(tmpdir):
    def create(lib_name):
        return create_local_lmdb_cfg(lib_name=lib_name, db_dir=str(tmpdir))

    return create


def create_default_config():
    return create_local_lmdb_cfg()


def add_data(version_store):
    version_store.write("symbol", sample_dataframe())
    version_store.write("pickled", {"a": 1}, pickle_on_failure=True)
    version_store.snapshot("mysnap")
    version_store.write("rec_norm", data={"a": np.arange(5), "b": np.arange(8), "c": None}, recursive_normalizers=True)
    version_store.write("symbol", sample_dataframe())
    version_store.snapshot("mysnap2")


def compare_two_libs(lib1, lib2):
    ver1 = lib1.list_versions()
    ver2 = lib2.list_versions()

    print(ver1)
    print(ver2)

    assert len(lib1.list_versions()) == len(lib2.list_versions())
    assert lib1.list_versions() == lib2.list_versions()
    assert lib1.list_snapshots() == lib2.list_snapshots()

    assert_frame_equal(lib1.read("symbol", as_of=0).data, lib2.read("symbol", as_of=0).data)
    assert_frame_equal(lib1.read("symbol", as_of=1).data, lib2.read("symbol", as_of=1).data)
    assert_frame_equal(lib1.read("symbol", as_of="mysnap").data, lib2.read("symbol", as_of="mysnap").data)
    assert_frame_equal(lib1.read("symbol", as_of="mysnap2").data, lib2.read("symbol", as_of="mysnap2").data)

    assert lib1.read("pickled").data == lib2.read("pickled").data
    assert lib1.read("pickled", as_of="mysnap").data == lib2.read("pickled", as_of="mysnap").data
    assert lib1.read("pickled", as_of="mysnap2").data == lib2.read("pickled", as_of="mysnap2").data

    assert lib1.read("rec_norm").data.keys() == lib2.read("rec_norm").data.keys()
    assert all(lib1.read("rec_norm").data["a"] == lib2.read("rec_norm").data["a"])
    assert all(lib1.read("rec_norm").data["b"] == lib2.read("rec_norm").data["b"])
    assert lib1.read("rec_norm").data["c"] == lib2.read("rec_norm").data["c"]
    assert lib1.read("rec_norm", as_of="mysnap2").data.keys() == lib2.read("rec_norm", as_of="mysnap2").data.keys()


@pytest.fixture(
    params=[True],
)
def check_single_threaded(request):
    if request.param:
        set_config_int("VersionStore.NumIOThreads", 1)
        set_config_int("VersionStore.NumCPUThreads", 1)
        reinit_task_scheduler()

    yield request.param

    if request.param:
        unset_config_int("VersionStore.NumIOThreads")
        unset_config_int("VersionStore.NumCPUThreads")
        reinit_task_scheduler()


def test_storage_mover_single_go(check_single_threaded, lmdb_version_store_v1, arctidb_native_local_lib_cfg_extra):
    add_data(lmdb_version_store_v1)
    arctic = ArcticMemoryConfig(arctidb_native_local_lib_cfg_extra(), env=Defaults.ENV)
    lib_cfg = get_lib_cfg(arctic, Defaults.ENV, "local.extra")
    lib_cfg.version.symbol_list = True
    dst_lib = arctic["local.extra"]

    s = StorageMover(lmdb_version_store_v1._library, dst_lib._library)
    s.go()

    compare_two_libs(lmdb_version_store_v1, dst_lib)


def test_storage_mover_key_by_key(check_single_threaded, lmdb_version_store_v1, arctidb_native_local_lib_cfg_extra):
    add_data(lmdb_version_store_v1)
    arctic = ArcticMemoryConfig(arctidb_native_local_lib_cfg_extra(), env=Defaults.ENV)
    lib_cfg = get_lib_cfg(arctic, Defaults.ENV, "local.extra")
    lib_cfg.version.symbol_list = True
    dst_lib = arctic["local.extra"]

    s = StorageMover(lmdb_version_store_v1._library, dst_lib._library)
    all_keys = s.get_all_source_keys()
    for key in all_keys:
        s.write_keys_from_source_to_target([key], 2)

    compare_two_libs(lmdb_version_store_v1, dst_lib)


@pytest.mark.xfail(sys.platform == "win32", reason="Numpy strings are not implemented for Windows")
@pytest.mark.parametrize("versions_to_delete", [0, [0, 1]])
def test_storage_mover_symbol_tree(
    check_single_threaded,
    arctidb_native_local_lib_cfg_extra,
    arctidb_native_local_lib_cfg,
    lib_name,
    versions_to_delete,
):
    col_per_group = 5
    row_per_segment = 10
    local_lib_cfg = arctidb_native_local_lib_cfg(lib_name)
    lib = local_lib_cfg.env_by_id[Defaults.ENV].lib_by_path[lib_name]
    lib.version.write_options.column_group_size = col_per_group
    lib.version.write_options.segment_row_size = row_per_segment
    lib.version.symbol_list = True
    lmdb_version_store_symbol_list = ArcticMemoryConfig(local_lib_cfg, Defaults.ENV)[lib_name]

    lmdb_version_store_symbol_list.write("symbol", sample_dataframe(), metadata="yolo")
    lmdb_version_store_symbol_list.write("symbol", sample_dataframe(), metadata="yolo2")
    if isinstance(versions_to_delete, list):
        lmdb_version_store_symbol_list.write("snapshot_test", 0)
        lmdb_version_store_symbol_list.write("snapshot_test", 1)
    else:
        lmdb_version_store_symbol_list.write("snapshot_test", 1)

    lmdb_version_store_symbol_list.snapshot("my_snap")
    lmdb_version_store_symbol_list.snapshot("my_snap2")
    lmdb_version_store_symbol_list.snapshot("snapshot_test", 2)
    if isinstance(versions_to_delete, list):
        lmdb_version_store_symbol_list.delete_versions("snapshot_test", versions_to_delete)
    else:
        lmdb_version_store_symbol_list.delete_version("snapshot_test", versions_to_delete)
    lmdb_version_store_symbol_list.write("pickled", {"a": 1}, metadata="cantyolo", pickle_on_failure=True)
    lmdb_version_store_symbol_list.write("pickled", {"b": 1}, metadata="cantyolo2", pickle_on_failure=True)
    lmdb_version_store_symbol_list.write("pickled", {"c": 1}, metadata="yoloded", pickle_on_failure=True)
    lmdb_version_store_symbol_list.write(
        "rec_norm",
        data={"a": np.arange(1000), "b": np.arange(8000), "c": None},
        metadata="realyolo",
        recursive_normalizers=True,
    )
    lmdb_version_store_symbol_list.write(
        "rec_norm",
        data={"e": np.arange(1000), "f": np.arange(8000), "g": None},
        metadata="realyolo2",
        recursive_normalizers=True,
    )

    lmdb_version_store_symbol_list.write("dup_data", np.array(["YOLO"] * 10000))

    arctic = ArcticMemoryConfig(arctidb_native_local_lib_cfg_extra(), env=Defaults.ENV)
    lib_cfg = get_lib_cfg(arctic, Defaults.ENV, "local.extra")
    lib_cfg.version.symbol_list = True
    dst_lib = arctic["local.extra"]

    s = StorageMover(lmdb_version_store_symbol_list._library, dst_lib._library)
    sv1 = SymbolVersionsPair("symbol", [1, 0])
    sv2 = SymbolVersionsPair("pickled", [2, 0])
    sv3 = SymbolVersionsPair("rec_norm", [1, 0])
    sv4 = SymbolVersionsPair("dup_data", [0])
    sv5 = SymbolVersionsPair("snapshot_test", ["my_snap", "my_snap2"])
    res = s.write_symbol_trees_from_source_to_target([sv1, sv2, sv3, sv4, sv5], False)
    assert len(res) == 5
    for r in res:
        for v in res[r]:
            assert type(res[r][v]) == int

    assert len(dst_lib.list_versions()) == 8
    assert_frame_equal(lmdb_version_store_symbol_list.read("symbol").data, dst_lib.read("symbol").data)
    assert_frame_equal(lmdb_version_store_symbol_list.read("symbol", 0).data, dst_lib.read("symbol", 0).data)
    assert lmdb_version_store_symbol_list.read("symbol").metadata == dst_lib.read("symbol").metadata
    assert lmdb_version_store_symbol_list.read("symbol", 0).metadata == dst_lib.read("symbol", 0).metadata

    assert lmdb_version_store_symbol_list.read("pickled").data == dst_lib.read("pickled").data
    assert lmdb_version_store_symbol_list.read("pickled", 0).data == dst_lib.read("pickled", 0).data
    assert lmdb_version_store_symbol_list.read("pickled").metadata == dst_lib.read("pickled").metadata
    assert lmdb_version_store_symbol_list.read("pickled", 0).metadata == dst_lib.read("pickled", 0).metadata

    def comp_dict(d1, d2):
        assert len(d1) == len(d2)
        for k in d1:
            if isinstance(d1[k], np.ndarray):
                assert (d1[k] == d2[k]).all()
            else:
                assert d1[k] == d2[k]

    comp_dict(lmdb_version_store_symbol_list.read("rec_norm").data, dst_lib.read("rec_norm").data)
    comp_dict(lmdb_version_store_symbol_list.read("rec_norm", 0).data, dst_lib.read("rec_norm", 0).data)
    assert lmdb_version_store_symbol_list.read("rec_norm").metadata == dst_lib.read("rec_norm").metadata
    assert lmdb_version_store_symbol_list.read("rec_norm", 0).metadata == dst_lib.read("rec_norm", 0).metadata

    np.testing.assert_equal(lmdb_version_store_symbol_list.read("dup_data").data, dst_lib.read("dup_data").data)
    assert lmdb_version_store_symbol_list.read("dup_data").metadata == dst_lib.read("dup_data").metadata

    assert lmdb_version_store_symbol_list.read("snapshot_test", "my_snap").data, dst_lib.read("snapshot_test", 0).data

    lmdb_version_store_symbol_list.write("new_symbol", 1)
    lmdb_version_store_symbol_list.snapshot("new_snap")
    lmdb_version_store_symbol_list.write("new_symbol", 2)
    lmdb_version_store_symbol_list.snapshot("new_snap2")
    lmdb_version_store_symbol_list.write("new_symbol", 3)
    lmdb_version_store_symbol_list.delete_version("new_symbol", 1)
    sv6 = SymbolVersionsPair("new_symbol", [2, 0, "new_snap", "new_snap2"])
    dst_lib.write("new_symbol", 0)

    res = s.write_symbol_trees_from_source_to_target([sv6], True)
    assert len(res) == 1
    assert "new_symbol" in res
    assert res["new_symbol"][2] == 3
    assert res["new_symbol"][0] == 1
    assert res["new_symbol"]["new_snap"] == 1
    assert res["new_symbol"]["new_snap2"] == 2

    assert dst_lib.read("new_symbol", 0).data == 0
    assert dst_lib.read("new_symbol", 1).data == 1
    assert dst_lib.read("new_symbol", 2).data == 2
    assert dst_lib.read("new_symbol", 3).data == 3


def test_storage_mover_and_key_checker(
    check_single_threaded, lmdb_version_store_v1, arctidb_native_local_lib_cfg_extra
):
    add_data(lmdb_version_store_v1)
    arctic = ArcticMemoryConfig(arctidb_native_local_lib_cfg_extra(), env=Defaults.ENV)
    lib_cfg = get_lib_cfg(arctic, Defaults.ENV, "local.extra")
    lib_cfg.version.symbol_list = True
    dst_lib = arctic["local.extra"]

    s = StorageMover(lmdb_version_store_v1._library, dst_lib._library)
    s.go()

    keys = s.get_keys_in_source_only()
    assert len(keys) == 0


def test_storage_mover_clone_keys_for_symbol(
    check_single_threaded, lmdb_version_store_v1, arctidb_native_local_lib_cfg_extra
):
    add_data(lmdb_version_store_v1)
    lmdb_version_store_v1.write("a", 1)
    lmdb_version_store_v1.write("a", 2)
    lmdb_version_store_v1.write("b", 1)
    arctic = ArcticMemoryConfig(arctidb_native_local_lib_cfg_extra(), env=Defaults.ENV)
    lib_cfg = get_lib_cfg(arctic, Defaults.ENV, "local.extra")
    lib_cfg.version.symbol_list = True
    dst_lib = arctic["local.extra"]

    s = StorageMover(lmdb_version_store_v1._library, dst_lib._library)
    s.clone_all_keys_for_symbol("a", 1000)
    assert dst_lib.read("a").data == 2


@pytest.fixture()
def lib_with_gaps_and_reused_keys(version_store_factory):
    lib = version_store_factory(name="source", de_duplication=True, col_per_group=2, segment_row_size=2)

    lib.write("x", 0)
    lib.write("x", 1)
    lib.write("x", 2)
    lib.snapshot("s2")
    lib.write("x", DataFrame({"c": [0, 1]}, index=[0, 1]))
    lib.write("x", DataFrame({"c": list(range(5))}, index=list(range(5))), prune_previous_version=True)  # 2 slices
    lib.write("x", 5)
    lib.delete_version("x", 5)
    lib.write("x", 6)

    return lib


@pytest.mark.parametrize("mode", ("check assumptions", "go", "no force"))
def test_correct_versions_in_destination(
    mode, check_single_threaded, lib_with_gaps_and_reused_keys, lmdb_version_store_v1
):
    s = StorageMover(lib_with_gaps_and_reused_keys._library, lmdb_version_store_v1._library)
    if mode == "check assumptions":
        check = lib_with_gaps_and_reused_keys
    elif mode == "go":
        s.go()
        check = lmdb_version_store_v1
    else:
        s.write_symbol_trees_from_source_to_target([SymbolVersionsPair("x", ["s2", 4, 6])], False)
        check = lmdb_version_store_v1

    lt = check.library_tool()

    assert {vi["version"] for vi in check.list_versions("x")} == {2, 4, 6}
    assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 3
    assert [k.version_id for k in lt.find_keys(KeyType.TABLE_DATA)] == [2, 3, 4, 4, 6]


@settings(deadline=None, suppress_health_check=(hypothesis.HealthCheck.function_scoped_fixture,))
@given(to_copy=st.permutations(["s2", 4, 6]), existing=st.booleans())
def test_correct_versions_in_destination_force(
    to_copy, existing, check_single_threaded, lib_with_gaps_and_reused_keys, version_store_factory
):
    try:
        _tmp_test_body(to_copy, existing, lib_with_gaps_and_reused_keys, version_store_factory)
    except:
        import traceback

        traceback.print_exc()
        raise


def _tmp_test_body(to_copy, existing, lib_with_gaps_and_reused_keys, version_store_factory):
    # mongoose_copy_data's force mode rewrite version numbers in the target
    source = lib_with_gaps_and_reused_keys
    target = version_store_factory(name="_unique_")

    if existing:
        target.write("x", 0)

    s = StorageMover(source._library, target._library)
    s.write_symbol_trees_from_source_to_target([SymbolVersionsPair("x", to_copy)], True)

    actual_vers = sorted(vi["version"] for vi in target.list_versions("x"))
    print(to_copy, existing, "->", actual_vers)

    lt = target.library_tool()
    start = 0 if existing else 2  # mover starts at the first input version if target is empty....
    n = int(existing) + len(to_copy)
    assert actual_vers == list(range(start, start + n))
    assert len(lt.find_keys(KeyType.TABLE_INDEX)) == n

    source_keys = source.library_tool().find_keys(KeyType.TABLE_DATA)
    expected_target = []
    for item in to_copy:
        if item == "s2":
            expected_target.append(source_keys[0])
        elif item == 4:
            expected_target.extend(source_keys[1:4])
        else:
            expected_target.append(source_keys[-1])
    expected_target.sort()  # key=lambda k: (k.version_id, k.start_index))

    target_keys = lt.find_keys(KeyType.TABLE_DATA)
    target_keys.sort()
    if existing:
        target_keys.pop(0)

    for a, e in zip(target_keys, expected_target):
        assert a.content_hash == e.content_hash
        assert a.creation_ts >= source_keys[-1].creation_ts
