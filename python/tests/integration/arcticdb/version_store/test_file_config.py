"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""
from arcticdb.config import Defaults
from arcticdb.version_store.helper import add_lmdb_library_to_env, save_envs_config, get_arctic_native_lib
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap


def _make_temp_lmdb_lib(tmpdir, library_name):
    cfg_filename = "{}/{}".format(tmpdir, "test_cfg")
    cfg = EnvironmentConfigsMap()
    add_lmdb_library_to_env(
        cfg, lib_name=library_name, env_name=Defaults.ENV, description="a test library", db_dir=str(tmpdir)
    )
    save_envs_config(cfg, conf_path=cfg_filename)
    return "{}@{}".format(library_name, cfg_filename)


def test_native_lmdb_library(tmpdir):
    lib = get_arctic_native_lib(_make_temp_lmdb_lib(tmpdir, "test.file_config"))
    lib.write("symbol", "thing")
    assert lib.read("symbol").data == "thing"
