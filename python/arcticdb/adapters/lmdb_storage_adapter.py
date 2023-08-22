import shutil
from typing import Optional, Any
from tempfile import mkdtemp

from arcticdb.version_store.helper import add_lmdb_library_to_env
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap

from .storage_fixture_api import *


class LmdbStorageFixture(StorageFixture):
    def __init__(self, db_dir: Optional[str]):
        self.db_dir = str(db_dir) or mkdtemp(suffix="LmdbStorageFixtureFactory")
        self.arctic_uri = "lmdb://" + self.db_dir  # TODO: extra config?

    def __exit__(self, exc_type, exc_value, traceback):
        for result in self.generated_libs.values():
            #  pytest holds a member variable `cached_result` equal to `result` above which keeps the storage alive and
            #  locked. See https://github.com/pytest-dev/pytest/issues/5642 . So we need to decref the C++ objects
            #  keeping the LMDB env open before they will release the lock and allow Windows to delete the LMDB files.
            result.version_store = None
            result._library = None

        shutil.rmtree(self.db_dir, ignore_errors=True)

    def create_test_cfg(self, lib_name: str, *, lmdb_config: Dict[str, Any] = {}) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        add_lmdb_library_to_env(
            cfg, lib_name=lib_name, env_name=Defaults.ENV, db_dir=self.db_dir, lmdb_config=lmdb_config
        )
        return cfg

    def create_version_store_factory(self, default_lib_name: str):
        def create_version_store(
            col_per_group: Optional[int] = None,  # Legacy option names
            row_per_segment: Optional[int] = None,
            lmdb_config: Dict[str, Any] = {},  # Mainly to allow setting map_size
            **kwargs,
        ) -> NativeVersionStore:
            if col_per_group is not None and "column_group_size" not in kwargs:
                kwargs["column_group_size"] = col_per_group
            if row_per_segment is not None and "segment_row_size" not in kwargs:
                kwargs["segment_row_size"] = row_per_segment
            cfg_factory = self.create_test_cfg
            if lmdb_config:
                cfg_factory = functools.partial(cfg_factory, lmdb_config=lmdb_config)
            return _version_store_factory_impl(self.generated_libs, cfg_factory, default_lib_name, **kwargs)

        return create_version_store

    def set_permission(self, *, read: bool, write: bool):
        raise NotImplementedError("LmdbStorageFixture don't support permissions")
