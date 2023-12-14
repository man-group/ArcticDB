"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import os.path as osp
import re
import time
from typing import Iterable, Dict, Any, Union

from arcticc.pb2.lmdb_storage_pb2 import Config as LmdbConfig
from arcticc.pb2.s3_storage_pb2 import Config as S3Config
from arcticc.pb2.azure_storage_pb2 import Config as AzureConfig
from arcticc.pb2.in_memory_storage_pb2 import Config as MemoryConfig
from arcticc.pb2.mongo_storage_pb2 import Config as MongoConfig
from arcticc.pb2.nfs_backed_storage_pb2 import Config as NfsConfig
from arcticc.pb2.storage_pb2 import (
    EnvironmentConfigsMap,
    EnvironmentConfig,
    LibraryConfig,
    LibraryDescriptor,
    VariantStorage,
    Permissions,
    NoCredentialsStore,
)

from arcticdb.config import *  # for backward compat after moving to config
from arcticdb.config import _expand_path
from arcticdb.exceptions import ArcticNativeException, LibraryNotFound, UserInputException
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.authorization.permissions import OpenMode


def create_lib_from_config(cfg, env=Defaults.ENV, lib_name=Defaults.LIB):
    return NativeVersionStore.create_lib_from_config(cfg, env, lib_name)


def create_lib_from_lib_config(lib_config, env=Defaults.ENV, open_mode=OpenMode.DELETE):
    return NativeVersionStore.create_lib_from_lib_config(lib_config, env, open_mode)


def extract_lib_config(env_cfg, lib_path):
    # type: (EnvironmentConfig, AnyStr)->LibraryConfig
    if lib_path not in env_cfg.lib_by_path:
        raise ArcticNativeException("Missing library {} in config {}".format(lib_path, env_cfg))
    cfg = LibraryConfig()
    lib = env_cfg.lib_by_path[lib_path]
    cfg.lib_desc.CopyFrom(lib)
    for sid in lib.storage_ids:
        cfg.storage_by_id[sid].CopyFrom(env_cfg.storage_by_id[sid])
    for sid in lib.backup_storage_ids:
        if sid in env_cfg.storage_by_id:
            cfg.storage_by_id[sid].CopyFrom(env_cfg.storage_by_id[sid])
    return cfg


class ArcticConfig(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __getitem__(self, lib_path):
        # type: (LibName)-> NativeVersionStore
        pass

    def get_library(self, lib_path):
        return self[lib_path]

    @abstractmethod
    def list_libraries(self):
        # type: ()->Iterable[LibName]
        pass

    def set_mongo_connector(self, connector):
        # type: (MongoDBConnector)->None
        self.mongo_connector = connector

    def set_uri_builder(self, uri_builder):
        self.uri_builder = uri_builder


class ArcticFileConfig(ArcticConfig):
    def __init__(self, env=Defaults.ENV, config_path=Defaults.ENV_FILE_PATH):
        # type: (EnvName, FilePath)->None
        self._conf_path = _expand_path(config_path)
        self._env = env

    def _check_config(self):
        if not osp.exists(self._conf_path):
            raise ArcticNativeException("Config file {} not found".format(self._conf_path))

    def __getitem__(self, lib_name):
        self._check_config()
        envs_cfg = load_envs_config(conf_path=self._conf_path)
        lib_cfg = extract_lib_config(envs_cfg.env_by_id[self._env], lib_name)

        # Local config assumes a read/write openmode
        return NativeVersionStore.create_store_from_lib_config(lib_cfg, self._env, OpenMode.DELETE)

    def list_libraries(self):
        self._check_config()
        return load_env_config(conf_path=self._conf_path, env=self._env).lib_by_path.keys()


class ArcticMemoryConfig(ArcticConfig):
    def __init__(self, cfg, env):
        # type: (EnvironmentConfigsMap, Optional[EnvName])->None
        self._cfg = cfg
        self._env = env

    def __getitem__(self, lib_path):
        # type: (LibName)->NativeVersionStore
        lib_cfg = extract_lib_config(self._cfg.env_by_id[self._env], lib_path)
        return NativeVersionStore.create_store_from_lib_config(lib_cfg, self._env, OpenMode.DELETE)

    @property
    def cfg(self):
        return self._cfg

    @property
    def env_cfg(self):
        return self._cfg.env_by_id[self._env]

    def list_libraries(self):
        return self.env_cfg.lib_by_path.keys()


def add_library_config_to_env(cfg, lib_cfg, env_name):
    # type: (EnvironmentConfigsMap, LibraryConfig, EnvName)->None
    env = cfg.env_by_id[env_name]

    for sid in lib_cfg.storage_by_id:
        env.storage_by_id[sid].CopyFrom(lib_cfg.storage_by_id[sid])

    env.lib_by_path[lib_cfg.lib_desc.name].CopyFrom(lib_cfg.lib_desc)


def get_storage_for_lib_name(lib_name, env):
    # type: (LibName, EnvironmentConfigsMap)->(StorageId, VariantStorage)
    sid = "{}_store".format(lib_name)
    return sid, env.storage_by_id[sid]


def get_secondary_storage_for_lib_name(lib_name, env):
    # type: (LibName, EnvironmentConfigsMap)->(StorageId, VariantStorage)
    sid = "{}_store_2".format(lib_name)
    return sid, env.storage_by_id[sid]


def _add_lib_desc_to_env(env, lib_name, sid, description=None):
    if lib_name in env.lib_by_path:
        raise ArcticNativeException("Library {} already configured in {}".format(lib_name, env))
    lib_desc = env.lib_by_path[lib_name]
    lib_desc.storage_ids.append(sid)
    lib_desc.name = lib_name
    if description:
        lib_desc.description = description


def _add_storage_to_env(env, lib_name, sid):
    lib_desc = env.lib_by_path[lib_name]
    lib_desc.storage_ids.append(sid)


def get_lib_cfg(cfg: ArcticMemoryConfig, env_name: str, lib_name: str) -> LibraryDescriptor:
    env = cfg.cfg.env_by_id[env_name]
    return env.lib_by_path[lib_name]


def add_lmdb_library_to_env(cfg, lib_name, env_name, db_dir=Defaults.DATA_DIR, description=None, *, lmdb_config={}):
    # type: (EnvironmentConfigsMap, LibName, EnvName, Optional[FilePath], Optional[str], None, Dict[str, Any])->None
    env = cfg.env_by_id[env_name]
    lmdb = LmdbConfig()
    lmdb.path = db_dir
    for k, v in lmdb_config.items():
        setattr(lmdb, k, v)

    sid, storage = get_storage_for_lib_name(lib_name, env)
    storage.config.Pack(lmdb, type_url_prefix="cxx.arctic.org")
    _add_lib_desc_to_env(env, lib_name, sid, description)


def add_memory_library_to_env(cfg, lib_name, env_name, description=None):
    env = cfg.env_by_id[env_name]
    in_mem = MemoryConfig()

    sid, storage = get_storage_for_lib_name(lib_name, env)
    storage.config.Pack(in_mem, type_url_prefix="cxx.arctic.org")
    _add_lib_desc_to_env(env, lib_name, sid, description)


def get_mongo_proto(cfg, lib_name, env_name, uri):
    env = cfg.env_by_id[env_name]
    mongo = MongoConfig()
    if uri is not None:
        mongo.uri = uri

    sid, storage = get_storage_for_lib_name(lib_name, env)
    storage.config.Pack(mongo, type_url_prefix="cxx.arctic.org")
    return sid, storage


def add_mongo_library_to_env(cfg, lib_name, env_name, uri=None, description=None):
    env = cfg.env_by_id[env_name]
    sid, storage = get_mongo_proto(
        cfg=cfg,
        lib_name=lib_name,
        env_name=env_name,
        uri=uri,
    )

    _add_lib_desc_to_env(env, lib_name, sid, description)


def get_s3_proto(
    cfg,
    lib_name,
    env_name,
    credential_name,
    credential_key,
    bucket_name,
    endpoint,
    with_prefix=True,
    is_https=False,
    region=None,
    use_virtual_addressing=False,
):
    env = cfg.env_by_id[env_name]
    s3 = S3Config()
    if bucket_name is not None:
        s3.bucket_name = bucket_name
    if credential_name is not None:
        s3.credential_name = credential_name
    if credential_key is not None:
        s3.credential_key = credential_key
    if endpoint is not None:
        s3.endpoint = endpoint
    if is_https is not None:
        s3.https = is_https
    if use_virtual_addressing is not None:
        s3.use_virtual_addressing = use_virtual_addressing
    # adding time to prefix - so that the s3 root folder is unique and we can delete and recreate fast
    if with_prefix:
        if isinstance(with_prefix, str):
            s3.prefix = with_prefix
        else:
            s3.prefix = f"{lib_name}{time.time() * 1e9:.0f}"
    else:
        s3.prefix = lib_name

    if region:
        s3.region = region

    sid, storage = get_storage_for_lib_name(s3.prefix, env)
    storage.config.Pack(s3, type_url_prefix="cxx.arctic.org")
    return sid, storage


def add_s3_library_to_env(
    cfg,
    lib_name,
    env_name,
    credential_name,
    credential_key,
    bucket_name,
    endpoint,
    description=None,
    with_prefix=True,
    is_https=False,
    region=None,
    use_virtual_addressing=False,
):
    env = cfg.env_by_id[env_name]
    if with_prefix and isinstance(with_prefix, str) and (with_prefix.endswith("/") or "//" in with_prefix):
        raise UserInputException(
            "path_prefix cannot contain // or end with a / because this breaks some S3 API calls, path_prefix was"
            f" [{with_prefix}]"
        )

    sid, storage = get_s3_proto(
        cfg=cfg,
        lib_name=lib_name,
        env_name=env_name,
        credential_name=credential_name,
        credential_key=credential_key,
        bucket_name=bucket_name,
        endpoint=endpoint,
        with_prefix=with_prefix,
        is_https=is_https,
        region=region,
        use_virtual_addressing=use_virtual_addressing,
    )

    _add_lib_desc_to_env(env, lib_name, sid, description)


def get_azure_proto(
    cfg,
    lib_name,
    env_name,
    container_name,
    endpoint,
    with_prefix: Optional[Union[bool, str]] = True,
    ca_cert_path: str = "",
):
    env = cfg.env_by_id[env_name]
    azure = AzureConfig()
    if not container_name:
        raise UserInputException("Container needs to be specified")
    azure.container_name = container_name
    azure.endpoint = endpoint
    if with_prefix:
        if isinstance(with_prefix, str):
            azure.prefix = with_prefix
        else:
            azure.prefix = f"{lib_name}{time.time() * 1e9:.0f}"
    else:
        azure.prefix = lib_name
    azure.ca_cert_path = ca_cert_path

    sid, storage = get_storage_for_lib_name(azure.prefix, env)
    storage.config.Pack(azure, type_url_prefix="cxx.arctic.org")
    return sid, storage


def add_azure_library_to_env(
    cfg,
    lib_name,
    env_name,
    container_name,
    endpoint,
    description: Optional[bool] = None,
    with_prefix: Optional[Union[bool, str]] = True,
    ca_cert_path: str = "",
):
    env = cfg.env_by_id[env_name]
    sid, storage = get_azure_proto(
        cfg=cfg,
        lib_name=lib_name,
        env_name=env_name,
        container_name=container_name,
        endpoint=endpoint,
        with_prefix=with_prefix,
        ca_cert_path=ca_cert_path,
    )

    _add_lib_desc_to_env(env, lib_name, sid, description)


# see https://regex101.com/r/mBCS80/1
_LIB_PATH_REGEX = re.compile(
    r"""\b
(
(?:\w+) # lib name first fragment as non capturing group
\.
(?:\w+) # lib name second fragment as non capturing group
)
@
(
\/?(?:(?:[^\/])+) # first fragment of path (optionaly starting with /)
(?:\/(?:[^\/])+)* # rest of path
)
\b""",
    re.VERBOSE,
)


def get_arctic_native_lib(lib_fqn):
    # type: (AnyStr)->NativeVersionStore
    m = _LIB_PATH_REGEX.match(lib_fqn)
    if m is None:
        raise LibraryNotFound(lib_fqn)
    lib, path = m.group(1), m.group(2)
    return ArcticFileConfig(config_path=path)[lib]
