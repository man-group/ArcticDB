"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import json
import os
import os.path as osp
from abc import abstractmethod, ABCMeta

import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from arcticc.pb2.logger_pb2 import LoggersConfig, LoggerConfig
from arcticc.pb2.config_pb2 import RuntimeConfig
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, EnvironmentConfig, LibraryConfig, LibraryDescriptor
from google.protobuf.json_format import MessageToJson, Parse as JsonToMessage
from google.protobuf.message import Message
from typing import AnyStr, Optional, Dict

from arcticdb.exceptions import ArcticNativeException
from arcticdb.log import logger_by_name, configure

_HOME = osp.expanduser("~/.arctic/native")


EnvName = AnyStr
LibName = AnyStr
StorageId = AnyStr
FilePath = AnyStr
MongoUri = AnyStr


def _expand_path(p):
    # type: (FilePath)->FilePath
    return osp.abspath(osp.expanduser(osp.expandvars(p)))


def arctic_native_path(p=None):
    return _expand_path(osp.join(_HOME, p)) if p else _HOME


_LOCAL_ENV = "local"
_DEFAULT_ENV = _LOCAL_ENV
_DEFAULT_ENVS_PATH = arctic_native_path("conf/envs.yaml")
_DEFAULT_LMDB_LIB = "lmdb.default"
_DEFAULT_LOG_CONF = arctic_native_path("conf/loggers.yaml")
_DEFAULT_LOG_DIR = arctic_native_path("logs")
_DEFAULT_LOG_LEVEL = "INFO"
_DEFAULT_RUNTIME_CONF = arctic_native_path("conf/runtime.yaml")
_DEFAULT_DATA_DIR = arctic_native_path("data")


# Public api defaults. Anything underscored is subject to change without warning
class Defaults(object):
    LOCAL_ENV = _LOCAL_ENV
    ENV = _DEFAULT_ENV
    LIB = _DEFAULT_LMDB_LIB
    ENV_FILE_PATH = _DEFAULT_ENVS_PATH
    LOG_CONF_FILE_PATH = _DEFAULT_LOG_CONF
    LOG_DIR = _DEFAULT_LOG_DIR
    RUNTIME_CONF_FILE_PATH = _DEFAULT_RUNTIME_CONF
    DATA_DIR = _DEFAULT_DATA_DIR
    DEFAULT_LOG_LEVEL = _DEFAULT_LOG_LEVEL


def _extract_lib_config(env_cfg, lib_path):
    # type: (EnvironmentConfig)->LibraryConfig
    if lib_path not in env_cfg.lib_by_path:
        raise ArcticNativeException("Missing library {} in config {}".format(lib_path, env_cfg))
    cfg = LibraryConfig()
    lib = env_cfg.lib_by_path[lib_path]
    cfg.lib_desc.CopyFrom(lib)
    for sid in lib.storage_ids:
        cfg.storage_by_id[sid].CopyFrom(env_cfg.storage_by_id[sid])
    return cfg


class ProtoConfConverter(object):
    __metaclass__ = ABCMeta

    def __init__(self, out_type):
        # type: (Type)->None
        if not issubclass(out_type, Message):
            raise TypeError("ProtoConf loader can only load proto message type. Actual {}".format(out_type))
        self._out_type = out_type

    @abstractmethod
    def loads(self, buf):
        # type: (AnyStr)->Message
        pass

    @abstractmethod
    def dumps(self, cfg):
        # type: (Message)->AnyStr
        pass


class YamlProtoConverter(ProtoConfConverter):
    def loads(self, buf):
        cfg = self._out_type()
        JsonToMessage(json.dumps(yaml.load(buf, Loader=Loader)), cfg, ignore_unknown_fields=True)
        return cfg

    def dumps(self, cfg):
        if not isinstance(cfg, self._out_type):
            raise TypeError("Unsupported type: {}, expected {}".format(type(cfg), self._out_type))
        j = MessageToJson(cfg, preserving_proto_field_name=True)
        return yaml.safe_dump(json.loads(j), default_flow_style=False)


def _load_config(conf_path, conf_type):
    # type: (FilePath, Type)->Any
    p = _expand_path(conf_path)
    if not osp.exists(p):
        raise ArcticNativeException("Config file {} for type {} not found".format(p, conf_type))

    converter = YamlProtoConverter(conf_type)
    with open(p, "r") as _if:
        return converter.loads(_if.read())


def _save_config(config, conf_path, conf_type):
    # type: (Any, FilePath, Type)->None
    p = _expand_path(conf_path)
    if not osp.exists(osp.dirname(p)):
        os.makedirs(osp.dirname(p))
    converter = YamlProtoConverter(conf_type)
    with open(p, "w") as of:
        of.write(converter.dumps(config))


def load_envs_config(conf_path=_DEFAULT_ENVS_PATH):
    # type: (Optional[AnyStr])->EnvironmentConfigsMap
    return _load_config(conf_path, EnvironmentConfigsMap)


def load_env_config(env=_DEFAULT_ENV, conf_path=_DEFAULT_ENVS_PATH):
    return load_envs_config(conf_path).env_by_id[env]


def save_envs_config(config, conf_path=_DEFAULT_ENVS_PATH):
    # type: (EnvironmentConfigsMap, Optional[FilePath])->None
    _save_config(config, conf_path, EnvironmentConfigsMap)


def load_loggers_config(path=Defaults.LOG_CONF_FILE_PATH):
    # type: (Optional[FilePath])->LoggersConfig
    return _load_config(path, LoggersConfig)


def save_loggers_config(config=None, path=Defaults.LOG_CONF_FILE_PATH):
    # type: (Optional[LoggersConfig], Optional[FilePath])->None
    config = config if config is not None else default_loggers_config()
    _save_config(config, path, LoggersConfig)


def load_runtime_config(path=Defaults.RUNTIME_CONF_FILE_PATH):
    # type: (Optional[FilePath])->RuntimeConfig
    return _load_config(path, RuntimeConfig)


def save_runtime_config(config=None, path=Defaults.RUNTIME_CONF_FILE_PATH):
    # type: (Optional[RuntimeConfig], Optional[FilePath])->None
    config = config if config is not None else default_runtime_config()
    _save_config(config, path, RuntimeConfig)


def make_loggers_config(
    default_level=Defaults.DEFAULT_LOG_LEVEL,
    specific_log_levels: Optional[Dict[str, str]] = None,
    console_output: bool = True,
    file_output_path: Optional[str] = None,
):
    """
    Generate a ``LoggersConfig`` object with sink set to stderr and the given log levels.

    Parameters
    ----------
    default_level
        Default log level for all the loggers unless overriden with specific_log_levels.
        Valid values are "DEBUG", "INFO", "WARN", "ERROR".
    specific_log_levels
        Optional overrides for specific logger(s). The possible logger names can be found in log.py.
    console_output
        Boolean indicating whether to output logs to the terminal.
    file_output_path
        If None, logs will not be written to a file. Otherwise, this value should be set to the path of a file to which
        logging output will be written.

    Examples
    --------
    >>> make_loggers_config("INFO", {'version': "DEBUG", 'storage': "DEBUG"})
    """
    log_cfgs = LoggersConfig()
    specific_log_levels = {} if not specific_log_levels else specific_log_levels

    if not console_output and not file_output_path:
        raise ValueError(
            "Logging configured with both console logging and file logging disabled. One of console logging "
            "or file logging must be enabled."
        )

    if console_output:
        sink = log_cfgs.sink_by_id["console"]
        sink.console.std_err = True

    if file_output_path:
        sink = log_cfgs.sink_by_id["file"]
        sink.file.path = file_output_path

    for logger_name in logger_by_name:
        level_to_set = specific_log_levels.get(logger_name, default_level)
        logger = log_cfgs.logger_by_id[logger_name]
        if console_output:
            logger.sink_ids.append("console")
        if file_output_path:
            logger.sink_ids.append("file")
        logger.level = getattr(LoggerConfig, level_to_set)

    return log_cfgs


def set_log_level(
    default_level=Defaults.DEFAULT_LOG_LEVEL, specific_log_levels=None, console_output=True, file_output_path=None
):
    """
    Passes the arguments to ``make_loggers_config`` and then configures the loggers, overwriting any existing config.

    For more information on the parameters this method takes, please see the documentation for `make_loggers_config`.
    """
    return configure(
        make_loggers_config(default_level, specific_log_levels, console_output, file_output_path), force=True
    )


def default_loggers_config():
    return make_loggers_config("INFO")


def default_runtime_config():
    # type: ()->RuntimeConfig
    runtime_cfg = RuntimeConfig()
    runtime_cfg.int_values["Store.NumThreads"] = 16
    return runtime_cfg
