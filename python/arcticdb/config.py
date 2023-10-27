"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import json
import sys
import os
import os.path as osp
from abc import abstractmethod, ABCMeta

from arcticc.pb2.logger_pb2 import LoggersConfig, LoggerConfig
from arcticc.pb2.config_pb2 import RuntimeConfig
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, EnvironmentConfig, LibraryConfig, LibraryDescriptor
from google.protobuf.json_format import MessageToJson, Parse as JsonToMessage
from google.protobuf.message import Message
from typing import AnyStr, Optional, Dict

from arcticdb.exceptions import ArcticNativeException
from arcticdb.log import logger_by_name, configure

_HOME = osp.expanduser("~/.arctic/native")

# TODO: Some tests are either segfaulting or failing on MacOS with conda builds.
# This is meant to be used as a temporary flag to skip/xfail those tests.
MACOS_CONDA_BUILD = sys.platform == "darwin" and os.getenv("ARCTICDB_USING_CONDA", "0") == "1"
MACOS_CONDA_BUILD_SKIP_REASON = (
    "Tests fail for macOS conda builds, either because Azurite is improperly configured"
    "on the CI or because there's problem with Azure SDK for C++ in this configuration."
)

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
_DEFAULT_LMDB_LIB = "lmdb.default"
_DEFAULT_LOG_DIR = arctic_native_path("logs")
_DEFAULT_LOG_LEVEL = "INFO"
_DEFAULT_DATA_DIR = arctic_native_path("data")


# Public api defaults. Anything underscored is subject to change without warning
class Defaults(object):
    LOCAL_ENV = _LOCAL_ENV
    ENV = _DEFAULT_ENV
    LIB = _DEFAULT_LMDB_LIB
    LOG_DIR = _DEFAULT_LOG_DIR
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
