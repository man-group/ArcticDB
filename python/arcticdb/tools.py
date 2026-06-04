"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import Dict
from arcticdb.config import set_log_level, Defaults
from arcticdb.log import storage as storage_log

from arcticdb_ext import set_config_int, set_config_string, set_config_double

# Setting config from environment variables. This code is used in the package __init__.py
_ARCTICDB_ENV_VAR_PREFIX = "ARCTICDB_"
_ARCTIC_NATIVE_ENV_VAR_PREFIX = "ARCTIC_NATIVE_"
_TYPE_INT = "INT"
_TYPE_FLOAT = "FLOAT"
_TYPE_STR = "STR"
_TYPE_LOGLEVEL = "LOGLEVEL"
_DEFAULT_TYPE = _TYPE_STR
_TYPE_SET = {_TYPE_INT, _TYPE_FLOAT, _TYPE_STR, _TYPE_LOGLEVEL}

# AWS SDK verbosity (Aws::Utils::Logging::LogLevel) by increasing verbosity, paired with the equivalent ArcticDB log
# level name used for the `s3` spdlog stream. Index == AWS LogLevel integer.
_AWS_LOG_LEVELS = ["OFF", "CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"]


def _aws_level_int_from_name(name: str) -> int:
    return _AWS_LOG_LEVELS.index(name.upper())


def set_config_from_env_vars(env_vars: Dict[str, str]):
    """
    The env_vars dictionary is a set of a key-value pairs. The values should all be strings.
    The key names can be anything. Only those starting with the "ARCTICDB_" prefix are considered relevant.
    The last word (delimited by underbars) is the type. If it is not in the defined types set it is defaulted.
    The names are transformed by removing the prefix/type and replace _ with .
    The arcticc set_config_xxx() function corresponding to the type is called.
    """
    if env_vars is None:
        return

    log_level_changes = {}
    default_log_level = Defaults.DEFAULT_LOG_LEVEL
    aws_log_level_env = None
    for k, v in env_vars.items():
        k = k.upper()
        start_index = None
        if k.startswith(_ARCTIC_NATIVE_ENV_VAR_PREFIX):  # 2 underscores in prefix
            start_index = 2
        elif k.startswith(_ARCTICDB_ENV_VAR_PREFIX):  # 1 underscore in prefix
            start_index = 1

        if start_index is not None:
            w = k.split("_")
            var_type_raw = w[-1] if w[-1] in _TYPE_SET else None
            var_type = var_type_raw if var_type_raw is not None else _TYPE_STR
            config_name = ".".join(w[start_index:] if var_type_raw is None else w[start_index:-1])
            try:
                if var_type == _TYPE_STR:
                    set_config_string(config_name, v)
                elif var_type == _TYPE_INT:
                    if config_name == "AWS.LOGLEVEL":
                        # Set later in _reconcile_aws_log_level
                        if 0 <= int(v) < len(_AWS_LOG_LEVELS):
                            aws_log_level_env = int(v)
                        else:
                            storage_log.error(
                                f"Ignoring ARCTICDB_AWS_LogLevel_int={v}: must be in 0..{len(_AWS_LOG_LEVELS) - 1}"
                            )
                    else:
                        set_config_int(config_name, int(v))
                elif var_type == _TYPE_FLOAT:
                    set_config_double(config_name, float(v))
                elif var_type == _TYPE_LOGLEVEL:
                    if config_name.upper() == "ALL":
                        default_log_level = v.upper()
                    else:
                        log_level_changes[config_name.lower()] = v.upper()
                else:
                    storage_log.error(f"Invalid type for env var {k} (value {v}), type used {var_type}")
            except Exception as e:
                storage_log.error(
                    f"Error setting env var {k} to value {v} using type {var_type} and config name {config_name}"
                    f" Exception: {e}"
                )

    _reconcile_aws_log_level(aws_log_level_env, log_level_changes)

    if log_level_changes or default_log_level != Defaults.DEFAULT_LOG_LEVEL:
        set_log_level(default_level=default_log_level, specific_log_levels=log_level_changes)


def _reconcile_aws_log_level(aws_log_level_env, log_level_changes):
    """
    AWS SDK logging and the `s3` spdlog stream share a single level, the more verbose of
    ``ARCTICDB_AWS_LogLevel_int`` (deprecated) and ``ARCTICDB_s3_loglevel``.
    When neither is set, AWS logging stays off (the `s3` stream defaults to CRITICAL).
    """
    s3_requested = log_level_changes.get("s3")
    if aws_log_level_env is None and s3_requested is None:
        return

    aws_from_s3 = 0
    if s3_requested is not None and s3_requested.upper() in _AWS_LOG_LEVELS:
        aws_from_s3 = _aws_level_int_from_name(s3_requested)
    effective = max(aws_log_level_env or 0, aws_from_s3)

    log_level_changes["s3"] = _AWS_LOG_LEVELS[effective]
    set_config_int("AWS.LogLevel", effective)

    if aws_log_level_env is not None:
        storage_log.warn(
            "ARCTICDB_AWS_LogLevel_int is deprecated. Use ARCTICDB_s3_loglevel (e.g. ARCTICDB_s3_loglevel=DEBUG) to "
            "control S3/AWS SDK logging, which is now routed through ArcticDB's `s3` log stream."
        )
