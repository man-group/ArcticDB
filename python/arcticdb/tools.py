"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import logging

from typing import Dict
from arcticdb.config import set_log_level, Defaults

from arcticdb_ext import set_config_int, set_config_string, set_config_double

# Setting config from environment variables. This code is used in the package __init__.py
_ARCTICDB_ENV_VAR_PREFIX = "ARCTICDB_"
_TYPE_INT = "INT"
_TYPE_FLOAT = "FLOAT"
_TYPE_STR = "STR"
_TYPE_LOGLEVEL = "LOGLEVEL"
_DEFAULT_TYPE = _TYPE_STR
_TYPE_SET = {_TYPE_INT, _TYPE_FLOAT, _TYPE_STR, _TYPE_LOGLEVEL}


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
    for k, v in env_vars.items():
        k = k.upper()
        start_index = None
        if k.startswith(_ARCTICDB_ENV_VAR_PREFIX):  # 1 underscore in prefix
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
                    set_config_int(config_name, int(v))
                elif var_type == _TYPE_FLOAT:
                    set_config_double(config_name, float(v))
                elif var_type == _TYPE_LOGLEVEL:
                    if config_name.upper() == "ALL":
                        default_log_level = v.upper()
                    else:
                        log_level_changes[config_name.lower()] = v.upper()
                else:
                    logging.error("Invalid type for env var %s (value %s), type used %s", k, v, var_type)
            except Exception as e:
                logging.error(
                    f"Error setting env var {k} to value {v} using type {var_type} and config name {config_name}"
                    f" Exception: {e}"
                )

    if log_level_changes or default_log_level != Defaults.DEFAULT_LOG_LEVEL:
        set_log_level(default_level=default_log_level, specific_log_levels=log_level_changes)
