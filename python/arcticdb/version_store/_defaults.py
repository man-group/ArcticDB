"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os


def resolve_defaults(
    param_name, proto_cfg, global_default, existing_value=None, uppercase=True, runtime_options=None, **kwargs
):
    """
    Precedence: existing_value > kwargs > runtime_defaults > env > proto_cfg > global_default

    Parameters
    ----------
    param_name: str
    proto_cfg
        Gets the param_name attribute of this object
        Most often is `self._write_options()` for the Protobuf write_options.
    global_default
        FUTURE: store this in a central location
    existing_value:
        The value already supplied to the caller
    uppercase
        If true (default), will look for `param_name.upper()` in OS environment variables; otherwise, the original
        case.
    runtime_options:
        The RuntimeOptions to use for the library.
        Uses the param_name attribute of runtime_options.
    kwargs
        For passing through the caller's kwargs in which we look for `param_name`
        *Deprecating: use `existing_value`*
    """

    if existing_value is not None:
        return existing_value

    param_value = kwargs.get(param_name)
    if param_value is not None:
        return param_value

    try:
        if runtime_options is not None:
            option_value = getattr(runtime_options, param_name)
            if option_value is not None:
                return option_value
    except AttributeError:
        pass

    env_name = param_name.upper() if uppercase else param_name
    env_value = os.getenv(env_name)
    if env_value is not None:
        return env_value not in ("", "0") and not env_value.lower().startswith("f")

    try:
        if proto_cfg is not None:
            config_value = getattr(proto_cfg, param_name)
            if config_value is not None:
                return config_value

    except AttributeError:
        pass

    return global_default
