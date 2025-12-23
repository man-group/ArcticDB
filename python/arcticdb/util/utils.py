"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to True or False.

    If the string is not one of the values below we return False.

    This function raises if and only if `val` is not a `str`, in which case it raises an AttributeError.
    """
    if not isinstance(val, str):
        raise AttributeError(f"Expected isinstance(val, str) but type(val)=[{type(val)}]")
    val = val.lower()
    return val in ("y", "yes", "t", "true", "on", "1")
