"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""
from arcticdb.exceptions import ArcticNativeException


def check(cond, msg, *args, **kwargs):
    if not cond:
        raise ArcticNativeException(msg.format(*args, **kwargs))
