"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.

This module implements a backwards compatible version of msgpack functions.
"""

import msgpack


def unpackb(*args, **kwargs):
    if msgpack.version >= (0, 6, 0):
        kwargs.setdefault("strict_map_key", False)
    return msgpack.unpackb(*args, **kwargs)


unpackb.__doc__ = msgpack.unpackb.__doc__
unpackb.__name__ = msgpack.unpackb.__name__
