"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.

This module implements a backwards compatible version of msgpack functions.
"""
import msgpack
from arcticdb.preconditions import check
from arcticdb.exceptions import ArcticNativeException

ExtType = msgpack.ExtType


def _check_valid_msgpack():
    pack_module = msgpack.packb.__module__
    packer_module = msgpack.Packer.__module__
    # Check that msgpack hasn't been monkey patched by another package
    # We only support the official cmsgpack and fallback modules
    if (pack_module in ("msgpack", "msgpack.fallback")) and (
        packer_module in ("msgpack._packer", "msgpack.fallback", "msgpack._cmsgpack")
    ):
        return
    raise ArcticNativeException("Unsupported msgpack variant, got: {}, {}".format(pack_module, packer_module))


def packb(obj, **kwargs):
    _check_valid_msgpack()
    # use_bin_type supported from msgpack==0.4.0 but became true later
    return msgpack.packb(obj, use_bin_type=True, strict_types=True, **kwargs)

packb.__doc__ = msgpack.packb.__doc__
packb.__name__ = msgpack.packb.__name__


def padded_packb(obj, **kwargs):
    """msgpack.packb with some defaults across msgpack versions and padded to 8 bytes
    returns: (packed bytes, nbytes of unpadded content)"""
    _check_valid_msgpack()
    # use_bin_type is supported from msgpack==0.4.0 but became true later
    # don't reset the buffer so we can append padding bytes
    packer = msgpack.Packer(autoreset=False, use_bin_type=True, strict_types=True, **kwargs)
    packer.pack(obj)
    nbytes = packer.getbuffer().nbytes
    pad = -nbytes % 8 # next multiple of 8 bytes
    [packer.pack(None) for _ in range(pad)] # None is packed as single byte b`\xc0`
    check(packer.getbuffer().nbytes % 8 == 0, 'Error in ArcticDB padded_packb. Padding failed. nbytes={}', packer.getbuffer().nbytes)
    return packer.bytes(), nbytes


def unpackb(packed, **kwargs):
    if msgpack.version >= (0, 6, 0):
        kwargs.setdefault("strict_map_key", False)
    return msgpack.unpackb(packed, **kwargs)

unpackb.__doc__ = msgpack.unpackb.__doc__
unpackb.__name__ = msgpack.unpackb.__name__