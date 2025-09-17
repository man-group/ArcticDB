"""How some of the Python 2 pickles in test_normalization.py were created.

Executed from a Python 2 env with msgpack 0.6.2
"""

from email import errors  # arbitrary module with some custom types to pickle
import pickle
import msgpack
import sys

major_version = sys.version[0]


def custom_pack(obj):
    # 102 is our extension code for pickled in Python 2
    return msgpack.ExtType(102, msgpack.packb(pickle.dumps(obj)))


def msgpack_packb(obj):
    return msgpack.packb(obj, use_bin_type=True, strict_types=True, default=custom_pack)


obj = errors.BoundaryError("bananas")
title = "py" + major_version + "_obj.bin"
with open(title, "wb") as f:
    msgpack.dump(obj, f, default=custom_pack)

obj = {"dict_key": errors.BoundaryError("bananas")}
title = "py" + major_version + "_dict.bin"
with open(title, "wb") as f:
    msgpack.dump(obj, f, default=custom_pack)

obj = "my_string"
title = "py" + major_version + "_str.bin"
with open(title, "wb") as f:
    msgpack.dump(obj, f, default=custom_pack)

obj = b"my_bytes"
title = "py" + major_version + "_str_bytes.bin"
with open(title, "wb") as f:
    msgpack.dump(obj, f, default=custom_pack)
