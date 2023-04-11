import os as _os
import sys as _sys
import google.protobuf as _protobuf

_proto_ver = _protobuf.__version__.split(".")[0]
if _proto_ver in "34":
    # Use the namespace package (https://peps.python.org/pep-0420) feature to select the pb2 files matching the protobuf
    _protos_path = _os.path.abspath(
        _os.path.join(_os.path.dirname(__file__), "..", "..", "arcticdb", "proto", _proto_ver, "arcticc", "pb2")
    )
    __path__.append(_protos_path)
else:
    raise NotImplementedError(f"We only support protobuf versions 3 & 4. You have {_protobuf.__version__}")
