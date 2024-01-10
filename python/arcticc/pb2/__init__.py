import os as _os
import sys as _sys
import google.protobuf as _protobuf

_proto_ver = _protobuf.__version__.split(".")[0]
if _proto_ver in "34":

    _python = _sys.version_info[:2]
    if _python >= (3, 11) and _proto_ver == "3":
        raise RuntimeError(
            "Your environment uses protobuf 3 which does not officially support Python>=3.11.\n"
            "Since the support of protobuf 3 for Python ended in 23Q2, we recommend adapting your environment\n"
            "to use protobuf 4 before trying to downgrade to Python 3.10 to continue using protobuf 3.\n"
            "For more information, see: https://protobuf.dev/support/version-support/#python"
        )
    elif _python <= (3, 6) and _proto_ver >= "4":
        raise RuntimeError(
            "Your environment uses protobuf 4 which does not support Python<=3.6.\n"
            "We recommend updating your environment to use the minimum supported version of Python.\n"
            "For more information, see: https://devguide.python.org/versions/#supported-versions"
        )

    # Use the namespace package (https://peps.python.org/pep-0420) feature to select the pb2 files matching the protobuf
    _protos_path = _os.path.abspath(
        _os.path.join(_os.path.dirname(__file__), "..", "..", "arcticdb", "proto", _proto_ver, "arcticc", "pb2")
    )
    __path__.append(_protos_path)
else:
    raise NotImplementedError(f"We only support protobuf versions 3 & 4. You have {_protobuf.__version__}")
