"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import Type, Callable, Dict, List
from enum import Enum
from collections import defaultdict

import arcticdb_ext.exceptions as _cpp
from arcticdb_ext.exceptions import (
    ErrorCode as _ErrorCode,
    ErrorCategory,
    enum_value_to_prefix as _enum_value_to_prefix,
    get_error_category,
)

# Not using arcticdb.log because arcticdb.__init__.py practically loads everything
from arcticdb_ext.log import log as _log, LogLevel as _LogLevel, LoggerId as _LoggerId


class ErrorCodeEnumBase(Enum):
    """No members. For type-checking only"""


class NormalizationError:
    """For auto-complete use. Will be overwritten by the for loop below."""


_py_module = globals()
_enum_type_to_exception: Dict[Type[ErrorCodeEnumBase], Type[Exception]] = {}
_enum_index: Dict[ErrorCategory, List[_ErrorCode]] = defaultdict(list)

for member in _ErrorCode.__members__.values():
    _enum_index[get_error_category(member)].append(member)


def _exception_name(category):
    output = ""
    tokens = category.split("_")
    for token in tokens:
        output += token[0] + token[1:].lower()

    return output


# Dynamically create each (sub-)ErrorCode enum:
for cat, error_codes in _enum_index.items():
    pascal_name = _exception_name(cat.name)
    sub_enum_name = pascal_name + "Error"
    per_cat_enum = ErrorCodeEnumBase(sub_enum_name, {member.name: member.value for member in error_codes})
    exception_type = getattr(_cpp, pascal_name + "Exception")
    _enum_type_to_exception[per_cat_enum] = exception_type
    _py_module[sub_enum_name] = per_cat_enum

del _enum_index


def arcticdb_raise(error_code: ErrorCodeEnumBase, error_msg_provider: Callable[[], str]):
    """Similar to the ``preconditions.hpp raise`` function

    Parameters
    ----------
    error_code
        One of the category-specific enum types in this module. The exception thrown will be based on the category.
    error_msg_provider
        A lambda to allow f-strings and other native formatting facilities in Python.
        Should return details of the error without the error code prefix.
    """
    msg = error_msg_provider()
    msg = f"{_enum_value_to_prefix[error_code.value]} {msg}"
    _log(_LoggerId.ROOT, _LogLevel.ERROR, msg)
    exp = _enum_type_to_exception[type(error_code)]
    raise exp(msg)
