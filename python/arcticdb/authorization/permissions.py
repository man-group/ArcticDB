"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticc.pb2.storage_pb2 import Permissions
from enum import IntEnum
from typing import AnyStr


class OpenMode(IntEnum):
    READ = (1,)
    WRITE = (3,)  # implies READ
    DELETE = 7  # implies READ + WRITE


def create_permission(library, write=False):
    # type: (AnyStr, bool) -> Permissions
    perms = Permissions()
    perms.library = library
    if write:
        perms.write.enabled = True
    else:
        perms.read.enabled = True

    return perms


def perms_to_openmode(perms):
    # type: (Permissions) -> OpenMode
    return OpenMode.DELETE if perms.write.enabled else OpenMode.READ
