"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb_ext.exceptions import *
from arcticdb_ext.exceptions import ArcticException as ArcticNativeException
from arcticdb_ext.storage import DuplicateKeyException, NoDataFoundException, PermissionException
from arcticdb_ext.version_store import NoSuchVersionException, StreamDescriptorMismatch


class ArcticDbNotYetImplemented(ArcticException):
    pass


# Backwards compat - this is the old name of ArcticDbNotYetImplemented
ArcticNativeNotYetImplemented = ArcticDbNotYetImplemented


class LibraryNotFound(ArcticException):
    pass


class MismatchingLibraryOptions(ArcticException):
    pass


class LmdbOptionsError(ArcticException):
    pass
