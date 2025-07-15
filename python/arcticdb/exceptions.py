"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from typing import List as _List

from arcticdb_ext.exceptions import *
from arcticdb_ext.exceptions import ArcticException as ArcticNativeException, DuplicateKeyException, PermissionException
from arcticdb_ext.storage import NoDataFoundException
from arcticdb_ext.storage import UnknownLibraryOption, UnsupportedLibraryOptionValue
from arcticdb_ext.version_store import NoSuchVersionException, StreamDescriptorMismatch
from arcticdb_ext.version_store import KeyNotFoundInTokenInfo as _KeyNotFoundInTokenInfo


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


class DataTooNestedException(UserInputException):
    pass


class UnsupportedKeyInDictionary(UserInputException):
    pass


class MissingKeysInTokensError(ArcticException):
    """This error is only raised when finalizing staged data using StageResult tokens. This describes which
    tokens failed to finalize because they refer to keys that no longer exist in storage. This is probably
    because they have been removed or finalized already."""

    def __init__(self, msg, tokens_with_missing_keys: _List[_KeyNotFoundInTokenInfo]):
        super().__init__(msg)
        self.msg = msg
        self.tokens_with_missing_keys = tokens_with_missing_keys

    def __repr__(self):
        return f"MissingKeysInTokensError(msg={repr(self.msg)}, tokens_with_missing_keys={repr(self.tokens_with_missing_keys)})"

    def __str__(self):
        return f"Tokens with missing keys are [{self.tokens_with_missing_keys}] msg={str(self.msg)}"

    def __eq__(self, other: "MissingKeysInTokensError"):
        return self.msg == other.msg and self.tokens_with_missing_keys == other.tokens_with_missing_keys
