"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import attr
import warnings

from typing import Any, Optional


@attr.s(slots=True, auto_attribs=False)
class VersionedItem:
    """
    Return value for many operations that captures the result and associated information.

    Attributes
    ----------
    library: str
        Library this result relates to.
    symbol: str
        Read or modified symbol.
    data: Any
        For data retrieval (read) operations, contains the data read.
        For data modification operations, the value might not be populated.
    version: int
        For data retrieval operations, the version the `as_of` argument resolved to.
        For data modification operations, the version the data has been written under.
    metadata: Any
        The metadata saved alongside `data`.
        Availability depends on the method used and may be different from that of `data`.
    host: Optional[str]
        Informational / for backwards compatibility.
    """

    symbol: str = attr.ib()
    library: str = attr.ib()
    data: Any = attr.ib(repr=lambda d: "n/a" if d is None else str(type(d)))
    version: int = attr.ib()
    metadata: Any = attr.ib(default=None)
    host: Optional[str] = attr.ib(default=None)

    def __iter__(self):  # Backwards compatible with the old NamedTuple implementation
        warnings.warn("Don't iterate VersionedItem. Use attrs.astuple() explicitly", SyntaxWarning, stacklevel=2)
        return iter(attr.astuple(self))

