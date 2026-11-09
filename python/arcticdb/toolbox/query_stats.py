"""
Query Statistics API for ArcticDB.

This module provides utilities for collecting query statistics.

Notes
----------
!!! warning
    This API is unstable and not governed by ArcticDB's semantic versioning.
    It may change or be removed in future versions without notice.
"""

from contextlib import contextmanager
from typing import Dict, Any, Iterator
import arcticdb_ext.tools.query_stats as qs
from arcticdb.exceptions import UserInputException


@contextmanager
def query_stats() -> Iterator[None]:
    """
    Context manager for enabling query statistics collection within a specific scope.

    When entering the context, query statistics collection is enabled.
    When exiting the context, it is automatically disabled.

    Raises
    ------
    UserInputException
        If query stats is already enabled.

    Examples
    --------
    >>> with query_stats():
    ...     store.list_symbols()


    Notes
    ----------
    !!! warning
        This API is unstable and not governed by semantic versioning.
    """
    if qs.is_enabled():
        # This will prohibit the unsupported nested context managers usage
        raise UserInputException("Query Stats is already enabled")
    enable()
    try:
        yield
    finally:
        disable()


def get_query_stats() -> Dict[str, Any]:
    """
    Get collected query statistics.

    Returns
    -------
    Dict[str, Any]:
        A dictionary containing statistics organized by storage operation and key type.
        Example output:
        {
            "storage_operations": {
                "S3_PutObject": {
                    "TABLE_DATA": {
                        "count": 1,
                        "total_time_ms": 108,
                        "size_bytes": 103,
                        "failure_count": 0,
                        "failure_time_ms": 0,
                        "attempts": 3,
                        "attempt_time_ms": 33,
                        "network_errors": 2,
                        "server_errors": 0,
                        "ttfb_ms": 15,
                        "connect_ms": 4,
                        "new_connections": 1
                    }
                }
            }
        }

        Each call is counted once, as succeeded or failed:

        - count, total_time_ms: calls that succeeded. A read, HeadObject or ListObjectsV2 that finds no key or bucket
          succeeded, as the storage answered.
        - failure_count, failure_time_ms: calls that failed with an error or raised.
        - size_bytes: bytes read or written by the calls that succeeded.

        Both times are of the whole call, including the retries in the AWS SDK and the waits between them.

        The other stats are per HTTP attempt to S3, over the attempts of all the calls. They are not recorded for
        S3_GetObjectAsync:

        - attempts, attempt_time_ms: HTTP attempts and the time spent in them. attempts - count - failure_count is
          the number of retries.
        - network_errors: attempts that did not get a complete HTTP response, e.g. DNS failure or connect timeout.
        - server_errors: attempts that got an HTTP 5xx or 429 response, e.g. SlowDown.
        - ttfb_ms, connect_ms: over the attempts that got a complete response, the sum of the time to the first
          response byte and the sum of the time to connect. (ttfb_ms - connect_ms) / (attempts - network_errors) is
          the mean time S3 took to answer. Both are in whole milliseconds per attempt, rounded down.
        - new_connections: connections opened. Attempts that reuse a pooled connection open none.

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by semantic versioning.
    """
    return qs.get_stats()


def reset_stats() -> None:
    """
    Reset all collected query statistics.

    This clears all statistics that have been collected since enabling
    the query statistics collection.

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by semantic versioning.
    """
    qs.reset_stats()


def enable() -> None:
    """
    Enable query statistics collection.

    Once enabled, statistics will be collected for operations performed
    until disable() is called or the context manager exits.

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by semantic versioning.
    """
    qs.enable()


def disable() -> None:
    """
    Disable query statistics collection.

    Stops collecting statistics for subsequent operations.
    Previously collected statistics remain available via get_query_stats().

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by semantic versioning.
    """
    qs.disable()
