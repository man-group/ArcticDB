"""
Query Statistics API for ArcticDB.

This module provides utilities for collecting query statistics.

.. warning::
    This API is unstable and not governed by ArcticDB's semantic versioning.
    It may change or be removed in future versions without notice.
"""

from contextlib import contextmanager
from typing import Dict, Any, Iterator
import arcticdb_ext.tools.query_stats as qs
from arcticdb_ext.exceptions import UserInputException


@contextmanager
def query_stats() -> Iterator[None]:
    """
    Context manager for enabling query statistics collection within a specific scope.
    
    When entering the context, query statistics collection is enabled.
    When exiting the context, it is automatically disabled.
    
    Raises:
        UserInputException: If query stats is already enabled.
        
    Example:
        >>> with query_stats():
        ...     store.list_symbols()
    
    .. warning::
        This API is unstable and not governed by semantic versioning.
    """
    if qs.is_enabled():
        # This will prohibit the unsupported nested context managers usage
        raise UserInputException("Query Stats is already enabled")
    enable()
    yield
    disable()
    

def get_query_stats() -> Dict[str, Any]:
    """
    Get collected query statistics.
    
    Returns:
        Dict[str, Any]: A dictionary containing statistics organized by key type,
            operation group, and task type. Each task contains timing and count information.
            
    Example output:
    {
        "storage_operations": {
            "S3_ListObjectsV2": {
                "total_time_ms": 83,
                "count": 3
            },
            "S3_GetObject": {
                "total_time_ms": 50,
                "count": 3,
                "size_bytes": 10
            }
        }
    }
    
    .. warning::
        This API is unstable and not governed by semantic versioning.
    """
    return qs.get_stats()


def reset_stats() -> None:
    """
    Reset all collected query statistics.
    
    This clears all statistics that have been collected since enabling
    the query statistics collection.
    
    .. warning::
        This API is unstable and not governed by semantic versioning.
    """
    qs.reset_stats()


def enable() -> None:
    """
    Enable query statistics collection.
    
    Once enabled, statistics will be collected for operations performed
    until disable() is called or the context manager exits.
    
    .. warning::
        This API is unstable and not governed by semantic versioning.
    """
    qs.enable()


def disable() -> None:
    """
    Disable query statistics collection.
    
    Stops collecting statistics for subsequent operations.
    Previously collected statistics remain available via get_query_stats().
    
    .. warning::
        This API is unstable and not governed by semantic versioning.
    """
    qs.disable()

