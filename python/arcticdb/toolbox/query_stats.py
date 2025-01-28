from datetime import datetime
import pandas as pd
from contextlib import contextmanager

from arcticdb.exceptions import UserInputException

class QueryStatsTools:
    # For demo
    _stats = [{
        "arcticdb_call": "read",
        "stage": "list",
        "key_type": "ref",
        "library": "a",
        "storage_op": "list",
        "count": 1,
        "max_time": 1,
        "min_time": 1,
        "avg_time": 1,
        "uncompressed_size": 10,
        "compressed_size": 10,
        "retries": 0,
        },
        {"arcticdb_call": "write",
        "stage": "write",
        "key_type": "d",
        "library": "a1",
        "storage_op": "write",
        "count": 5,
        "max_time": 10,
        "min_time": 20,
        "avg_time": 15,
        "uncompressed_size": 1000,
        "compressed_size": 20,
        "retries": 0,
        }
    ]

    def __init__(self, nvs):
        self._nvs = nvs
        self._create_time = datetime.now()
        self._is_context_manager = False

    def __sub__(self, other):
        return self._populate_stats(other._create_time)

    def _populate_stats(self, other_time):
        return pd.DataFrame(self._stats)
    
    @classmethod
    def context_manager(cls, lib):
        @contextmanager
        def _func():
            query_stats_tools = cls(lib._nvs)
            query_stats_tools._is_context_manager = True
            yield query_stats_tools
            query_stats_tools._end_time = datetime.now()
        return _func()
    
    def get_query_stats(self):
        if self._is_context_manager:
            return self._populate_stats(self._end_time)
        else:
            raise UserInputException("get_query_stats should be used with a context manager initialized QueryStatsTools")
    
    @classmethod
    def reset_stats(cls):
        pass
