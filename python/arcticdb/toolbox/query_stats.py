from datetime import datetime
import pandas as pd
from contextlib import contextmanager

from arcticdb.exceptions import UserInputException

class QueryStatsTools:
    # For demo
    _stats = [
        {"arcticdb_call": "list_symbols", "stage": None, "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": 1, "time_count_50": None, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
        {"arcticdb_call": "list_symbols", "stage": "list", "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": 1, "time_count_50": None, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
        {"arcticdb_call": "list_symbols", "stage": "list", "key_type": "sl", "storage_op": "ListObjectsV2", "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": 1, "time_count_50": None, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
        {"arcticdb_call": "read", "stage": None, "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": None, "time_count_50": None, "time_count_80": None, "time_count_100": 1, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
        {"arcticdb_call": "read", "stage": "find_version", "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": None, "time_count_50": 1, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
        {"arcticdb_call": "read", "stage": "find_version", "key_type": "vref", "storage_op": "GetObject", "parallelized": False, "count": 1, "ver": 1, "tindex": 1, "tdata": None, "tall": None, "uncompressed_size": 15, "compressed_size": None, "bandwidth": 260, "time_count_30": None, "time_count_50": 1, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
        {"arcticdb_call": "read", "stage": "read", "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": None, "time_count_50": None, "time_count_80": 1, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
        {"arcticdb_call": "read", "stage": "read", "key_type": "tdata", "storage_op": "GetObject", "parallelized": True, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": 2000, "compressed_size": 200, "bandwidth": 5714, "time_count_30": 1, "time_count_50": None, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
        {"arcticdb_call": "read", "stage": "read", "key_type": "tindex", "storage_op": "GetObject", "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": 1, "tall": None, "uncompressed_size": 16, "compressed_size": None, "bandwidth": 301, "time_count_30": None, "time_count_50": 1, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
    ]

    def __init__(self):
        self._create_time = datetime.now()
        self._is_context_manager = False

    def __sub__(self, other):
        return self._populate_stats(other._create_time)

    def _populate_stats(self, other_time):
        return pd.DataFrame(self._stats)
    
    @classmethod
    def context_manager(cls):
        @contextmanager
        def _func():
            query_stats_tools = cls()
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
        pass # This is not implemented in the demo code
