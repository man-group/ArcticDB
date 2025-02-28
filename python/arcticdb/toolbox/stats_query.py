from datetime import datetime
import pandas as pd
from contextlib import contextmanager

from arcticdb.exceptions import UserInputException
from arcticdb_ext.tools import StatsQuery

class StatsQueryTool:
    def __init__(self):
        self._create_time = datetime.now()
        self._is_context_manager = False
        StatsQuery.register_new_query_stat_tool()

    def __del__(self):
        StatsQuery.unregister_query_stat_tool()

    def __sub__(self, other):
        return self._populate_stats(other._create_time)

    def _populate_stats(self, other_time):
        raw_stats = StatsQuery.get_stats()
        return raw_stats

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
        StatsQuery.reset_stats()