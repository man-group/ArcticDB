import time
import pandas as pd
from contextlib import contextmanager
import numpy as np

from arcticdb.exceptions import UserInputException
from arcticdb_ext.tools import QueryStats

class QueryStatsTool:
    def __init__(self):
        self._create_time = time.time_ns()
        self._is_context_manager = False
        QueryStats.register_new_query_stat_tool()

    def __del__(self):
        QueryStats.deregister_query_stat_tool()

    def __sub__(self, other):
        return self._populate_stats(other._create_time, self._create_time)

    def _populate_stats(self, start_time, end_time):
        df = pd.DataFrame(QueryStats.get_stats())
        if df.empty:
            return {}
        
        df["exec_time"] = pd.to_numeric(df["exec_time"], errors="coerce")
        df = df[df["exec_time"].between(start_time, end_time)]
        df = df.drop(columns=["exec_time"])
        
        if "result_count" in df.columns:
            df["result_count"] = pd.to_numeric(df["result_count"], errors="coerce")
        
        groupby_cols = ["arcticdb_call", "stage", "key_type", "storage_op"]
        
        for col in groupby_cols:
            if col not in df.columns:
                df[col] = pd.Series(dtype='object')

        def process_time_values(time_values):
            time_buckets = {}
            for time_val in time_values:
                bucket = (time_val // 10) * 10
                time_buckets[str(bucket)] = time_buckets.get(str(bucket), 0) + 1
            return time_buckets

        def get_non_grouped_times(data, current_level):
            # Only process NaN values for the current grouping level
            mask = data[current_level].isna()
            if not mask.any():
                return {}
            
            time_values = pd.to_numeric(data.loc[mask, "time"].dropna(), errors="coerce")
            if not time_values.empty:
                time_buckets = process_time_values(time_values)
                if time_buckets:
                    return {"time": time_buckets}
            return {}

        def process_group(group_data, is_leaf):
            result = {}
            
            if is_leaf:
                numeric_cols = [col for col in group_data.columns if col not in groupby_cols and col != "time"]
                for col in numeric_cols:
                    values = pd.to_numeric(group_data[col].dropna(), errors="coerce")
                    if not values.empty:
                        total = values.sum()
                        if not np.isnan(total):
                            result[col] = int(total)
                
                time_values = pd.to_numeric(group_data["time"].dropna(), errors="coerce")
                if not time_values.empty:
                    time_buckets = process_time_values(time_values)
                    if time_buckets:
                        result["time"] = time_buckets
            
            return result

        def group_by_level(data, columns):
            if not columns:
                return process_group(data, True)
            
            result = {}
            current_col = columns[0]
            
            non_grouped = get_non_grouped_times(data, current_col)
            result.update(non_grouped)
            
            grouped = data[~data[current_col].isna()].groupby(current_col)
            nested = {}
            
            for name, group in grouped:
                sub_result = group_by_level(group, columns[1:])
                if sub_result:
                    nested[str(name)] = sub_result
            
            if nested:
                result[f"{current_col}s"] = nested
            
            return result

        result = {}
        for call_name, call_group in df.groupby("arcticdb_call"):
            if pd.isna(call_name):
                continue
            call_result = group_by_level(call_group, groupby_cols[1:])
            if call_result:
                result[str(call_name)] = call_result
        
        return result

    @classmethod
    def context_manager(cls):
        @contextmanager
        def _func():
            query_stats_tools = cls()
            query_stats_tools._is_context_manager = True
            yield query_stats_tools
            query_stats_tools._end_time = time.time_ns()
        return _func()

    def get_query_stats(self):
        if self._is_context_manager:
            return self._populate_stats(self._create_time, self._end_time)
        else:
            raise UserInputException("get_query_stats should be used with a context manager initialized QueryStatsTools")

    @classmethod
    def reset_stats(cls):
        QueryStats.reset_stats()