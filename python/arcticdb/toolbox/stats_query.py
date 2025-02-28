from datetime import datetime
import pandas as pd
from contextlib import contextmanager
import numpy as np

from arcticdb.exceptions import UserInputException
from arcticdb_ext.tools import StatsQuery

class StatsQueryTool:
    def __init__(self):
        self._create_time = datetime.now()
        self._is_context_manager = False
        StatsQuery.register_new_query_stat_tool()

    def __del__(self):
        StatsQuery.deregister_query_stat_tool()

    def __sub__(self, other):
        return self._populate_stats(other._create_time)

    def _populate_stats(self, other_time):
        df = pd.DataFrame(StatsQuery.get_stats())
        
        if "count" in df.columns:
            df["count"] = pd.to_numeric(df["count"], errors="coerce")
        
        groupby_cols = ["arcticdb_call", "stage", "key_type", "storage_op", "parallelized"]
        
        for col in groupby_cols:
            if col not in df.columns:
                df[col] = pd.Series(dtype='object')
        
        if "key_type" in df.columns:
            mask = df["key_type"].notna() & df["key_type"].ne("") & df["parallelized"].isna()
            df.loc[mask, "parallelized"] = "False"
        
        tmp_df = df.copy()
        for col in groupby_cols:
            tmp_df[col] = tmp_df[col].fillna("__NA__")
        
        grouped_data = []
        for name, group in tmp_df.groupby(groupby_cols):
            row = {}
            
            for i, col in enumerate(groupby_cols):
                row[col] = None if name[i] == "__NA__" else name[i]
            
            if "count" in group.columns and not group["count"].isna().all():
                count_sum = group["count"].sum()
                if not np.isnan(count_sum):
                    row["count"] = int(count_sum)
            
            if "time" in group.columns:
                time_values = pd.to_numeric(group["time"].dropna(), errors="coerce")
                
                if not time_values.empty:
                    time_buckets = {}
                    for time_val in time_values:
                        bucket = (time_val // 10) * 10
                        bucket_name = f"time_count_{bucket}"
                        time_buckets[bucket_name] = time_buckets.get(bucket_name, 0) + 1
                    
                    for bucket_name, count in time_buckets.items():
                        row[bucket_name] = count
            
            grouped_data.append(row)
        
        result_df = pd.DataFrame(grouped_data)
        
        fixed_cols = ["arcticdb_call", "stage", "key_type", "storage_op", "parallelized", "count"]
        time_cols = [col for col in result_df.columns if col.startswith("time_count_")]
        time_cols.sort(key=lambda x: int(x.split("_")[-1]))
        
        for col in time_cols:
            if col in result_df.columns:
                result_df[col] = result_df[col].fillna(0).astype("Int64")
        
        all_cols = fixed_cols + time_cols
        result_df = result_df.reindex(columns=[col for col in all_cols if col in result_df.columns])
        
        if "count" in result_df.columns:
            result_df["count"] = result_df["count"].replace({np.nan: None})
            result_df.loc[result_df["count"].notna(), "count"] = result_df.loc[result_df["count"].notna(), "count"].map(int)
        
        return result_df

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