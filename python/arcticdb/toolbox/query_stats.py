from contextlib import contextmanager
from typing import Dict, Any, Iterator
import arcticdb_ext.tools.query_stats as qs
from arcticdb_ext.exceptions import UserInputException
from arcticdb_ext.storage import KeyType


@contextmanager
def query_stats() -> Iterator[None]:
    if qs.is_enabled():
        raise UserInputException("Query Stats is already enabled")
    enable()
    yield
    disable()


def get_query_stats() -> Dict[str, Any]:
    raw_stats = qs.get_stats()
    
    result = {}
    
    for key_type_idx in range(len(raw_stats.keys_stats_)):
        key_type_str = KeyType(key_type_idx).name
        key_type_data = {"storage_ops": {}}
        has_data = False
        
        for task_type_idx in range(len(raw_stats.keys_stats_[key_type_idx])):
            op_stats = raw_stats.keys_stats_[key_type_idx][task_type_idx]
            
            task_type_name = qs.TaskType(task_type_idx).name
            stats_data = {}
            
            result_count = op_stats.result_count
            total_time_ms = op_stats.total_time_ms
            count = op_stats.count
            
            if result_count > 0:
                stats_data["result_count"] = result_count
                has_data = True
                
            if total_time_ms > 0:
                stats_data["total_time_ms"] = total_time_ms
                has_data = True
                
            if count > 0:
                stats_data["count"] = count
                has_data = True
            
            logical_key_counts = op_stats.logical_key_counts
            key_type_counts = {}
            
            for logical_key_idx, logical_key_count in enumerate(logical_key_counts):
                if logical_key_count > 0:
                    logical_key_type_str = KeyType(logical_key_idx).name
                    key_type_counts[logical_key_type_str] = {"count": logical_key_count}
                    has_data = True
            
            if key_type_counts:
                stats_data["key_type"] = key_type_counts
            
            if stats_data:
                key_type_data["storage_ops"][task_type_name] = stats_data
        
        if has_data:
            result[key_type_str] = key_type_data
    
    return result


def reset_stats() -> None:
    qs.reset_stats()


def enable() -> None:
    qs.enable()


def disable() -> None:
    qs.disable()

