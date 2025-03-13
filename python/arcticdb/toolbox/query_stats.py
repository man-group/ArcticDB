from contextlib import contextmanager
import numpy as np
import arcticdb_ext.tools.query_stats as qs
from arcticdb_ext.exceptions import UserInputException

# Define enum values as lists since pybind11 enums are not iterable
_STATS_NAME_VALUES = [qs.StatsName.result_count, qs.StatsName.total_time_ms, qs.StatsName.count]
_STATS_GROUP_NAME_VALUES = [qs.GroupName.arcticdb_call, qs.GroupName.key_type, qs.GroupName.storage_ops]


@contextmanager
def query_stats():
    if qs.is_enabled():
        raise UserInputException("Query Stats is already enabled")
    enable()
    yield
    disable()


def get_query_stats():
    # Get raw stats from C++ layer
    raw_stats = qs.root_levels()
    
    # Transform raw stats into structured dictionary
    result = {}
    
    # Process each layer
    for layer in raw_stats:
        if layer:
            _process_layer(layer, result)
        
    return result


def _process_layer(layer, current_dict):
    def _get_enum_name(enum_value):
        return str(enum_value).split('.')[-1]
    
    # Process stats array
    stats_array = layer.stats
    for stat_enum in _STATS_NAME_VALUES:
        stat_idx = int(stat_enum)
        if stats_array[stat_idx] > 0:
            stat_name = _get_enum_name(stat_enum)
            if stat_name not in current_dict:
                current_dict[stat_name] = stats_array[stat_idx]
            else:
                current_dict[stat_name] += stats_array[stat_idx]
    
    # Process next_layer_maps
    next_layer_maps = layer.next_layer_maps
    for group_enum in _STATS_GROUP_NAME_VALUES:
        group_idx = int(group_enum)
        
        if not next_layer_maps[group_idx]:
            continue
            
        next_layer_map = next_layer_maps[group_idx]
        
        # top level
        if group_enum == qs.GroupName.arcticdb_call:
            for op_name, op_layer in next_layer_map.items():
                if op_name not in current_dict:
                    current_dict[op_name] = {}
                _process_layer(op_layer, current_dict[op_name])
        else:
            layer_type = _get_enum_name(group_enum)

            if layer_type not in current_dict:
                current_dict[layer_type] = {}
            for sub_name, sub_layer in next_layer_map.items():
                if group_enum == qs.GroupName.key_type:
                    sub_name = sub_name.split("::")[1] # e.g. KeyType::VERSION_REF -> VERSION_REF
                if sub_name not in current_dict[layer_type]:
                    current_dict[layer_type][sub_name] = {}
                _process_layer(sub_layer, current_dict[layer_type][sub_name])


def reset_stats():
    qs.reset_stats()


def enable():
    qs.enable()


def disable():
    qs.disable()

