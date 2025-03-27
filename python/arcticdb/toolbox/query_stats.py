from contextlib import contextmanager
from typing import Dict, Any, Iterator
import arcticdb_ext.tools.query_stats as qs
from arcticdb_ext.exceptions import UserInputException

# Define enum values as lists since pybind11 enums are not iterable
_STATS_NAME_VALUES = [qs.StatsName.result_count, qs.StatsName.total_time_ms, qs.StatsName.count, qs.StatsName.encode_compressed_size_bytes, qs.StatsName.encode_uncompressed_size_bytes, qs.StatsName.decode_compressed_size_bytes, qs.StatsName.decode_uncompressed_size_bytes]
_STATS_GROUP_NAME_VALUES = [qs.GroupName.encode_key_type, qs.GroupName.decode_key_type, qs.GroupName.arcticdb_call, qs.GroupName.key_type, qs.GroupName.storage_ops]


@contextmanager
def query_stats() -> Iterator[None]:
    if qs.is_enabled():
        raise UserInputException("Query Stats is already enabled")
    enable()
    yield
    disable()


def get_query_stats() -> Dict[str, Any]:
    # Get raw stats from C++ layer
    raw_stats = qs.root_levels()
    
    # Transform raw stats into structured dictionary
    result = {}
    
    # Process each level
    for level in raw_stats:
        if level:
            _process_level(level, result)
        
    return result


def _process_level(level: Any, current_dict: Dict[str, Any]) -> None:
    def _get_enum_name(enum_value):
        return str(enum_value).split('.')[-1]
    
    encode_size_metrics = {}
    decode_size_metrics = {}
    
    '''
    Process stats array
    e.g.
    {
        "result_count": 1,
        "total_time_ms": 35,
        "count": 2,
        ...more stats...
    }
    '''
    stats_array = level.stats
    for stat_enum in _STATS_NAME_VALUES:
        stat_idx = int(stat_enum)
        if stats_array[stat_idx] > 0:
            stat_name = _get_enum_name(stat_enum)
            # For size metrics, only collect them for later use but don't add to current_dict
            if stat_name in ["encode_compressed_size_bytes", "encode_uncompressed_size_bytes"]:
                encode_size_metrics[stat_name] = stats_array[stat_idx]
                continue
            elif stat_name in ["decode_compressed_size_bytes", "decode_uncompressed_size_bytes"]:
                decode_size_metrics[stat_name] = stats_array[stat_idx]
                continue
                
            if stat_name in current_dict:
                current_dict[stat_name] += stats_array[stat_idx]
            else:
                current_dict[stat_name] = stats_array[stat_idx]
    '''
    Process next_level_maps
    e.g.
    [
        arcticdb_call : {
            "list_symbols": {
                ...more next_level_maps...
            }
        },
        key_type : {
            "SYMBOL_LIST": {
                ...more next_level_maps...
            },
            "VERSION_REF": {
                ...more next_level_maps...
            }
        },
        storage_ops : {},
        ...more groupable columns...
    ]
    '''
    next_level_maps = level.next_level_maps
    for group_enum in _STATS_GROUP_NAME_VALUES:
        group_idx = int(group_enum)
        
        if not next_level_maps[group_idx]:
            continue
            
        next_level_map = next_level_maps[group_idx]
        level_type = _get_enum_name(group_enum)
        
        # top level
        if group_enum == qs.GroupName.arcticdb_call:
            for op_name, op_level in next_level_map.items():
                if op_name not in current_dict:
                    current_dict[op_name] = {}
                _process_level(op_level, current_dict[op_name])
        else:
            if level_type not in current_dict:
                current_dict[level_type] = {}
                
            for sub_name, sub_layer in next_level_map.items():
                if group_enum == qs.GroupName.key_type or group_enum == qs.GroupName.encode_key_type or group_enum == qs.GroupName.decode_key_type:
                    sub_name = sub_name.split("::")[1]  # e.g. KeyType::VERSION_REF -> VERSION_REF
                
                if sub_name not in current_dict[level_type]:
                    current_dict[level_type][sub_name] = {}
                
                if level_type == "storage_ops":
                    op_map = current_dict[level_type][sub_name]
                    if sub_name == "PutObject":
                        for metric, value in encode_size_metrics.items():
                            op_map[metric] = op_map.get(metric, 0) + value
                        if _get_enum_name(qs.GroupName.encode_key_type) in current_dict:
                            if _get_enum_name(qs.GroupName.key_type) in op_map:
                                op_map[_get_enum_name(qs.GroupName.key_type)].update(current_dict[_get_enum_name(qs.GroupName.encode_key_type)])
                            else:
                                op_map[_get_enum_name(qs.GroupName.key_type)] = current_dict[_get_enum_name(qs.GroupName.encode_key_type)]
                            del current_dict[_get_enum_name(qs.GroupName.encode_key_type)]
                    if sub_name == "GetObject":
                        for metric, value in decode_size_metrics.items():
                            op_map[metric] = op_map.get(metric, 0) + value
                        if _get_enum_name(qs.GroupName.decode_key_type) in current_dict:
                            if _get_enum_name(qs.GroupName.key_type) in op_map:
                                op_map[_get_enum_name(qs.GroupName.key_type)].update(current_dict[_get_enum_name(qs.GroupName.decode_key_type)])
                            else:
                                op_map[_get_enum_name(qs.GroupName.key_type)] = current_dict[_get_enum_name(qs.GroupName.decode_key_type)]
                            del current_dict[_get_enum_name(qs.GroupName.decode_key_type)]

                _process_level(sub_layer, current_dict[level_type][sub_name])


def reset_stats() -> None:
    qs.reset_stats()


def enable() -> None:
    qs.enable()


def disable() -> None:
    qs.disable()

