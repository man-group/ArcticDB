from datetime import datetime
import pandas as pd

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


    def __sub__(self, other):
        # raise NotImplementedError
        return pd.DataFrame(self._stats)
    
    @classmethod
    def reset_stats(cls):
        cls._stats = []
