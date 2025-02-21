import pandas as pd
import numpy as np
from arcticc.pb2.descriptors_pb2 import SortedValue, TypeDescriptor


def test_value_type_is_protobuf(lmdb_version_store):
    lib = lmdb_version_store
    symbol = "test_value_type_proto"
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=25, freq="H").repeat(2),
            "col1": np.arange(1, 51),
            "col2": [f"a{i:02d}" for i in range(1, 51)],
        }
    ).set_index("timestamp")
    lib.write(symbol, df)
    symbol_info = lib.get_info(symbol)
    assert symbol_info["col_names"]["index_dtype"][0].value_type == TypeDescriptor.NANOSECONDS_UTC
