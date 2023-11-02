from arcticdb_ext.arrow import get_test_array
import pyarrow as pa

def test_buffer():
    arr = get_test_array()
    py_arr = pa.Array._import_from_c(arr.addr(), arr.schema().addr())
    print(py_arr)

