import numpy
from arcticdb_ext.version_store import StringArray

def test_string():
    d = StringArray()
    s = d.get_array()