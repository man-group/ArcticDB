from arcticdb.util.test import sample_dataframe
from arcticdb_ext.storage import KeyType


def test_symbol_sizes(basic_store):
    sizes = basic_store.version_store.scan_object_sizes_by_stream()
    assert sizes == dict()

    sym_names = []
    for i in range(5):
        df = sample_dataframe(100, i)
        sym = "sym_{}".format(i)
        sym_names.append(sym)
        basic_store.write(sym, df)

    sizes = basic_store.version_store.scan_object_sizes_by_stream()

    assert len(sizes) == 5
    for s in sym_names:
        assert s in sizes

    assert len(sizes["sym_0"]) == 3
    assert sizes["sym_0"][KeyType.VERSION] < 1000
    assert sizes["sym_0"][KeyType.TABLE_INDEX] < 5000
    assert sizes["sym_0"][KeyType.TABLE_DATA] < 15000


def test_symbol_sizes_big(basic_store):
    df = sample_dataframe(1000)
    basic_store.write("sym", df)

    sizes = basic_store.version_store.scan_object_sizes_by_stream()

    assert len(sizes) == 1
    assert sizes["sym"][KeyType.VERSION] < 1000
    assert sizes["sym"][KeyType.TABLE_INDEX] < 5000
    assert 15_000 < sizes["sym"][KeyType.TABLE_DATA] < 100_000


"""
Manual testing lines up well:

In [11]: lib._nvs.version_store.scan_object_sizes_by_stream()
Out[11]: 
{'sym': {<KeyType.VERSION: 4>: 1160,
  <KeyType.TABLE_INDEX: 3>: 2506,
  <KeyType.TABLE_DATA: 2>: 5553859}}

In [12]: lib
Out[12]: Library(Arctic(config=LMDB(path=/home/alex/source/ArcticDB/python/blah)), path=tst3, storage=lmdb_storage)

(310) ➜  tst3 git:(size-by-symbol) ✗ du -h .  
5.5M    .
(310) ➜  tst3 git:(size-by-symbol) ✗ pwd 
/home/alex/source/ArcticDB/python/blah/tst3
"""


def test_scan_object_sizes(basic_store):
    df = sample_dataframe(1000)
    basic_store.write("sym", df)

    sizes = basic_store.version_store.scan_object_sizes()

    assert len(sizes) == 5
    assert sizes[KeyType.VERSION][1] < 1000
    assert sizes[KeyType.TABLE_INDEX][1] < 5000
    assert 15_000 < sizes[KeyType.TABLE_DATA][1] < 100_000

    assert sizes[KeyType.VERSION][0] == 1
    assert sizes[KeyType.TABLE_INDEX][0] == 1
    assert sizes[KeyType.TABLE_DATA][0] == 1
