#import ray
#import arcticdb

def test_thing(lmdb_version_store):
    lmdb_version_store.write("Hello", 23)