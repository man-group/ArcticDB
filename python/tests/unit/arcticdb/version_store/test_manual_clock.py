from arcticdb_ext.version_store import ManualClockVersionStore


def test_ManualClockVersionStore_static():
    nanos = 1_690_087_316 * 1_000_000_000
    ManualClockVersionStore.time = nanos
    assert ManualClockVersionStore.time == nanos


def test_ManualClockVersionStore(lmdb_version_store_v2):
    lib = lmdb_version_store_v2
    lib.version_store = ManualClockVersionStore(lib._library)
    nanos = 1_690_087_316 * 1_000_000_000
    lib.time = nanos
    assert lib.time == nanos
