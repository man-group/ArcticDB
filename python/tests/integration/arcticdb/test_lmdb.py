from arcticdb import Arctic
import pandas as pd

from arcticdb.util.test import assert_frame_equal


def test_batch_read_only_segfault_regression(tmpdir):
    # See Github issue #520
    # This segfaults with arcticdb==1.5.0
    ac = Arctic(f"lmdb://{tmpdir}/lmdb_instance")
    ac.create_library("test_lib")
    lib = ac["test_lib"]
    df = pd.DataFrame({"a": list(range(100))}, index=list(range(100)))
    for i in range(100):
        lib.write(str(i), df, prune_previous_versions=True)

    # New Arctic instance is essential to repro the bug
    fresh_lib = Arctic(f"lmdb://{tmpdir}/lmdb_instance")["test_lib"]
    vis = fresh_lib.read_batch([str(i) for i in range(100)])  # used to crash
    assert len(vis) == 100
    assert_frame_equal(vis[0].data, df)
