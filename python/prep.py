import itertools
import multiprocessing

import pandas as pd

from arcticdb import Arctic
from arcticdb.version_store.library import WritePayload, DeleteRequest

ac = Arctic("lmdb:///tmp/list-versions")


def _library_name(n_syms: int, n_snaps: int):
    return f"n_syms-{n_syms}_n_snaps-{n_snaps}"

def snap(lib, i):
    lib.snapshot(f"snap_{i}")


# (syms, snaps)
n_syms = (1000, 100_000)
n_snaps = (0, 1, 1_000)
input_data = itertools.product(n_syms, n_snaps)

def create_lib(n_syms, n_snaps):
    print(f"Creating lib {_library_name(n_syms, n_snaps)}")
    lib_name = _library_name(n_syms, n_snaps)
    lib = ac.get_library(lib_name, create_if_missing=True)
    lib._nvs.version_store.clear()

    df = pd.DataFrame({'a': [1]})
    payloads = [WritePayload(f"{i}", df) for i in range(n_syms)]
    print(f"Creating symbols")
    lib.write_batch(payloads)
    assert len(lib.list_symbols()) == n_syms

    print(f"Creating snapshots")
    snapshot_args = [(lib, i) for i in range(n_snaps)]
    with multiprocessing.Pool(20) as p:
        p.starmap(snap, snapshot_args)

    assert len(lib.list_snapshots()) == n_snaps

    if n_snaps:
        print(f"Deleting symbols")
        delete_payloads = [DeleteRequest(f"{i}", [0]) for i in range(n_syms)]
        lib.delete_batch(delete_payloads)
        assert len(lib.list_symbols()) == 0

    print(f"Done")


for n_syms, n_snaps in input_data:
    create_lib(n_syms, n_snaps)
