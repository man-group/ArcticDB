import gc
import multiprocessing
import threading
import numpy as np
import pandas as pd
import pytest
from arcticdb import Arctic
from arcticdb_ext import set_config_int, unset_config_int

HOLD = {}
ROWS = 200_000


def _child_drop():
    HOLD.clear()
    gc.collect()


def _child_drop_then_read():
    lib = HOLD["lib"]
    HOLD.clear()
    del lib
    gc.collect()


def _child_read():
    HOLD["lib"].read("sym")


def _dump_hung_child(pid):
    import subprocess

    try:
        out = subprocess.run(
            ["gdb", "-p", str(pid), "-batch", "-ex", "thread apply all bt 14"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        print(f"=== HUNG CHILD {pid} ===\n{out.stdout}\n{out.stderr}", flush=True)
    except Exception as e:
        print(f"=== could not dump {pid}: {e} ===", flush=True)


def _reader(stop):
    while not stop.is_set():
        try:
            HOLD["lib"].read("sym")
        except Exception:
            pass


@pytest.mark.parametrize("s3_async", [0, 1])
@pytest.mark.parametrize("child_fn", [_child_drop, _child_read], ids=["drop", "read"])
@pytest.mark.parametrize("nchild", [1, 4])
def test_fork_while_io_in_flight(s3_storage, lib_name, s3_async, child_fn, nchild):
    set_config_int("S3.Async", s3_async)
    try:
        ac = Arctic(s3_storage.arctic_uri)
        lib = ac.create_library(lib_name)
        lib.write("sym", pd.DataFrame({"a": np.arange(ROWS), "b": np.arange(ROWS) * 1.5}))
        lib.read("sym")
        HOLD["ac"] = ac
        HOLD["lib"] = lib
        del ac, lib
        gc.collect()

        stop = threading.Event()
        readers = [threading.Thread(target=_reader, args=(stop,), daemon=True) for _ in range(4)]
        for t in readers:
            t.start()

        ctx = multiprocessing.get_context("fork")
        hung, crashed, launched = 0, 0, 0
        for i in range(40):
            procs = [ctx.Process(target=child_fn) for _ in range(nchild)]
            for p in procs:
                p.start()
                launched += 1
            for p in procs:
                p.join(30)
                if p.is_alive():
                    _dump_hung_child(p.pid)
                    p.kill()
                    p.join()
                    hung += 1
                    print(f"iteration {i}: child hung", flush=True)
                elif p.exitcode != 0:
                    crashed += 1
                    print(f"iteration {i}: child exitcode {p.exitcode}", flush=True)
        stop.set()
        for t in readers:
            t.join(30)
        # Counts first: pytest truncates long assertion messages, and a per-child list gets cut off exactly
        # when there is most to report.
        assert hung == 0 and crashed == 0, f"of {launched} forked children, {crashed} crashed and {hung} hung"
    finally:
        HOLD.clear()
        gc.collect()
        unset_config_int("S3.Async")
