from arcticdb import Arctic, QueryBuilder
from arcticdb_ext import set_config_int
import os
import resource
import time

URI = (
    "s3://s3.gdc.res.m:alpha-data-dev-arcticnative-ahl-research-2"
    "?access=PSFBIAZFKAAAPHGD"
    "&secret=01330000a+950d2890F14714bfd183KPCLMPEJEBLGBFGN"
    "&path_prefix=aseaton_tst"
)

SYMBOL_ZEROS = "f64_10m_100c_zeros"
LIB_NAME = "col_stats_mem"

set_config_int("VersionStore.NumIOThreads", 96)
set_config_int("VersionStore.NumCPUThreads", 64)
residency = os.environ.get("RESIDENCY", "0")  # 0 = kill switch; "default" leaves the config unset
if residency != "default":
    set_config_int("VersionStore.NumProcessingUnitsLive", int(residency))
if "READ_WINDOW" in os.environ:
    set_config_int("VersionStore.SegmentReadWindow", int(os.environ["READ_WINDOW"]))


def measure_fn(lib, sym):
    q = QueryBuilder()
    # -1.0 never occurs (zeros are 0.0, random values are in [0, 1)), so the result is empty and the
    # measurement reflects input-decode residency rather than the size of the output frame.
    q = q[q["f_0"] == -1.0]
    return lib.read(sym, query_builder=q)


ac = Arctic(URI)
lib = ac[LIB_NAME]

r0 = resource.getrusage(resource.RUSAGE_SELF)
t0 = time.perf_counter()
result = measure_fn(lib, SYMBOL_ZEROS)
dt = time.perf_counter() - t0
r1 = resource.getrusage(resource.RUSAGE_SELF)
peak_kb = r1.ru_maxrss
print(f"wall= {dt:.2f}s")
print(f"peak ru_maxrss        = {peak_kb / 1024:.1f} MiB")
