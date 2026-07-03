"""Memory attribution probe for the filter read path.

One measurement per process (clean high-water). Config via env:
  RESIDENCY   VersionStore.NumProcessingUnitsLive (default 0 = kill switch; "default" leaves unset)
  READ_WINDOW VersionStore.SegmentReadWindow (unset = 2*io default)
Allocator behaviour is controlled by the launching env (MALLOC_ARENA_MAX, LD_PRELOAD, MALLOC_CONF).

Reports: wall, peak RSS (ru_maxrss and VmHWM), RSS before/after malloc_trim(0), and the
anon/file split of RSS at the end from /proc/self/smaps_rollup.
"""

import ctypes
import os
import resource
import time

from arcticdb import Arctic, QueryBuilder
from arcticdb_ext import set_config_int

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
residency = os.environ.get("RESIDENCY", "0")
if residency != "default":
    set_config_int("VersionStore.NumProcessingUnitsLive", int(residency))
if "READ_WINDOW" in os.environ:
    set_config_int("VersionStore.SegmentReadWindow", int(os.environ["READ_WINDOW"]))


def rss_kib():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return -1


def vmhwm_kib():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmHWM:"):
                return int(line.split()[1])
    return -1


def smaps_rollup():
    out = {}
    try:
        with open("/proc/self/smaps_rollup") as f:
            for line in f:
                for key in ("Rss:", "Anonymous:", "Shared_Clean:", "Shared_Dirty:", "Private_Dirty:"):
                    if line.startswith(key):
                        out[key.rstrip(":")] = int(line.split()[1])
    except FileNotFoundError:
        pass
    return out


def measure(lib, sym):
    q = QueryBuilder()
    q = q[q["f_0"] == -1.0]
    return lib.read(sym, query_builder=q)


ac = Arctic(URI)
lib = ac[LIB_NAME]

t0 = time.perf_counter()
result = measure(lib, SYMBOL_ZEROS)
dt = time.perf_counter() - t0

peak_maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
hwm = vmhwm_kib() / 1024
rss_before = rss_kib() / 1024
roll = smaps_rollup()

libc = ctypes.CDLL("libc.so.6")
reclaimed_flag = libc.malloc_trim(0)
rss_after = rss_kib() / 1024

cfg = f"RESIDENCY={residency} READ_WINDOW={os.environ.get('READ_WINDOW', 'default')} ARENA_MAX={os.environ.get('MALLOC_ARENA_MAX', 'unset')}"
print(f"[{cfg}]")
print(f"  wall                = {dt:.2f}s")
print(f"  peak ru_maxrss      = {peak_maxrss:.1f} MiB")
print(f"  VmHWM (peak)        = {hwm:.1f} MiB")
print(f"  VmRSS before trim   = {rss_before:.1f} MiB")
print(f"  VmRSS after trim    = {rss_after:.1f} MiB   (trim reclaimed {rss_before - rss_after:.1f} MiB)")
if roll:
    print(
        f"  smaps_rollup end    = Rss {roll.get('Rss', 0) / 1024:.1f} | "
        f"Anon {roll.get('Anonymous', 0) / 1024:.1f} | "
        f"SharedClean {roll.get('Shared_Clean', 0) / 1024:.1f} MiB"
    )
