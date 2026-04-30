"""Read the malloc_trim symbol from LMDB N times and dump glibc heap state.

Optionally call malloc_trim(0) and dump again so the contribution of explicit
trimming can be measured against ordinary free()-driven release.
"""
import argparse
import ctypes
import gc
import resource
import xml.etree.ElementTree as ET

from arcticdb import Arctic

LMDB_URI = "lmdb:///tmp/arcticdb_malloc_trim_lmdb"
LIBRARY = "malloc_trim"
SYMBOL = "data"

_libc = ctypes.CDLL("libc.so.6")
_libc.open_memstream.argtypes = [ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_size_t)]
_libc.open_memstream.restype = ctypes.c_void_p
_libc.malloc_info.argtypes = [ctypes.c_int, ctypes.c_void_p]
_libc.malloc_info.restype = ctypes.c_int
_libc.malloc_trim.argtypes = [ctypes.c_size_t]
_libc.malloc_trim.restype = ctypes.c_int
_libc.fclose.argtypes = [ctypes.c_void_p]
_libc.fclose.restype = ctypes.c_int
_libc.free.argtypes = [ctypes.c_void_p]
_libc.free.restype = None


def malloc_info_xml() -> str:
    buf = ctypes.c_char_p()
    size = ctypes.c_size_t(0)
    fp = _libc.open_memstream(ctypes.byref(buf), ctypes.byref(size))
    if not fp:
        raise OSError("open_memstream failed")
    try:
        rc = _libc.malloc_info(0, fp)
        if rc != 0:
            raise OSError(f"malloc_info returned {rc}")
    finally:
        _libc.fclose(fp)
    try:
        return ctypes.string_at(buf, size.value).decode()
    finally:
        _libc.free(buf)


def parse_malloc_info(xml_str: str) -> dict:
    root = ET.fromstring(xml_str)
    main_system = main_free = 0
    nonmain_system = nonmain_free = 0
    for heap in root.findall("heap"):
        nr = int(heap.get("nr"))
        fast = next(t for t in heap.findall("total") if t.get("type") == "fast")
        rest = next(t for t in heap.findall("total") if t.get("type") == "rest")
        sys_cur = next(s for s in heap.findall("system") if s.get("type") == "current")
        system = int(sys_cur.get("size"))
        free = int(fast.get("size")) + int(rest.get("size"))
        if nr == 0:
            main_system += system
            main_free += free
        else:
            nonmain_system += system
            nonmain_free += free
    mmap_total = next((t for t in root.findall("total") if t.get("type") == "mmap"), None)
    return {
        "arenas": len(root.findall("heap")),
        "main_system": main_system,
        "main_free": main_free,
        "nonmain_system": nonmain_system,
        "nonmain_free": nonmain_free,
        "mmap_size": int(mmap_total.get("size")) if mmap_total is not None else 0,
        "mmap_count": int(mmap_total.get("count")) if mmap_total is not None else 0,
    }


def proc_status() -> dict:
    keys = {"VmRSS", "VmSize", "VmHWM", "VmPeak", "VmData"}
    out = {}
    with open("/proc/self/status") as f:
        for line in f:
            k = line.split(":", 1)[0]
            if k in keys:
                out[k] = int(line.split()[1])
    return out


def smaps_rollup() -> dict:
    out = {}
    with open("/proc/self/smaps_rollup") as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 2 and parts[0].endswith(":") and parts[-1] in ("kB", "KB"):
                out[parts[0][:-1]] = int(parts[1])
    return out


def snapshot() -> dict:
    return {
        "ru_maxrss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "status": proc_status(),
        "smaps": smaps_rollup(),
        "minfo": parse_malloc_info(malloc_info_xml()),
    }


def mib(kib: int) -> float:
    return kib / 1024


def b_mib(b: int) -> float:
    return b / 2**20


def render(label: str, snap: dict) -> None:
    s = snap["status"]
    sm = snap["smaps"]
    mi = snap["minfo"]
    print(f"[{label}]")
    print(
        f"  process: VmRSS={mib(s['VmRSS']):.1f} MiB  VmSize={mib(s['VmSize']):.1f} MiB  "
        f"VmHWM={mib(s['VmHWM']):.1f} MiB  VmPeak={mib(s['VmPeak']):.1f} MiB"
    )
    print(
        f"  smaps:   Rss={mib(sm.get('Rss', 0)):.1f} MiB  Anonymous={mib(sm.get('Anonymous', 0)):.1f} MiB  "
        f"Pss={mib(sm.get('Pss', 0)):.1f} MiB"
    )
    print(
        f"  malloc:  arenas={mi['arenas']}  "
        f"main: sys={b_mib(mi['main_system']):.1f} free={b_mib(mi['main_free']):.1f}  "
        f"nonmain: sys={b_mib(mi['nonmain_system']):.1f} free={b_mib(mi['nonmain_free']):.1f}  "
        f"mmap={b_mib(mi['mmap_size']):.1f} MiB ({mi['mmap_count']})"
    )


def render_diff(before: dict, after: dict) -> None:
    print("[diff after - before]")
    print(
        f"  process: ΔVmRSS={mib(after['status']['VmRSS'] - before['status']['VmRSS']):+.1f} MiB  "
        f"ΔVmSize={mib(after['status']['VmSize'] - before['status']['VmSize']):+.1f} MiB"
    )
    print(
        f"  smaps:   ΔRss={mib(after['smaps'].get('Rss', 0) - before['smaps'].get('Rss', 0)):+.1f} MiB  "
        f"ΔAnonymous={mib(after['smaps'].get('Anonymous', 0) - before['smaps'].get('Anonymous', 0)):+.1f} MiB"
    )
    mi_b, mi_a = before["minfo"], after["minfo"]
    print(
        f"  malloc:  Δmain_sys={b_mib(mi_a['main_system'] - mi_b['main_system']):+.1f} "
        f"Δmain_free={b_mib(mi_a['main_free'] - mi_b['main_free']):+.1f}  "
        f"Δnonmain_sys={b_mib(mi_a['nonmain_system'] - mi_b['nonmain_system']):+.1f} "
        f"Δnonmain_free={b_mib(mi_a['nonmain_free'] - mi_b['nonmain_free']):+.1f}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num-reads", type=int, default=20)
    parser.add_argument("--quiet", action="store_true", help="suppress per-read output")
    parser.add_argument("--with-trim", action="store_true", help="call malloc_trim(0) after reads and dump again")
    args = parser.parse_args()

    arctic = Arctic(LMDB_URI)
    library = arctic[LIBRARY]

    for i in range(args.num_reads):
        result = library.read(SYMBOL)
        rows = len(result.data)
        del result
        collected = gc.collect()
        rss_kib = proc_status()["VmRSS"]
        if not args.quiet:
            print(
                f"read {i + 1}/{args.num_reads}: {rows} rows  "
                f"gc.collect()={collected}  VmRSS={mib(rss_kib):.1f} MiB"
            )

    before = snapshot()
    render("after_reads", before)
    import shutil
    shutil.copyfile("/proc/self/maps", "/tmp/maps_snapshot.txt")
    if args.with_trim:
        rc = _libc.malloc_trim(0)
        after = snapshot()
        render(f"after_trim (malloc_trim returned {rc})", after)
        render_diff(before, after)


if __name__ == "__main__":
    main()
