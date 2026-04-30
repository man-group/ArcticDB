"""Group malloc_trace.log allocations by size and symbolize their backtraces."""
import argparse
import collections
import re
import subprocess
from pathlib import Path

ALLOC_RE = re.compile(r"^([MACRP]) size=(\d+) ptr=(\S+) tid=(\d+)$")
FRAME_RE = re.compile(r"^  (0x[0-9a-fA-F]+)$")


def parse_log(path: Path):
    events = []
    cur = None
    with path.open() as f:
        for line in f:
            m = ALLOC_RE.match(line.rstrip())
            if m:
                if cur is not None:
                    events.append(cur)
                cur = {"op": m.group(1), "size": int(m.group(2)), "ptr": m.group(3), "frames": []}
                continue
            m = FRAME_RE.match(line.rstrip())
            if m and cur is not None:
                cur["frames"].append(m.group(1))
                continue
            if line.startswith("F size="):
                if cur is not None:
                    events.append(cur)
                    cur = None
    if cur is not None:
        events.append(cur)
    return events


def get_proc_maps():
    maps = []
    with open("/proc/self/maps") as f:
        for line in f:
            pass
    # Caller passes a saved maps file from the traced process; fallback to our own won't help.
    return maps


def addr2line_for_lib(lib: str, addr_offsets: list, addr2line="addr2line") -> dict:
    out = {}
    if not addr_offsets:
        return out
    try:
        proc = subprocess.run(
            [addr2line, "-Cfpe", lib] + addr_offsets, capture_output=True, text=True, timeout=120
        )
    except FileNotFoundError:
        return out
    lines = proc.stdout.strip().splitlines()
    for addr, line in zip(addr_offsets, lines):
        out[addr] = line
    return out


def parse_maps_file(path: Path):
    """Return list of (start, end, offset, lib) tuples from a /proc/<pid>/maps snapshot."""
    out = []
    with path.open() as f:
        for line in f:
            parts = line.split()
            if len(parts) < 6:
                continue
            addr_range, perms, offset, _dev, _inode = parts[:5]
            lib = " ".join(parts[5:])
            if "x" not in perms:
                continue
            start_s, end_s = addr_range.split("-")
            out.append((int(start_s, 16), int(end_s, 16), int(offset, 16), lib))
    return out


def find_lib(addr: int, maps):
    for start, end, offset, lib in maps:
        if start <= addr < end:
            return lib, addr - start + offset
    return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log", type=Path)
    parser.add_argument("maps", type=Path, help="/proc/<pid>/maps snapshot from the traced run")
    parser.add_argument("--top", type=int, default=10, help="show top N sizes")
    parser.add_argument("--frames", type=int, default=12)
    args = parser.parse_args()

    events = [e for e in parse_log(args.log) if e["op"] != "F"]
    by_size = collections.defaultdict(list)
    for e in events:
        by_size[e["size"]].append(e)

    maps = parse_maps_file(args.maps)

    # Collect addresses per library to batch addr2line calls.
    addrs_per_lib = collections.defaultdict(set)
    for e in events:
        for fr in e["frames"][: args.frames]:
            addr = int(fr, 16)
            lib, off = find_lib(addr, maps)
            if lib:
                addrs_per_lib[lib].add(hex(off))

    sym_per_lib = {}
    for lib, addrs in addrs_per_lib.items():
        sym_per_lib[lib] = addr2line_for_lib(lib, sorted(addrs))

    def symbolize(addr_hex):
        addr = int(addr_hex, 16)
        lib, off = find_lib(addr, maps)
        if not lib:
            return f"{addr_hex} <unknown>"
        sym = sym_per_lib.get(lib, {}).get(hex(off))
        return f"{sym}  [{lib}]" if sym else f"{addr_hex} {lib}+{hex(off)}"

    # Show top sizes by total bytes.
    sizes_by_bytes = sorted(by_size.items(), key=lambda kv: -kv[0] * len(kv[1]))[: args.top]
    for size, es in sizes_by_bytes:
        n = len(es)
        print(f"\n=== size={size} count={n} total={n * size / 2**20:.2f} MiB ===")
        # Use the first event's frames as representative
        for fr in es[0]["frames"][: args.frames]:
            print(f"  {symbolize(fr)}")


if __name__ == "__main__":
    main()
