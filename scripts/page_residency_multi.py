"""Allocate the per-read mix of detachable buffers, free them all, then
inspect page residency over the whole coalesced span. Reproduces the
six 800k + two 400k pattern observed in the read trace.
"""
import ctypes
import resource

PAGESIZE = resource.getpagesize()
SIZES = [800_000] * 6 + [400_000] * 2
SENTINEL = 0x55

_libc = ctypes.CDLL("libc.so.6")
_libc.malloc.argtypes = [ctypes.c_size_t]
_libc.malloc.restype = ctypes.c_void_p
_libc.free.argtypes = [ctypes.c_void_p]
_libc.free.restype = None
_libc.memset.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_size_t]
_libc.memset.restype = ctypes.c_void_p
_libc.malloc_trim.argtypes = [ctypes.c_size_t]
_libc.malloc_trim.restype = ctypes.c_int
_libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p]
_libc.mincore.restype = ctypes.c_int


def mincore_span(start: int, end: int) -> bytes:
    page_start = start & ~(PAGESIZE - 1)
    page_end = (end + PAGESIZE - 1) & ~(PAGESIZE - 1)
    npages = (page_end - page_start) // PAGESIZE
    buf = ctypes.create_string_buffer(npages)
    rc = _libc.mincore(ctypes.c_void_p(page_start), page_end - page_start, buf)
    if rc != 0:
        raise OSError(f"mincore returned {rc}, errno={ctypes.get_errno()}")
    return bytes(buf.raw[:npages])


def report(label: str, lo: int, hi: int) -> None:
    vec = mincore_span(lo, hi)
    resident = sum(1 for b in vec if b & 1)
    total = len(vec)
    page_lo = lo & ~(PAGESIZE - 1)
    page_hi = (hi + PAGESIZE - 1) & ~(PAGESIZE - 1)
    print(
        f"[{label}] span=0x{page_lo:x}-0x{page_hi:x}  "
        f"pages={total}  resident={resident}  ({resident*PAGESIZE/2**20:.2f} MiB)"
    )


def main() -> None:
    # Warm allocator.
    junk = _libc.malloc(800_000)
    _libc.free(junk)

    ptrs = []
    for s in SIZES:
        p = _libc.malloc(s)
        _libc.memset(p, SENTINEL, s)
        ptrs.append((p, s))

    lo = min(p for p, _ in ptrs)
    hi = max(p + s for p, s in ptrs)
    print(f"allocations: {len(ptrs)}  total bytes={sum(s for _, s in ptrs):,}")
    print(f"address range: 0x{lo:x} - 0x{hi:x}  ({(hi - lo)/2**20:.2f} MiB)")
    report("after all allocs", lo, hi)

    for p, _ in ptrs:
        _libc.free(ctypes.c_void_p(p))
    report("after all frees", lo, hi)

    rc = _libc.malloc_trim(0)
    print(f"malloc_trim returned {rc}")
    report("after malloc_trim(0)", lo, hi)


if __name__ == "__main__":
    main()
