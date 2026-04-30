"""Same as page_residency.py but with a page-aligned pointer AND a
page-multiple size. Tests the hypothesis that aligning the allocation
would let free() (or malloc_trim) release every page.
"""
import ctypes
import resource

PAGESIZE = resource.getpagesize()
RAW = 800_000
SIZE = (RAW + PAGESIZE - 1) & ~(PAGESIZE - 1)  # round up to page multiple

_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_libc.malloc.argtypes = [ctypes.c_size_t]
_libc.malloc.restype = ctypes.c_void_p
_libc.posix_memalign.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t, ctypes.c_size_t]
_libc.posix_memalign.restype = ctypes.c_int
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
    page_lo = lo & ~(PAGESIZE - 1)
    page_hi = (hi + PAGESIZE - 1) & ~(PAGESIZE - 1)
    print(
        f"[{label}] span=0x{page_lo:x}-0x{page_hi:x}  "
        f"pages={len(vec)}  resident={resident}  ({resident*PAGESIZE/2**20:.2f} MiB)"
    )


def main() -> None:
    junk = _libc.malloc(SIZE)
    _libc.free(junk)

    p = ctypes.c_void_p()
    rc = _libc.posix_memalign(ctypes.byref(p), PAGESIZE, SIZE)
    assert rc == 0
    addr = p.value
    _libc.memset(addr, 0x55, SIZE)
    print(
        f"raw_size={RAW}  rounded_size={SIZE}  ptr=0x{addr:x}  "
        f"page_aligned={(addr & (PAGESIZE-1)) == 0}"
    )
    report("after posix_memalign + memset", addr, addr + SIZE)

    _libc.free(p)
    report("after free", addr, addr + SIZE)

    rc = _libc.malloc_trim(0)
    print(f"malloc_trim returned {rc}")
    report("after malloc_trim(0)", addr, addr + SIZE)


if __name__ == "__main__":
    main()
