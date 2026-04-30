"""Test the dynamic M_MMAP_THRESHOLD effect with mixed allocation sizes.

Allocates a sequence of page-aligned buffers of mixed sizes (matching
the per-read pattern: six 800K + two 400K), frees each, and uses mincore
to detect whether the pages were unmapped or kept resident.

If the dynamic threshold raise occurs after an 800K mmap'd chunk is
freed, subsequent 400K allocations fall under the threshold and go via
the arena instead of mmap.
"""
import ctypes
import resource

PAGESIZE = resource.getpagesize()
SIZES = [800_000, 800_000, 800_000, 800_000, 800_000, 800_000, 400_000, 400_000]
SENTINEL = 0x55

_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_libc.posix_memalign.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t, ctypes.c_size_t]
_libc.posix_memalign.restype = ctypes.c_int
_libc.free.argtypes = [ctypes.c_void_p]
_libc.free.restype = None
_libc.memset.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_size_t]
_libc.memset.restype = ctypes.c_void_p
_libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p]
_libc.mincore.restype = ctypes.c_int


def round_up(n: int) -> int:
    return (n + PAGESIZE - 1) & ~(PAGESIZE - 1)


def mincore_status(start: int, length: int) -> tuple[int, int]:
    npages = length // PAGESIZE
    buf = ctypes.create_string_buffer(npages)
    rc = _libc.mincore(ctypes.c_void_p(start), length, buf)
    if rc != 0:
        return -1, ctypes.get_errno()
    resident = sum(1 for b in buf.raw[:npages] if b & 1)
    return resident, 0


def alloc_aligned(size: int) -> int:
    p = ctypes.c_void_p()
    rc = _libc.posix_memalign(ctypes.byref(p), PAGESIZE, round_up(size))
    assert rc == 0
    _libc.memset(p, SENTINEL, size)
    return p.value


def main() -> None:
    print(f"sequence: {SIZES}")
    print()
    print(f"{'i':>2}  {'size':>8}  {'rounded':>8}  {'ptr':>16}  {'after_free':>16}")
    for i, sz in enumerate(SIZES):
        addr = alloc_aligned(sz)
        rounded = round_up(sz)
        before, _ = mincore_status(addr, rounded)
        _libc.free(ctypes.c_void_p(addr))
        after, err = mincore_status(addr, rounded)
        if after < 0:
            tag = "ENOMEM (unmapped)" if err == 12 else f"errno={err}"
        else:
            tag = f"{after}/{rounded//PAGESIZE} resident"
        print(f"{i:>2}  {sz:>8}  {rounded:>8}  0x{addr:>14x}  {tag:>16}")


if __name__ == "__main__":
    main()
