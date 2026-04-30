"""Same mixed-size test, but pin M_MMAP_THRESHOLD via mallopt to disable
the dynamic raise. All page-aligned allocations should now reliably go
via mmap and unmap on free.
"""
import ctypes
import resource

PAGESIZE = resource.getpagesize()
SIZES = [800_000, 800_000, 800_000, 800_000, 800_000, 800_000, 400_000, 400_000]

M_MMAP_THRESHOLD = -3  # from <malloc.h>

_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_libc.mallopt.argtypes = [ctypes.c_int, ctypes.c_int]
_libc.mallopt.restype = ctypes.c_int
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


def main() -> None:
    rc = _libc.mallopt(M_MMAP_THRESHOLD, 128 * 1024)
    print(f"mallopt(M_MMAP_THRESHOLD, 128*1024) returned {rc}")
    print()

    for i, sz in enumerate(SIZES):
        rounded = round_up(sz)
        p = ctypes.c_void_p()
        _libc.posix_memalign(ctypes.byref(p), PAGESIZE, rounded)
        addr = p.value
        _libc.memset(addr, 0x55, sz)
        _libc.free(p)
        after, err = mincore_status(addr, rounded)
        if after < 0:
            tag = "ENOMEM (unmapped)" if err == 12 else f"errno={err}"
        else:
            tag = f"{after}/{rounded//PAGESIZE} resident"
        print(f"i={i}  size={sz:>8}  ptr=0x{addr:x}  after_free: {tag}")


if __name__ == "__main__":
    main()
