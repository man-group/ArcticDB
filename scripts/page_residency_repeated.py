"""Allocate-and-free a page-aligned 800K buffer many times in a row,
checking each time whether free() unmapped the pages (mincore returns
ENOMEM) or kept them resident.

If glibc's dynamic M_MMAP_THRESHOLD raises after a few cycles, later
free()s would leave the pages resident.
"""
import ctypes
import resource

PAGESIZE = resource.getpagesize()
RAW = 800_000
SIZE = (RAW + PAGESIZE - 1) & ~(PAGESIZE - 1)
N = 30

_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_libc.posix_memalign.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t, ctypes.c_size_t]
_libc.posix_memalign.restype = ctypes.c_int
_libc.free.argtypes = [ctypes.c_void_p]
_libc.free.restype = None
_libc.memset.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_size_t]
_libc.memset.restype = ctypes.c_void_p
_libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p]
_libc.mincore.restype = ctypes.c_int


def mincore_status(start: int, length: int) -> tuple[int, int]:
    npages = length // PAGESIZE
    buf = ctypes.create_string_buffer(npages)
    rc = _libc.mincore(ctypes.c_void_p(start), length, buf)
    if rc != 0:
        return -1, ctypes.get_errno()
    resident = sum(1 for b in buf.raw[:npages] if b & 1)
    return resident, 0


def main() -> None:
    for i in range(N):
        p = ctypes.c_void_p()
        rc = _libc.posix_memalign(ctypes.byref(p), PAGESIZE, SIZE)
        assert rc == 0
        addr = p.value
        _libc.memset(addr, 0x55, SIZE)
        before_res, _ = mincore_status(addr, SIZE)
        _libc.free(p)
        after_res, after_err = mincore_status(addr, SIZE)
        if after_res < 0:
            err_name = {12: "ENOMEM(unmapped)", 22: "EINVAL"}.get(after_err, str(after_err))
            print(f"alloc {i:2d}: ptr=0x{addr:x}  before_resident={before_res}/{SIZE//PAGESIZE}  "
                  f"after_free={err_name}")
        else:
            print(f"alloc {i:2d}: ptr=0x{addr:x}  before_resident={before_res}/{SIZE//PAGESIZE}  "
                  f"after_free_resident={after_res}/{SIZE//PAGESIZE}")


if __name__ == "__main__":
    main()
