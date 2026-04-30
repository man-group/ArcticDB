"""Check whether pages of a freed glibc malloc chunk remain resident.

Allocates an 800,000-byte buffer (size matching a per-column detachable buffer
in our read trace), then frees it and uses mincore() to inspect page residency
before and after malloc_trim(0). After the trim, peeks at the bytes that
remain on the two boundary pages to identify what is sharing them.
"""
import ctypes
import resource

PAGESIZE = resource.getpagesize()
SIZE = 800_000
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


def page_aligned_range(ptr: int, size: int) -> tuple[int, int]:
    start = ptr & ~(PAGESIZE - 1)
    end = (ptr + size + PAGESIZE - 1) & ~(PAGESIZE - 1)
    return start, end


def mincore(ptr: int, size: int) -> bytes:
    start, end = page_aligned_range(ptr, size)
    npages = (end - start) // PAGESIZE
    buf = ctypes.create_string_buffer(npages)
    rc = _libc.mincore(ctypes.c_void_p(start), end - start, buf)
    if rc != 0:
        raise OSError(f"mincore returned {rc}, errno={ctypes.get_errno()}")
    return bytes(buf.raw[:npages])


def report(label: str, ptr: int, size: int) -> bytes:
    vec = mincore(ptr, size)
    resident = sum(1 for b in vec if b & 1)
    total = len(vec)
    start, end = page_aligned_range(ptr, size)
    print(
        f"[{label}] ptr=0x{ptr:x}  span=0x{start:x}-0x{end:x}  "
        f"pages={total}  resident={resident}  ({resident*PAGESIZE/2**20:.2f} MiB)"
    )
    return vec


def page_resident_indices(vec: bytes) -> list[int]:
    return [i for i, b in enumerate(vec) if b & 1]


def dump_page(label: str, page_addr: int, ptr: int, size: int) -> None:
    """Print the layout of one page, distinguishing chunk-header / user / foreign bytes."""
    chunk_header = ptr - 16  # glibc malloc_chunk: prev_size + size, 16 bytes total on x86_64.
    user_start = ptr
    user_end = ptr + size
    chunk_end_footer = user_end  # next chunk's prev_size field lives here.
    page_end = page_addr + PAGESIZE
    print(f"  page 0x{page_addr:x}-0x{page_end:x}:")
    if chunk_header < page_end and chunk_header + 16 > page_addr:
        lo = max(chunk_header, page_addr) - page_addr
        hi = min(chunk_header + 16, page_end) - page_addr
        print(f"    [0x{lo:04x}-0x{hi:04x}] our chunk header (prev_size + size+flags)")
    if user_start < page_end and user_end > page_addr:
        lo = max(user_start, page_addr) - page_addr
        hi = min(user_end, page_end) - page_addr
        print(f"    [0x{lo:04x}-0x{hi:04x}] our user data (was memset 0x{SENTINEL:02x})")
    foreign_lo = 0
    foreign_hi = PAGESIZE
    if chunk_header >= page_addr and chunk_header < page_end:
        foreign_hi = chunk_header - page_addr
    if user_end > page_addr and user_end < page_end:
        foreign_lo = max(foreign_lo, user_end - page_addr)
    if foreign_lo < foreign_hi:
        chunk_at_page = (ctypes.c_uint8 * PAGESIZE).from_address(page_addr)
        bs = bytes(chunk_at_page)[foreign_lo:foreign_hi]
        all_zero = all(b == 0 for b in bs)
        all_sentinel = all(b == SENTINEL for b in bs)
        sample = bs[: min(64, len(bs))].hex(" ")
        tag = "(all zero)" if all_zero else "(all 0x55 = ours!?)" if all_sentinel else ""
        print(
            f"    [0x{foreign_lo:04x}-0x{foreign_hi:04x}] FOREIGN bytes "
            f"(adjacent chunk's data) {tag}"
        )
        print(f"        first {min(64, len(bs))} bytes: {sample}")


def main() -> None:
    # Warm the allocator so we are not the very first chunk in the arena.
    junk = _libc.malloc(SIZE)
    _libc.memset(junk, 0xAA, SIZE)
    _libc.free(junk)

    p = _libc.malloc(SIZE)
    _libc.memset(p, SENTINEL, SIZE)
    print(f"chunk user pointer = 0x{p:x}  chunk header = 0x{p-16:x}  pagesize = {PAGESIZE}")
    report("after malloc + memset", p, SIZE)

    _libc.free(ctypes.c_void_p(p))
    report("after free", p, SIZE)

    rc = _libc.malloc_trim(0)
    print(f"malloc_trim returned {rc}")
    vec = report("after malloc_trim(0)", p, SIZE)

    idxs = page_resident_indices(vec)
    span_start = (p) & ~(PAGESIZE - 1)
    print(f"\nresident page indices after trim: {idxs}")
    print(f"resident page addrs: {[hex(span_start + i*PAGESIZE) for i in idxs]}\n")
    print("contents of remaining resident pages:")
    for i in idxs:
        dump_page(f"page#{i}", span_start + i * PAGESIZE, p, SIZE)


if __name__ == "__main__":
    main()
