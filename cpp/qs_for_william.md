### Arrow data lifecycle
- When reading segments we fill in data in Arrow arrays essentially returning a single record batch?
- Who is responsible for freeing the allocated memory? Is it ref-counted in python?

### Arrow data structures
- Why does RecordBatchData have a single array? My understanding is that a RecordBatch is a collection of equally sized arrays.

- Why do we need `ExtraBufferType` in `ChunkedBuffer`?

### Internal Segments
- Type handler responsibility? - Mostly for strings?
- Difference between `AllocationType`s. Meaning of detachable in chunked buffer? How is detachable used?



Streaming data in batches

Date range filter allocates more memory than it needs, filter in C++ not python

Interesting point about fragmentation:
- we build row groups (record batches) based on segments. Too fragmented symbols might be problematic for post-processing