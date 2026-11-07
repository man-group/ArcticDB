# Branch work log: fix-arrow-validity-bitmap-dealloc-size

## 2026-11-07: fix oversized Arrow validity bitmaps on end-truncated slices

- Bug: ASan `new-delete-type-mismatch` on Arrow output. A string column's validity bitmap was allocated at 176 bytes
  but sparrow freed it as 51 bytes. Reproducer: 1000-row segments, a string column with nulls,
  `lib.read(sym, date_range=(idx[300], idx[1400]), output_format="pyarrow")`.
- Root cause: `ColumnTruncation` bounds are frame rows, but the bitsets passed to
  `handle_truncation(util::BitSet&, ...)` are indexed from the first row of their slice. For the last slice of a
  date/row range (when it is not the first slice), `bv.resize(end_)` grew the bitset to the frame row (1401 bits)
  instead of shrinking it to 401. `create_validity_bitmap` then hands sparrow the block's row count, so sparrow frees
  `ceil(401 / 8) = 51` bytes of a 176-byte allocation. The values were right, as the extra bits are zero; only the size
  was wrong.
- Fix: `handle_truncation(util::BitSet&, truncate, first_row)` subtracts the slice's first frame row
  (`ColumnMapping::first_row_`) from both bounds. Updated the four callers (string variable-length and dictionary
  encoders, sparse bool handler, numeric decode path in `read_frame.cpp`).
- Test: `ArrowStringColumnRead.TruncatedLastSliceValidityBitmapSize` (all three string formats) checks the bitmap's
  byte size equals `bitset_packed_size_bytes` of the truncated row count. Failed with 176 != 51 before the fix.
- Verified end to end with an ASan build on gpetrov-1 (separate root `/root/bitmapfix`).
