# Wide-Table Performance Analysis: Arrow Output Path

## Problem

`lib.read(symbol, output_format='pyarrow')` is slower than `lib.read(symbol)` (pandas output) on wide tables with string columns. This impacts `lib.sql()` and `lib.duckdb()` which stream data through the lazy Arrow record batch iterator.

## CRITICAL FINDING: Debug vs Release

**The original 30-60x slowdown was a debug build artifact.** In release builds, the gap is **2-3x**.

| Build | 50 cols (20 str + 30 num, 1M rows) | Ratio |
|---|---|---|
| Debug: PyArrow | 35.0s | 44x |
| Debug: Pandas | 0.79s | 1x |
| **Release: PyArrow** | **0.475s** | **2.3x** |
| **Release: Pandas** | **0.203s** | **1x** |

The debug build exaggeration was caused by `sparrow::string_array_impl` constructor, which calls `fill_initialize` to allocate and fill validity bitmaps — a loop that takes 130ms/block in debug (no inlining) but <1ms in release.

## Release Build Benchmark Results

| Columns | PyArrow | Pandas | Ratio |
|---|---|---|---|
| 9 cols (3 str, 6 num, 1M rows) | 0.049s | 0.036s | **1.4x** |
| 50 cols (20 str, 30 num, 1M rows) | 0.475s | 0.203s | **2.3x** |
| 100 cols (40 str, 60 num, 1M rows) | 0.942s | 0.285s | **3.3x** |
| 200 cols (85 str, 115 num, 500K rows) | 1.041s | 0.388s | **2.7x** |
| 385 cols (170 str, 215 num, 250K rows) | 1.361s | 0.565s | **2.4x** |

Key observations:
- **Column selection reads remain fast** — reading 3 cols from 385-col table takes 5-6ms regardless of output format
- **Filter pushdown is effective** — `filter + 3 cols` takes ~10ms
- **The gap scales sub-linearly** — 9 cols is 1.4x, 385 cols is 2.4x
- **Pure numeric data has minimal gap** — ~2x overhead for Arrow format conversion

## Detailed Release Profiling (50 cols, 1M rows)

### Arrow path breakdown (total 0.533s)

| Step | Time | Notes |
|---|---|---|
| C++ pipeline: storage read + parallel decode | 236ms | Includes encode_variable_length |
| C++ `segment_to_arrow_data` | 297ms | Serial: builds sparrow arrays from SegmentInMemory |
| Python: extract_record_batches + import | 3ms | Arrow C Data Interface export |
| Python: Table.from_batches + denormalize | 2ms | Negligible |

### Pandas path breakdown (total 0.141s)

| Step | Time | Notes |
|---|---|---|
| C++ pipeline: storage read + parallel decode | 137ms | Includes DynamicStringReducer |
| C++ create_python_read_result | <1ms | No Arrow conversion needed |
| Python: denormalize | 4ms | Negligible |

### Where the 2.4x gap comes from

1. **Pipeline decode: 236ms vs 137ms (1.7x)**
   - Arrow's `encode_variable_length` copies string data from pool → contiguous buffer (O(total_string_bytes))
   - Pandas' `DynamicStringReducer` writes Python object pointers (O(N) pointer writes)
   - Arrow path does ~3x more memory work per string column

2. **`segment_to_arrow_data`: 297ms vs 0ms**
   - Arrow path creates `sparrow::string_array_impl` per block per string column
   - ~1.5ms per 100K-row string block in release (sparrow `make_arrow_schema` + `make_arrow_array` overhead)
   - 20 str cols × 10 blocks = 200 string arrays × 1.5ms ≈ 300ms
   - Pandas path has no equivalent step

## Optimization Opportunities

### 1. Reduce sparrow overhead in `segment_to_arrow_data` (~297ms → ~50ms)

**Status**: Blocked by sparrow API limitations

The `sparrow::string_array_impl` constructor allocates and fills an all-true validity bitmap even when there are no nulls. The `sparrow::array(arrow_proxy&&)` constructor is private, preventing direct Arrow C struct construction.

Options:
- **PR to sparrow**: Add `bool nullable` constructor overload for buffer-based `string_array_impl`, calling `create_proxy_impl` with `std::nullopt` bitmap
- **Bypass sparrow**: Build `ArrowArray`/`ArrowSchema` directly and pass to Python without going through sparrow. Would require restructuring `ArrowOutputFrame`
- **Reduce block count**: Increasing segment row count from 100K to 1M would reduce blocks from 10→1, cutting overhead ~10x. But this is a user-facing storage config change

### 2. Parallelize `segment_to_arrow_data` (~297ms → ~30ms)

Currently serial — iterates columns one at a time. Could be parallelized with `folly::window` or thread pool since each column's arrays are independent. The `record_batch.add_column` step would remain serial but is already fast (0.03ms total).

### 3. Optimize `encode_variable_length` (~160ms wall → ~80ms)

- **Two-pass approach**: Pass 1 computes offsets and total string bytes. Pass 2 copies strings. Avoids the intermediate `vector<string_view>` allocation. Tested but showed negligible improvement in release (the vector alloc is well-optimized)
- **SIMD string copy**: The inner loop copies strings one at a time. Could benefit from batch memcpy for same-length strings
- **String pool caching**: Cache `string_pool->get_const_view()` per unique offset. Already investigated — no measurable improvement because the string pool lookup is already O(1)

### 4. Change default string format to CATEGORICAL

**Status**: Most impactful user-facing change

CATEGORICAL (dictionary encoding) is inherently more efficient for typical string data:
- Only copies unique strings once (O(U) vs O(N) string bytes)
- Stores int32 keys per row (4 bytes vs avg 16+ bytes per string)
- DuckDB natively supports dictionary-encoded columns
- PyArrow supports dictionary type natively

Previous benchmarks showed CATEGORICAL is 10x faster than LARGE_STRING for 100 unique strings.

This would require changing `ArrowOutputConfig::default_string_format_` from `LARGE_STRING` to `CATEGORICAL`, or making the choice adaptive based on cardinality.

## Files

| File | Purpose |
|---|---|
| `python/benchmarks/non_asv/duckdb/bench_wide_table_arrow.py` | Benchmark script |
| `cpp/arcticdb/arrow/arrow_handlers.cpp` | String encoding: `encode_variable_length`, `encode_dictionary` |
| `cpp/arcticdb/arrow/arrow_utils.cpp` | `segment_to_arrow_data`: SegmentInMemory → sparrow record batches |
| `cpp/arcticdb/arrow/arrow_output_frame.cpp` | ArrowOutputFrame: sparrow → Arrow C Data Interface export |
| `cpp/arcticdb/arrow/arrow_output_options.hpp` | `ArrowOutputConfig`, string format enum |
| `cpp/arcticdb/pipeline/pipeline_utils.hpp` | `create_python_read_result`: calls `segment_to_arrow_data` |
| `scratch/profile_timing*.py` | Profiling scripts (not committed) |
