# Branch Work Log: arrow-wide-table-perf

## Session 1: Initial Investigation
- Created branch off master
- Added `bench_wide_table_arrow.py` benchmark
- Ran debug build benchmarks showing 30-60x slowdown
- Identified string columns as the bottleneck
- Attempted string pool caching optimization in `encode_variable_length` — no improvement

## Session 2: Deeper Profiling (debug build)
- Added timing instrumentation to entire Arrow output pipeline
- Traced through `ArrowStringHandler::handle_type` → `encode_variable_length`
- Tried two-pass rewrite of `encode_variable_length` to eliminate intermediate vector
- Discovered the real bottleneck is NOT in `encode_variable_length` but in `segment_to_arrow_data`
- `segment_to_arrow_data` takes 96.6% of total time (4.0s of 4.15s) in debug build

## Session 3: Root Cause Found + Release Validation
- Added per-function, per-block timing inside `segment_to_arrow_data`
- Found `sparrow::string_array_impl` constructor takes 130ms/block in debug
- Root cause: `ensure_validity_bitmap` → `fill_initialize` does per-element construction (no inlining in debug)
- **Key discovery: ran release build — the 30-60x gap drops to 2-3x**
- The debug build hugely exaggerated the problem
- In release: `segment_to_arrow_data` takes ~300ms (sparrow schema/array creation) + `encode_variable_length` takes ~160ms wall
- Attempted null-pointer validity bitmap optimization — didn't help because the bitmap fill is fast in release; the overhead is in `make_arrow_schema`/`make_arrow_array`
- Attempted direct `arrow_proxy` construction — blocked because `array(arrow_proxy&&)` is private in sparrow
- Reverted all instrumentation code
- Updated analysis.md with corrected findings and optimization recommendations
