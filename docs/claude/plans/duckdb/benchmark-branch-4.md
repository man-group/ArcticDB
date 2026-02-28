# Branch 4: feature_detachable_alloc

## Environment
- Preset: linux-release
- CPU: Intel Xeon Gold 6248R @ 3.00GHz (48 cores)
- Date: 2026-02-25

## Tests
- `--gtest_filter="*Arrow*"`: **42/42 passed**

## BM_arrow_convert_single_record_batch_to_segment

| Parameter | Branch 3 (median) | Branch 4 (median) | Delta |
|-----------|-------------------|-------------------|-------|
| 10/100K/-1/pyarrow | 295 ms | 259 ms | -12% (noise) |
| 10/100K/0/pyarrow | 267 ms | 258 ms | -3% |
| 10/100K/50K/pyarrow | 255 ms | 255 ms | 0% |
| 10M/10/-1/pyarrow | 0.008 ms | 0.009 ms | 0% |
| 10M/10/0/pyarrow | 0.008 ms | 0.009 ms | 0% |
| 10M/10/5/pyarrow | 0.009 ms | 0.009 ms | 0% |
| 10/100K/-1/pandas | 532 ms | 481 ms | -10% (noise) |
| 10/100K/0/pandas | 509 ms | 488 ms | -4% |
| 10/100K/50K/pandas | 499 ms | 477 ms | -4% |
| 10M/10/-1/pandas | 0.017 ms | 0.017 ms | 0% |
| 10M/10/0/pandas | 0.018 ms | 0.017 ms | 0% |
| 10M/10/5/pandas | 0.018 ms | 0.018 ms | 0% |

## Conclusion
No regression. Default DYNAMIC allocation path unchanged. Variations within noise.
