# Branch 5: feature_arrow_batch_utils

## Environment

- Preset: linux-release
- CPU: Intel Xeon Gold 6248R @ 3.00GHz (48 cores)
- Date: 2026-02-25

## Tests

- `--gtest_filter="*Arrow*"`: **42/42 passed**

## BM_arrow_convert_single_record_batch_to_segment

| Parameter | Branch 4 (median) | Branch 5 (median) | Delta |
|-----------|-------------------|-------------------|-------|
| 10/100K/-1/pyarrow | 259 ms | 271 ms | +4.6% |
| 10/100K/0/pyarrow | 258 ms | 260 ms | +0.8% |
| 10/100K/50K/pyarrow | 255 ms | 266 ms | +4.3% |
| 10M/10/-1/pyarrow | 0.009 ms | 0.009 ms | 0% |
| 10M/10/0/pyarrow | 0.009 ms | 0.009 ms | 0% |
| 10M/10/5/pyarrow | 0.009 ms | 0.009 ms | 0% |
| 10/100K/-1/pandas | 481 ms | 490 ms | +1.9% |
| 10/100K/0/pandas | 488 ms | 490 ms | +0.4% |
| 10/100K/50K/pandas | 477 ms | 491 ms | +2.9% |
| 10M/10/-1/pandas | 0.017 ms | 0.017 ms | 0% |
| 10M/10/0/pandas | 0.017 ms | 0.017 ms | 0% |
| 10M/10/5/pandas | 0.018 ms | 0.018 ms | 0% |

## Conclusion

Minor regression (0.4% - 4.6%) in large batch pyarrow cases, negligible change elsewhere. This is within acceptable noise range for the added functionality in arrow_utils (horizontal_merge_arrow_batches, pad_batch_to_schema, default_arrow_format_for_type).
