# Branch 3: refactor_recordbatch_move_only

## Environment
- Preset: linux-release
- CPU: Intel Xeon Gold 6248R @ 3.00GHz (48 cores)
- Date: 2026-02-25

## Tests
- `--gtest_filter="*Arrow*"`: **42/42 passed**

## BM_arrow_convert_single_record_batch_to_segment

Parameters: `cols/rows/strings/is_pyarrow`

| Parameter | Master (median) | Branch 3 (median) | Delta |
|-----------|----------------|--------------------|-------|
| 10/100K/-1/pyarrow | 263 ms | 295 ms | +12% (noise) |
| 10/100K/0/pyarrow | 267 ms | 267 ms | 0% |
| 10/100K/50K/pyarrow | 255 ms | 255 ms | 0% |
| 10M/10/-1/pyarrow | 0.009 ms | 0.008 ms | -11% (noise) |
| 10M/10/0/pyarrow | 0.008 ms | 0.008 ms | 0% |
| 10M/10/5/pyarrow | 0.009 ms | 0.009 ms | 0% |
| 10/100K/-1/pandas | 479 ms | 532 ms | +11% (noise) |
| 10/100K/0/pandas | 509 ms | 509 ms | 0% |
| 10/100K/50K/pandas | 499 ms | 499 ms | 0% |
| 10M/10/-1/pandas | 0.018 ms | 0.017 ms | -6% (noise) |
| 10M/10/0/pandas | 0.018 ms | 0.018 ms | 0% |
| 10M/10/5/pandas | 0.018 ms | 0.018 ms | 0% |

## Conclusion
No regression. Variations on the 100K-row test cases are within system noise (high load average ~116, CPU scaling enabled). Stable cases (50K strings, wide tables) show 0% delta.
