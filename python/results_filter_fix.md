# Filter read-path fix — verification (read_filter.py)

Two-throttle admission (read window + residency budget). Symbol `f64_10m_100c_zeros` (10M rows, 100
float64 cols) over S3, empty filter `q["f_0"] == -1.0`, IO=96/CPU=64. Each row is a fresh process
(clean `ru_maxrss`), two runs each. Config via `RESIDENCY` (`VersionStore.NumProcessingUnitsLive`,
`0` = kill switch, `default` = unset) and `READ_WINDOW` (`VersionStore.SegmentReadWindow`).

| scenario | residency `E` | read window `W` | wall s | peak MiB |
|----------|---------------|-----------------|--------|----------|
| Flood — old K=0 behaviour | unbounded (0) | unbounded (1e6) | 3.99, 2.79 | ~7000 |
| Kill switch — new | unbounded (0) | default (2·io=192) | 2.09, 2.27 | ~7130 |
| Computed default | unset (default) | unset (default) | 2.24, 2.09 | ~7100 |
| Small read window | unbounded (0) | 16 | 3.23, 2.47 | ~1330 |
| Tight residency | 8 units | default (192) | 5.88, 6.02 | ~5000 |

## Reading

- **Regression fixed at the kill switch.** Reproducing the old K=0 flood (residency *and* window
  unbounded, so every read fires at once) gives 2.8–4.0 s and erratic. Bounding submission with the
  default read window drops it to a steady ~2.1 s. Same build, same script — a clean A/B.
- **Default is non-binding.** With both knobs unset the computed default residency sits above the
  read window, so the window governs and wall matches the kill switch (~2.1 s). The regression is
  fixed out-of-the-box.
- **The two throttles are decoupled and independent.** The read window alone bounds resident reads
  (`W=16` → ~1330 MiB at unbounded residency); the residency budget alone throttles for memory
  (`E=8` → 5.9 s, lower peak). Neither requires the other, confirming submission is no longer clocked
  by processing completion when memory is not binding.

The window being a tighter control on resident *segments* than the unit-granular residency budget is
the motivation for the byte-based residency follow-up noted in `read_memory_fix_plan.md`.
