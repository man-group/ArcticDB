# Branch work log: gpetrov/ci_speedup

## Baseline (PR run 32735025613, 2026-08-24)
- 241 jobs, 4h12m wall-clock, ~2190 job-minutes.
- C++ compile is already fast (sccache 100% hit on Linux; 7-15 min wheel builds).
- Critical path is Windows pytest jobs: 50-107 min each, 1375 Windows test job-minutes vs 512 Linux.
- Same 19,490 `unit` tests: 36 CPU-min on Linux, 171-387 CPU-min on Windows (5-10x per test).

## 2026-08-28
- Added a Windows Defender exclusion / realtime-monitoring-off step before the Python tests
  (`build_steps.yml`, python_tests job). Experiment 1: measure Windows unit/integration job times
  against the baseline above.
- Next candidates: shard Windows unit/integration with pytest-split (already installed, unused);
  reduce `ci_windows` hypothesis max_examples; trim Windows Python matrix on PRs.
