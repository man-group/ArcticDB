# Branch work log: gpetrov/ci_job_tuning

Matrix and job-shape changes. No production code.

- **Defender exclusions on Windows test jobs.** Real-time scanning of the thousands of small LMDB and temp files
  the tests create is worth 5-10x per test. Note this helps *test* jobs only: the same exclusions on the compile
  job were measured and made no difference, so that experiment was dropped.
- **enduser tests move into the hypothesis job.** Both were short jobs paying a full setup each.
- **The stateful hypothesis test is sharded.** It was a single ~20 minute test in a job that otherwise finished in
  three, so it set the length of the hypothesis job on every platform; parametrising over
  `HYPOTHESIS_STATEFUL_SHARDS` gives xdist something to distribute. Fewer examples per shard raises the share of
  overruns, so `data_too_large` is suppressed next to the `filter_too_much` that was already there. The group went
  from 34-42 min to 12-22.
- **Matrix trims.** `inferstr` and the version-cache variants differ only in Python-level behaviour, so one
  platform covers them. This is a coverage decision, not just a speed one, and should be agreed rather than waved
  through.
- **Benchmark smoke run** shortened on pull requests, where the job only needs to prove the benchmarks still run.
