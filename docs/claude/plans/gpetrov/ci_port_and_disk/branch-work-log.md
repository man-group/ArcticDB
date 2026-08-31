# Branch work log: gpetrov/ci_port_and_disk

Sits on the test-job stack (`gpetrov/ci_stress_nightly_only`). Two changes found while looking for what sets the
wall now that Windows and compile are fixed: the run's critical path is the Linux `integration` job (27.1 min) and
the merged `{hypothesis,...}` job (29.3 min), both of which start promptly and simply run long.

## The 20 second sleep

`get_ephemeral_port` held the probed socket open for 20s (30 under conda) on the **success** path, as a way of
letting a second caller racing for the same number discover the collision. Every moto S3, GCP, azurite and mongod
startup paid it.

Evidence it dominates, from the junit XML of `3.9 Linux / integration-DefaultCache` in run 33316030365 - the
duration histogram has nothing at all in the 19-20.5s band and a cluster immediately above it:

```
  15-19 s   n=  4    1.08 min
19-20.5 s   n=  0    0.00 min   <- nothing
20.5-23 s   n= 33   12.00 min
```

55 tests at >=20s, **18.3 of the job's 81.6 CPU-min**. `test_read_path_with_dot`, which writes one symbol and reads
it back, took 21.7s. The Linux `unit` job spends ~8.3 CPU-min the same way, a quarter of that suite. On macOS the
same test takes 92s and 21 such tests are 57% of the job.

Replaced with a disjoint block of 100 ports per (seed, xdist worker), walked from a pid-derived offset. A
collision inside a run is now impossible by construction rather than unlikely, and jobs are on separate runners.
The bind was only ever a probe - the caller binds for real afterwards, so the race the sleep was guarding against
existed either way - and the retries in `MotoS3StorageFixtureFactory._safe_enter` and `start_with_retry` still
cover something outside the run taking the port in between.

Verified locally: 15 calls across 5 seeds in 0.001s, all distinct; distinct across simulated `gw0`-`gw3`;
`tests/integration/arcticdb/test_s3.py` goes from not finishing within 120s to **10.4s**.

## The disk cleanup nobody needed

`Free Disk Space (Container)` deleted ~20 GB from every containerised Linux test job. From the job log:

```
14:19:07  overlay 146G  65G  82G  45% /   <- before the cleanup
14:20:53  overlay 146G  45G 101G  31% /   <- after 105s of rm -rf
14:44:59  overlay 146G  46G 100G  32% /   <- after the whole test run
```

82 GB free before it ran; the entire job consumed 1 GB. Same picture on `unit` and `stress`. It cost 48-105s on
each of 44 Linux test jobs, ~66 job-min per run. Removed from the test job; the compile jobs keep their own.

The `Free Disk Space (Base Runner)` variant beside it was skipped on all 44 jobs, so it is left alone.

## Not done here

- **Shard the integration suite 2x** (~-8.7 wall min, +46 job-min). Only worth it *after* the port fix: session
  factories start once per worker per job, so a second shard duplicates ~40 startups, which used to mean +14
  CPU-min of new sleep.
- **Run the slowest tests first.** ~1.2 min of tail on integration - at 14:43:21 the run is at 99% with the
  `test_compact_data` variants left, and three workers idle from 14:44:22 to 14:44:58. `pytest-split` is already
  installed by the workflow and never used. `--dist worksteal` cannot fix this: it rebalances queues but cannot
  split a single 60s test.
- **The hypothesis job's real pathology**, which is separate work: 96% of that job is `test_stateful`, and 96% of
  *that* is the nine `@invariant()` methods, whose per-step cost grows ~4x across the run because several iterate
  every version or snapshot and do real storage reads. Cost is quadratic in `stateful_step_count`, which is set to
  100 against hypothesis's default of 50. Gating the four heaviest with `@precondition` is the fix.
- Also measured, for the record: `sanitizers` and `enduser` are **100% skipped** in that job (0.00s, 41 skips), and
  `compat311` is not inherently slower - across four runs of the same variant the non-stateful part is flat at
  20.6-24.5 CPU-min and all the variance is the hypothesis seed.
