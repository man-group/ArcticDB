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
- **Restricting `test_stateful` to fewer matrix variants** (~-5 wall min, ~-190 job-min) - a real coverage cut, so
  it should be argued on its own rather than folded in here.
- Also measured, for the record: `sanitizers` and `enduser` are **100% skipped** in that job (0.00s, 41 skips), and
  `compat311` is not inherently slower - across four runs of the same variant the non-stateful part is flat at
  20.6-24.5 CPU-min and all the variance is the hypothesis seed.

## The stateful hypothesis test (added after the first two changes)

96% of the merged `{hypothesis,...}` job is `test_stateful`, and 96% of *that* was the nine `@invariant()` methods
rather than the code under test. `test_list_versions_all_and_read` dominated: `read_metadata` + `read` +
`assert_frame_equal` for every version of every symbol, after every step. Since a step changes one symbol, at step
50 with ~40 live versions 39 of those 40 reads re-proved what the previous step had already proved. Measured
per-step invariant cost grew 37ms (step 10) -> 147ms (step 90).

The rules now mark the `(symbol, version)` pairs they change and the invariant reads back only those. The full
`list_versions()` comparison still runs every step, so the deleted flags, snapshot sets and the "failed to find
these" check are unchanged - only the redundant re-reads go.

The subtle case, and the reason this needs a guard rather than eyeballing: `delete_snapshot` changes no version's
data, but un-pinning can make a tombstoned version deletable, which changes whether it reads back at all. So it
has to mark every version the snapshot held.

Two guards against a gap in the marking silently un-testing a version:
- `teardown()` reads everything back once per example, so a gap still fails - one example later rather than one
  step later. It returns early if an exception is already propagating, so it cannot mask a rule's own failure.
- `test_dirty_versions_cover_every_model_change` fingerprints the model around each rule and asserts every change
  was marked. Verified to bite: deleting the `delete_snapshot` marking fails it with the pinned version.

Measured locally, 8 examples of one shard on an otherwise idle machine: **171.4s -> 57.2s (3.0x)**.

### Why `stateful_step_count` was left at 100

Cost per example is quadratic in it and hypothesis's own default is 50, so halving it looked like a free 4x. It
is not: measured on top of the change above, 57.2s -> 46.5s, only **1.23x**, because scoping the reads had already
removed most of the quadratic term. Paying for 19% by halving the length of the operation sequences explored is a
bad trade in a test whose whole purpose is to find version-map and snapshot interactions that need a sequence of
writes, prunes, snapshots and deletes to line up. Left at 100.

A caution on measuring this locally: the first "after" reading was 70s rather than 46.5s because another process
was busy on the same machine. Both halves of an A/B here have to be taken on an idle box.

## The dedup flake, and the dead feature behind it

`test_string_dedup_basic` failed in four separate runs over two days and four times in 25 failed master runs over
60 days, always as `assert <with_dedup> <= <without_dedup>` with margins from 0.5% to 18%.

`getsize()` keeps ids, not references. With Arrow-backed strings (every observed failure was an `-inferstr` job)
each loop iteration materialises a temporary that is freed before the next, so its id is reused and the function
sums a handful of arbitrary objects rather than 4000. Measuring **the same frame** five times running returns
1582, 888, 888, 888, 888 - a 78% swing with no change in data. The assertion was reporting the sign of allocator
noise.

Replaced with a count of distinct string objects, holding the values alive so ids stay distinct, guarded on
object dtype. 40 repeats under `-inferstr` show no variance.

### `optimise_string_memory` has been a no-op since 2024-07-02

The counts come out exactly equal under object dtype because the feature does nothing. Verified directly:

- `DecodePathData::set_optimize_for_memory()` (`decode_path_data.hpp:45`), the switch read at
  `python_strings.cpp:194` to choose global deduplication, **has no callers anywhere in the repo**.
- `ReadOptions::optimise_string_memory_` (`read_options.hpp:32`) is written from Python and **never read** in C++;
  there is no getter.
- `7df641fba` "Release GIL on read" (2024-07-02) dropped the wiring: before it `read_frame.cpp` had the
  `unique_string_map_` machinery, after it there are zero references.

So a documented `read()` kwarg has silently done nothing for two years - users get one Python string object per
row where they asked for one per distinct value. Recorded as `test_string_dedup_shares_string_objects`,
`xfail(strict=True)`, which flips to a pass when the wiring returns.

Anyone restoring it should first fix what looks like a refcount bug in the now-unreachable `assign_strings_shared`
(`python_strings.cpp:135-183`): `get_allocated_strings` pre-populates `allocated` for strings an earlier column
created, and the main loop then skips those offsets without `inc_ref` while `write_strings_to_destination` still
stores `count` more pointers. That is a code reading of dead code, not a runtime observation.

## Windows jobs that hit the 120-minute step limit

Two `3.11 Windows / unit` jobs burned the full step limit. Neither hung: 20,209 of 20,212 tests had completed,
the last one passed 13 seconds before the kill, and summed per-worker gaps show the workers ~99% busy throughout.

The cause is the runner. The same job takes **46 min or 123 min** depending on which Windows VM it lands on, and
the discriminator is the temp disk size reported by the `Disk usage` step:

| D: size | duration |
|---|---|
| 220G | 123 min, 123 min (both timed out) |
| 150G | 46 min |

Both report `cores: 4, RAM: 16378MB` and the same runner image. The slowdown is uniform across unrelated test
families (`test_realistic` 405s -> 1190s, `recursive_normalizers` 433s -> 1063s), so it is not a test regression.
It affects roughly 5% of Windows `unit` jobs, and it is the same effect behind the Windows *compile* jobs that
occasionally take 2-3x their usual time - `Install Required MSVC`, which only downloads and installs, doubles too.

Three diagnostic gaps this exposed, all now fixed:
- A step timeout SIGKILLs the step, so no junit XML, no `--durations`, no crash dumps. Now SIGINT at 100m first.
- `--durations` was not enabled, so "everything is uniformly slower" was invisible in the log.
- `ARCTICDB_FAULTHANDLER_DIR` is `$TEST_OUTPUT_DIR/faulthandler` but the artifact glob was `$TEST_OUTPUT_DIR/*test*`,
  so the watchdog's output could never reach CI. (The watchdog itself behaved correctly here: it is a per-test
  budget of 3300s and the slowest test was 1190s. Wrong tool for an aggregate overrun.)
