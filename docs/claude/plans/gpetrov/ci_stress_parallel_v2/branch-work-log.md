# Branch work log: gpetrov/ci_stress_parallel_v2

The stress suite ran with `-n 0` everywhere because a few of its tests peak at several gigabytes and two at once
would evict the runner. `--dist loadgroup` plus an `xdist_group` on the heavy tests keeps exactly those serialised
against each other while the rest run four-up, so Linux stress went from 22.8 to 16-17 min.

`--dist loadgroup` is required: `worksteal`, which the other jobs use, ignores `xdist_group` entirely, so picking
it would put the multi-gigabyte tests on different workers at the same time - the thing the grouping exists to
prevent.

Also dropped stress from the compat311 matrix, where it duplicates the main run, and excluded it from the Serial
admission limit, which it no longer needs now that it is not the serial job.
