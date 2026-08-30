# Branch work log: gpetrov/ci_stress_nightly_only

Runs the stress suite on master pushes and the nightly, not on pull requests.

This trades coverage for time, so here is the evidence. Over 90 days and 176 master runs of `build.yml`
(`ci_job_failure_stats.py`, job-level conclusions from the Actions API):

| job type | failed jobs |
|---|---:|
| unit | 63 |
| compile | 47 |
| integration | 46 |
| **stress** (2,465 jobs) | **1** |

The single stress failure was a runner shutdown, not a test. So the suite has not caught a regression on master
in three months while costing a job slot on every pull request; master and the nightly keep it.

The reviewer's call is whether "one failure in 2,465, and that one infrastructural" is enough to move a suite off
the pull-request path. Everything else in the stack stands on its own if this one is rejected.
