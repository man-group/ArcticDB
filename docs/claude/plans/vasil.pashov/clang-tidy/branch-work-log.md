# Branch work log: vasil.pashov/clang-tidy

Integrates clang-tidy into the ArcticDB analysis CI.

## Review follow-ups

- Verified the compilation database is already generated with clang, not gcc: the
  `clang-tidy` job runs in `ghcr.io/man-group/arcticdb-dev-clang:latest`, whose Dockerfile
  sets `CC=/usr/bin/clang` / `CXX=/usr/bin/clang++` (clang 19.1.7), and the `linux-debug`
  preset does not override the compiler. clang-tidy is pinned to 19.1.7 to match. PR comment
  r4063743358 assumed gcc; that premise does not hold for this job, so no compiler change was
  needed.
- Bumped the external actions in `clang_tidy.yml` to their latest major versions:
  `actions/checkout@v7`, `github/codeql-action/upload-sarif@v4`, `actions/upload-artifact@v7`.
  `mozilla-actions/sccache-action` only publishes `v0.0.x` tags (no floating major), so it is
  pinned to `v0.0.11`. The dev-clang image ships node 24, so the newer runtimes run.
- Stopped clang-tidy running on master pushes (merges): added `github.event_name != 'push'` to
  the job condition in `analysis_workflow.yml`. It now runs on pull requests, the nightly cron
  and manual dispatch only.
- Enabled HTML report artifacts on pull request runs (previously nightly-only) by extending
  `upload_artifacts` to `pull_request || schedule`.
- Updated the trigger table and prose in `CLAUDE.md` and the header comment in `clang_tidy.yml`
  to match.
