# Branch work log: aseaton/fix/ci-do-not-run-docs

## 2026-08-04

- Added `.github/workflows/detect_docs_only.yml`, a reusable workflow that
  diffs changed files against the base SHA and outputs `docs_only` when every
  changed file matches `*.md`, `docs/**`, `LICENSE*`, or `.gitignore`.
  Defaults to `docs_only=false` for any event/ambiguity it can't confidently
  classify (schedule, workflow_dispatch, unborn base SHA).
- Wired it into `build.yml`: gated `cibw_docker_image` (and everything that
  cascades from it — `common_config`, the `cpp-test-*` and
  `build-python-wheels-*` jobs) and `publish_pytest_data` behind
  `docs_only != 'true'`. `can_merge` needed no change — it already treats
  skipped dependencies as passing.
- Wired it into `build_with_conda.yml`: gated `linux_64`, `linux_aarch64`,
  `osx_arm64`, `win_64`, and `publish_pytest_data` the same way.
  `can_merge_conda` needed no change for the same reason.
- Motivated by PR #3294 (a one-line README change) triggering the full
  cross-platform build/test matrix.
