# External contributor CI work log

## 2026-10-02 — approved fork PR builds

- Branch: `codex/external-contributor-ci`, created from freshly fetched `origin/master` at
  `5e389f7ccdaee341bf0d7e86609afe76637368c0` in the separate `external-contributor-ci` worktree.
- Corrected explicit checkout/reusable-workflow permissions. Build/test jobs request read permissions; the image
  publisher retains its trusted package-write permission, which GitHub downgrades for fork PRs. The reusable image
  workflow adds no write-permission requirement. Both pytest callers and the reporter explicitly retain artifact-read access.
- Kept the full Linux/Windows/macOS C++ and Python 3.9–3.14 wheel/test matrix, conda builds, ASV checks, and sanitizers.
  Fork PRs skip EC2 benchmarks, S3 pytest telemetry, credentialed review, and release-label comment writes.
  Public ASV checkout falls back to the normal token when the Man PAT is unavailable.
- Replaced registry-dependent image misses with a local image build on each fork Linux wheel runner. Existing public
  images are inspected/pulled anonymously; trusted image publication and explicit image overrides remain supported.
- Added `off`/`read`/`readwrite` NuGet cache modes and source-build fallback on missing credentials, feed denial,
  failed setup, or optional NuGet download failure. Man's optional feed remains read-only. Credential-free build/ASV
  callers select `off`; cold-cache builds retain real compilation and test coverage.
- Added sccache backend selection before setup. Pinned sccache treats the presence of `SCCACHE_BUCKET` as S3
  configuration, including an empty string, and parses an empty SSL boolean as invalid. The helper unsets inactive
  settings and chooses GHA without AWS; Linux wheels receive the v2 service flag and runtime configuration.
  Verified against [pinned configuration parsing](https://github.com/mozilla/sccache/blob/v0.10.0/src/config.rs),
  [sccache-action setup](https://github.com/mozilla-actions/sccache-action/blob/v0.0.9/src/setup.ts), and
  [OpenDAL's pinned GHA backend](https://github.com/apache/opendal/blob/v0.52.0/core/src/services/ghac/core.rs).
- Preserved `can_merge` and `All conda jobs completed` context names while making them fail for failures,
  cancellations, and unexpected skips. Added lint and every platform's persistent verification to the main gate,
  including storage setup/cleanup. Docs-only builds and disabled persistent tests are the only allowed skips.
- Added CI-script regression coverage, routed workflow/cache-script changes to that workflow, and documented the
  approval process, retained test coverage, infrastructure skips, and cache/image behavior in CI and contributor guides.
- Followed red/green slices: demonstrated failing checks before permission/routing, cache, image, aggregate-gate,
  test-trigger/dependency, artifact-permission, and backend-selection changes. Shell tests use stubbed external commands
  and check no credentials, read-only mode, denied feeds, cold image resolution, force rebuilds, and trusted publication.
- Validation: `python -m pytest .github/scripts/tests/` passes all 193 tests (64 new regressions); required `make lint` passes with isolated Codex tooling
  under `/private/tmp/codex-external-ci-venv`, leaving the 294 Python and 650 C++ files unchanged. New tooling Python files
  are formatted with the same pinned Black version. Bash syntax checks and `git diff --check` pass.
- `actionlint` reports the same seven pre-existing diagnostics as a pristine master snapshot: dummy matrix types,
  missing `benchmark_all_tags` input declarations, and existing local-action metadata. No new diagnostics were introduced.
- Verified pinned cibuildwheel accepts a correctly matching local image with `--pull=never` in its
  [container implementation](https://github.com/pypa/cibuildwheel/blob/v3.1.4/cibuildwheel/oci_container.py).
- No ArcticDB engine/API code or ASV benchmark source changed; no engine build or cloud run was needed for local
  CI-script validation. No commit, push, PR creation, repository-setting change, workflow approval, or rerun was performed.

## Remaining live validation

- Run an actual external fork PR after maintainer approval, verify every normal build/test and artifact job, and confirm
  infrastructure/publication jobs skip. Test fresh commits, image hits/misses, an image-input change, and empty caches.
- Validate cold manylinux dependency/image builds within GitHub runner disk/time limits; local tests stub network/tools
  and do not prove a full wheel build on a live runner. Cold builds can be slower because no repository NuGet secrets exist.
- Confirm required check settings still refer to the preserved contexts and that the public developer images are
  anonymously readable. GitHub's approval policy remains a maintainer/admin responsibility.

## 2026-11-06 — rebase and publish

- Motivated by fork PR #3515: every wheel/C++ compile failed in sccache startup with S3 `AuthorizationHeaderMalformed`
  because `SCCACHE_BUCKET` was set while the AWS secrets were empty. `sccache_config.sh` addresses exactly this.
- Rebased onto `origin/master` (clean, no conflicts) and re-ran validation: 193 CI-script tests pass, Python lint clean,
  actionlint 180 findings vs 195 on master, shellcheck adds nothing new beyond pre-existing warnings.

## 2026-11-06 — review fixes

- `VCPKG_BINARY_CACHE_MODE` keyed off `secrets.VCPKG_NUGET_TOKEN`, which the repo doesn't define (trusted runs use
  the `GITHUB_TOKEN` fallback), so the NuGet cache was off everywhere. Now selected by trust: `readwrite` for pushes and
  same-repo PRs, `read` for forks (a denied feed already falls back to source builds).
- Restored `packages: write` on every `build_steps.yml` caller; GitHub caps it to read for fork PRs.
  `persistent_storage.yml` callers stay on `packages: read` since they never touch the feed.
- Regression tests: trusted runs without the NuGet secret get `readwrite`; `build_steps.yml` callers keep `packages: write`.
