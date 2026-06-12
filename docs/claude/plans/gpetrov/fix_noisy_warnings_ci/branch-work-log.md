# Branch work log: gpetrov/fix_noisy_warnings_ci

## Round 1 — warnings from the 3.11 scheduled run (committed, in PR #3168)

Analyzed https://github.com/man-group/ArcticDB/actions/runs/27384163226 (48 pytest jobs, 38 unique warnings).

- Moved pytest marker registration from `pyproject.toml` into `pytest_configure` in `python/tests/conftest.py` — CI runs pytest from a temp dir (`build_tooling/parallel_test.sh`) where `pyproject.toml` is invisible, so all 24 marks raised `PytestUnknownMarkWarning`. One-mark registration added to `python/installation_tests/conftest.py`.
- Added per-file `pytestmark = pytest.mark.filterwarnings("ignore:Staging data with:DeprecationWarning")` to the 5 files intentionally testing deprecated staging APIs.
- 8 commits of mechanical pandas-deprecation fixes: `H`/`S` freq aliases, positional `Series.__getitem__` → `.iloc`, None-vs-NaN expected-frame normalization, `fillna(False)` downcast → `where().astype(bool)`, chained assignment, `arctic_simulator` column-insert fragmentation, raw-string regex prefixes, float32 overflow in `generate_random_floats`.
- Deliberately deferred: `BlockManagerUnconsolidated` deprecation (zero-copy read path in `_normalization.py`, needs design decision). `M`/`Q`/`Y` freq aliases not migrated (replacements absent in older supported pandas).

## Round 2 — warnings from the PR's 3.13 matrix (this session, uncommitted)

The PR CI (run 27407218638, job "3.13 Linux / unit-DefaultCache") surfaced a new warning population on Python 3.13 + newer pandas. Verified all round-1 categories were gone there.

- `.iloc` fixes in `arcticdb/util/append_and_defrag.py`, `defrag_timeseries.py` (5241 occurrences, hot loops) and 5 more test files (test_parallel, test_resample, test_sort_merge, test_symbol_concatenation, plus MultiIndex `.dtypes[i]`).
- SettingWithCopyWarning: `.copy()` on sliced/queried expected frames in test_lazy_dataframe, test_query_builder, test_filtering.
- test_ternary: replaced `fillna(..., inplace=True)` on column slices (chained-assignment FutureWarning) and whole-frame/column `fillna(False)` object→bool downcasts.
- Added `nans_to_none()` helper to `arcticdb/util/test.py`; used in test_empty_writes, test_symbol_concatenation, test_ternary for dynamic-schema None-vs-NaN expected frames.
- `test_match_on_string_none_nan_indistinguishable` (×2 in test_merge_update) deliberately asserts None/NaN equivalence → filterwarnings mark instead of data fix. NOTE: these will need a real fix when pandas makes mismatched null-likes raise.
- Staging suppression pytestmark added to test_arrow_writes (missed in round 1; only ran on newer matrix).
- defrag-API deprecation (defragment_symbol_data/is_symbol_fragmented) testing tests got filterwarnings marks (test_append, test_filtering, test_kwarg_validation).
- `datetime.utcnow()` → `datetime.now(timezone.utc)` in both conftests (Python 3.12+ deprecation).
- test_empty_writes append-empty-series: exclude empty entries before `pd.concat` (pandas empty-concat dtype FutureWarning).
- test_resample unsupported-frequency test passes deprecated aliases deliberately → filterwarnings mark; pickling test's `resample("T")` → `"min"`.
- conftest filterwarnings ignore for hypothesis's internal `is_categorical_dtype` deprecation (hypothesis pinned <6.73 on purpose — commit 783c893eb — so unfixable here; 2611 occurrences).
- Left alone: fork()-in-multithreaded-process DeprecationWarning (Python 3.13, env-level), InsecureRequestWarning (localhost TLS-less simulators), mongo fixture-cleanup noise, BlockManagerUnconsolidated.

Verification: local runs in `/tmp/mark_repro` (CI-style symlinked tests dir) with venv `~/venvs/314`; changed `arcticdb/` util files must be copied into the venv's site-packages before runs (tests import installed package, not repo source).

## Round 3 — full-matrix PR run (run 27409690219, head 0e805a63d)

The full 3.9–3.14 matrix surfaced pre-existing emitters the 3.11-only runs hid (and my round-1
counts had under-counted: pytest "file.py: N warnings" multiplier lines weren't expanded).

- **Switched staging + BlockManagerUnconsolidated suppression to global conftest filterwarnings**
  (user request) and removed all 17 per-file `pytestmark` staging suppressions added in rounds 1–3.
- `nans_to_none` on expected frame in test_stress_dynamic_bucketize (~226k warnings, the biggest single source).
- Both-sides `nans_to_none` in the two string schema-change tests in test_stress_append (written NaNs
  round-trip as NaN, backfill comes back as None — one-sided normalization can't work).
- `assert_frame_equal_with_arrow` now normalizes null-likes on both sides (arrow has a single null type) —
  fixes test_arrow_read/test_arrow_api wholesale.
- hypothesis/test_sort_merge `assert_equal` helper normalizes both sides; the two hypothesis merge_update
  tests and the three deliberate null-filtering tests in test_filtering got filterwarnings marks.
- `arcticdb/util/tasks.py`: added `_utcnow()` helper returning naive UTC — a plain `datetime.now(timezone.utc)`
  substitution would have broken the snapshot-name strptime roundtrip and naive/aware subtraction.
- test_stress_merge_update: accumulate parts + single concat (empty-entry concat deprecation).
- test_stress_write_peakmem `create_mixed_type_df`: dict-astype + `.copy()` — astype(dict) still leaves one
  block per column (verified empirically); only copy() consolidates, which silences the fragmentation warning
  raised from `_normalization.py:961` (`reset_index(inplace=True)` on the MultiIndex write path).
- Marked test_defragment_no_work_to_do (defrag deprecation), added hypothesis `Series.__setitem__`
  module-scoped ignore (verified module-scoped filters keep the same message visible from our code).
- Unit-job failures in CI (test_deep_nesting_metastruct_size_over_limit) are pre-existing, unrelated.

## Round 4 — stragglers from the completed full-matrix run (run 27415317796, head 541743f3d)

Run confirmed the round-3 fixes: 8 unique warnings across all 138 pytest jobs (from 15 unique /
~300k occurrences). Three were still actionable, fixed in 41dd4c516:
- filterwarnings mark on test_prune_previous_defragment_symbol_data (276x, deliberate defrag-API test)
- utcnow in test_stress_delete via naive `now(timezone.utc).replace(tzinfo=None)` — values go into
  library names, aware isoformat would inject "+00:00"
- filterwarnings mark on hypothesis test_resample_dynamic_schema (empty/all-NA concat inherent to
  generated differing-column-set frames)

Remaining (environment-level, intentionally left): fork()-in-multithreaded (3.12+), localhost
InsecureRequestWarning, LMDB-on-Windows (238x — possible fixture bug worth a look) and Mongo
fixture-cleanup warnings, boto3 3.9 EOL notice.

Pre-existing test_deep_nesting_metastruct_size_over_limit failures now hit every unit job on all
18 matrices — unrelated to this PR, needs its own ticket.

## Round 4 follow-up — suppress environment-level warnings (70f20cad1)

Per user decision: the four environment-level sources (ExceptionInCleanUpWarning from storage
fixtures, fork()-while-multi-threaded on 3.12+, InsecureRequestWarning, boto3 3.9 EOL) are now
globally ignored in conftest, with investigation tracked in GitHub issue #3170. Filter semantics
verified locally (all four suppressed, unrelated warnings still visible). CI pytest warning
summaries should now be empty.
