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
