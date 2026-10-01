# Branch: delete-staged-data-by-stage-result

## Goal

Augment the existing staged-data deletion APIs so they accept a `StageResult` or a
`List[StageResult]` in addition to a `symbol: str`. For the stage-result forms, only the
`APPEND_DATA` keys named by those receipt(s) are deleted (idempotent); the `str` form is
unchanged (deletes all incompletes for the symbol).

Motivation (Numeric): they stage from Spark workers and finalize via the listing-based path,
but retain the `StageResult` receipts. Pre-emption/retries re-stage partitions, creating
duplicate `APPEND_DATA` bundles; they need to delete precisely the bundles named by specific
receipts without wiping other workers' good staged data.

## Public API change (V1 and V2)

- V1: `NativeVersionStore.remove_incomplete(symbol: str | StageResult | List[StageResult])`
- V2: `Library.delete_staged_data(symbol: str | StageResult | List[StageResult])`

Backwards compatible — the `str` form behaves exactly as before.

## What was done

- **C++ helper (reuse, not reinvent):** extracted the flatten-and-remove block from
  `version_core.cpp:DeleteIncompleteKeysOnExit::~DeleteIncompleteKeysOnExit` into
  `incompletes.{hpp,cpp}:delete_incomplete_keys_for_stage_results(store, stage_results)` — a
  single batched `store->remove_keys(keys, RemoveOpts{.ignores_missing_key_ = true})`. The
  destructor now calls the helper (behaviour-preserving).
- **Engine:** `LocalVersionedEngine::remove_incompletes_for_stage_results(vector<StageResult>)`
  delegates to the helper. Inherited by `PythonVersionStore` (no `version_store_api` change
  needed).
- **pybind:** added `remove_incompletes_for_stage_results` binding next to `remove_incomplete`.
- **Python dispatch (single brain in V1):** `NativeVersionStore.remove_incomplete` inspects the
  argument — `str` → old C++ path; `StageResult` → wrapped in a one-element list; `list` →
  as-is; both non-str forms call the new binding. V2 `delete_staged_data` stays a thin forwarder
  to V1. Signatures widened to `Union[str, StageResult, List[StageResult]]` (no `@overload`
  stubs, per preference).
- **Design choices:** single batched delete (not a per-receipt loop) to avoid N storage
  round-trips; idempotent via `ignores_missing_key_`; cross-symbol lists allowed (atom keys are
  self-describing).

## Tests (`python/tests/unit/arcticdb/version_store/test_parallel.py`)

- `test_delete_staged_data_by_single_stage_result` (V1) — originally the failing/RED test.
- `test_delete_staged_data_by_list_of_stage_results` (V1)
- `test_delete_staged_data_by_string_removes_all` (V1 regression)
- `test_delete_staged_data_by_stage_result_is_idempotent` (V1)
- `test_delete_staged_data_by_stage_result_cross_symbol` (V1)
- `test_delete_staged_data_v2_by_stage_result` (V2 entry point)

All pass; full `test_parallel.py` suite green (549 passed, 63 skipped).

## Docs

- `docs/claude/cpp/STREAM.md` — added a "Deletion" subsection (by-symbol vs by-stage-result).
- `Library.delete_staged_data` docstring — widened, added `Examples` and a `stage` See Also.
- `NativeVersionStore.remove_incomplete` docstring — widened to describe all three forms.
