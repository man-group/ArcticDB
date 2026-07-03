# Plan: make the admission handler the single source of segment futures and processing units

Status: **planned, not implemented.**

## Motivation

In `read_and_schedule_processing` the plumbing around the admission handler is redundant:

```cpp
auto admission = std::make_shared<ProcessingUnitAdmissionHandler>(
        store->make_uncompressed_reader(columns_to_decode(pipeline_context)),
        std::move(ranges_and_keys),
        std::vector(processing_unit_indexes),   // a COPY; the original goes to schedule_clause_processing
        max_admitted_processing_units);
auto segment_and_slice_futures =
        add_schema_check(pipeline_context, admission->futures(), std::move(incomplete_bitset), processing_config);
auto processed = schedule_clause_processing(
        component_manager, std::move(segment_and_slice_futures),
        std::move(processing_unit_indexes),      // the original
        std::make_shared<std::vector<std::shared_ptr<Clause>>>(read_query->clauses_),
        admission);
```

- `processing_unit_indexes` is held twice: a copy inside the handler and the original passed to
  `schedule_clause_processing`.
- `segment_and_slice_futures` is just `admission->futures()` with the incomplete-schema check chained
  on, yet both it and `admission` are passed to `schedule_clause_processing`.

## Design

Fold the schema check into the reader closure the handler is constructed with, so `admission->futures()`
already yields schema-checked futures. Then `schedule_clause_processing` takes only the admission handler
and reads both the futures and the processing-unit structure off it.

The per-segment incomplete flag is already carried by `RangesAndKey::is_incomplete()` (that is exactly
what `get_incompletes_bitset` is built from), so the check can be applied per-call inside the reader
rather than positionally over a vector.

### Composed reader (in `read_and_schedule_processing`)

```cpp
auto base = store->make_uncompressed_reader(columns_to_decode(pipeline_context));
SegmentReader reader = [base = std::move(base),
                        desc = pipeline_context->descriptor(),
                        processing_config](pipelines::RangesAndKey&& rk) {
    const bool is_incomplete = rk.is_incomplete();
    return base(std::move(rk)).thenValueInline(
            [desc, processing_config, is_incomplete](pipelines::SegmentAndSlice&& r) {
                if (is_incomplete && !processing_config.dynamic_schema_) {
                    auto check = check_schema_matches_incomplete(r.segment_in_memory_.descriptor(), desc);
                    if (std::holds_alternative<Error>(check)) {
                        std::get<Error>(check).throw_error();
                    }
                }
                return std::move(r);
            });
};
```

`fire()` already sets the promise via `setTry`, so a thrown schema error propagates exactly as it does
through `add_schema_check` today.

### `schedule_clause_processing` reads off the handler

New signature:

```cpp
folly::Future<std::vector<EntityId>> schedule_clause_processing(
        std::shared_ptr<ComponentManager> component_manager,
        std::shared_ptr<ProcessingUnitAdmissionHandler> admission,
        std::shared_ptr<std::vector<std::shared_ptr<Clause>>> clauses);
```

Body derives what it needs:

```cpp
auto segment_and_slice_futures = admission->futures();          // called exactly once
const auto& processing_unit_indexes = admission->processing_units();
```

`generate_segment_fetch_counts` already takes a `const span`, and `get_entity_ids_and_position_map` only
*reads* `processing_unit_indexes` (its `&&` parameter is never moved from) — change that parameter to
`const std::vector<std::vector<size_t>>&`. The handler becomes the sole owner of the indexes.

### `read_and_schedule_processing` collapses to

```cpp
const size_t max_admitted_processing_units = num_processing_units_live(processing_unit_indexes);
auto admission = std::make_shared<ProcessingUnitAdmissionHandler>(
        reader, std::move(ranges_and_keys), std::move(processing_unit_indexes), max_admitted_processing_units);
auto processed = schedule_clause_processing(
        component_manager, admission,
        std::make_shared<std::vector<std::shared_ptr<Clause>>>(read_query->clauses_));
admission->admit_initial(max_admitted_processing_units);
return std::move(processed).via(&async::cpu_executor());
```

`num_processing_units_live` and `get_incompletes_bitset`/`add_schema_check` are read *before* the moves, so
ordering is fine — except `get_incompletes_bitset` and `add_schema_check` are no longer needed at all.

## Handler changes (`ProcessingUnitAdmissionHandler`, version_core.hpp)

- Add `const std::vector<std::vector<size_t>>& processing_units() const { return processing_units_; }`.
- `futures()` is unchanged (still one-shot; must be called exactly once — now by `schedule_clause_processing`).

## `schedule_first_iteration`: guard becomes an assertion

Because every caller now supplies an admission handler, replace

```cpp
if (admission) {
    processing_fut = std::move(processing_fut).ensure([admission]() { admission->on_unit_complete(); });
}
```

with an assertion that it is present, then unconditionally chain:

```cpp
internal::check<ErrorCode::E_ASSERTION_FAILURE>(
        static_cast<bool>(admission), "schedule_first_iteration requires an admission handler");
processing_fut = std::move(processing_fut).ensure([admission]() { admission->on_unit_complete(); });
```

NB: this is only valid once the test call sites (below) pass a handler; it cannot be done as a standalone
edit before them, since the parallel-processing tests currently pass `nullptr`.

## Dead code to remove

- `add_schema_check` (version_core.cpp) — folded into the reader.
- `get_incompletes_bitset` (version_core.cpp) — the per-segment flag on `RangesAndKey` replaces it.
- The `= nullptr` default on `schedule_clause_processing`'s admission parameter (parameter is being
  removed anyway; the new signature has no optional admission).

## Test changes (`test_parallel_processing.cpp`)

Two call sites (`ScheduleRowSliceProcessing...` around lines 222 and 263) currently pass promise-backed
futures and no admission handler. Change them to construct a handler whose reader resolves via those
promises — the same shape as `make_admission_handler` in `test_column_stats_memory.cpp`:

```cpp
std::vector<RangesAndKey> ranges;
for (size_t i = 0; i < num_segments; ++i)
    ranges.emplace_back(RowRange{i, i + 1}, ColRange{0, 1}, entity::AtomKey{});
version_store::SegmentReader reader =
        [&promises](RangesAndKey&& rk) { return promises[rk.row_range().first].getFuture(); };
auto admission = std::make_shared<version_store::ProcessingUnitAdmissionHandler>(
        std::move(reader), std::move(ranges), std::move(processing_unit_indexes), /*k=*/num_segments);
auto fut = schedule_clause_processing(component_manager, admission, clauses);
admission->admit_initial(num_segments);
push_segments(segment_and_slice_promises);
```

`push_segments` is unchanged; the handler just wraps the promises. Benefit: these tests now exercise the
real admission-driven path instead of hand-fed futures. Lift a small shared helper for the two sites.

## Open decision

`admit_initial`: keep it caller-driven (production + both tests call it after `schedule_clause_processing`),
or store `k` in the handler and fold `admit_initial()` into the end of `schedule_clause_processing` so no
caller has to. Folding it in removes an ordering footgun (must run after the `.ensure(on_unit_complete)`
chain is set up) at the cost of a little more handler state. Leaning toward folding in; decide at
implementation time.

## Verification

- `make lint`.
- C++: `*ProcessingUnitAdmission*`, `*Residency*`, `*NumProcessingUnitsLive*`, and the parallel-processing
  `Clause` tests (`ScheduleRowSlice*`), plus the existing column-stats suite.
- Python: `test_column_stats_creation.py` against a rebuilt extension.
- Confirm an incomplete-schema mismatch still raises (the schema check moved into the reader) — covered by
  existing append/compaction read paths; add/verify a targeted case if none exercises it.
