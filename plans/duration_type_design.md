# Design: a query-only duration type

Follow-on from PR #3281 (`aseaton/bug/8065794446/mixed-time-int`). That PR narrowed `is_numeric_type` to exclude
time types, rejected mixed timestamp/numeric expressions, and rejected `+`, `*`, `/` and `**` between two
timestamps. It deliberately left `timestamp ± integer` and `timestamp - timestamp` as warts, because there was no
type to express a duration. This document specifies that type.

Decisions below are settled unless marked **DEFAULT** — those are proposals awaiting confirmation.

## 1. Scope

The duration type is **query-only**. It is a real `DataType` that flows through the processing pipeline and reaches
Python as a `timedelta64[ns]` column. It is never written to storage: the write path refuses it, which
`_normalization.py:239` already does for `timedelta64` via `ArcticDbNotYetImplemented`.

Reading a projection that produced a duration column and writing that DataFrame straight back therefore fails with
that existing error. This is intended.

## 2. Representation

`int64` nanoseconds, the same physical representation as `NANOSECONDS_UTC64`.

A new `ValueType` is required, since `DataType` is composed from `ValueType` and `SizeBits` via
`detail::combine_val_bits` (`cpp/arcticdb/entity/types.hpp:196`):

```
// cpp/proto/arcticc/pb2/descriptors.proto — additive, currently tops out at BOOL_OBJECT = 14
TIMEDELTA = 15;
```

```cpp
TIMEDELTA_NS64 = detail::combine_val_bits(ValueType::TIMEDELTA, SizeBits::S64),
```

Adding the enumerator is additive and breaks no reader, because no client ever writes it. **DEFAULT** on the
names `ValueType::TIMEDELTA` / `DataType::TIMEDELTA_NS64`: they mirror pandas' vocabulary, where the existing
`NANOSECONDS_UTC` mirrors nothing in particular. `DURATION_NS64` is the alternative.

New predicates alongside the ones PR #3281 introduced: `is_duration_type`, and a revised `is_time_type` that must
**not** include durations — a duration is not a point in time, and conflating them would silently re-admit every
nonsense the previous PR just closed off.

## 3. Arithmetic

| Expression | Result | Note |
|---|---|---|
| `ts - ts` | duration | **breaking**, returns `int64` today |
| `ts + duration`, `duration + ts` | timestamp | |
| `ts - duration` | timestamp | |
| `duration - ts` | error | not meaningful |
| `duration ± duration` | duration | |
| `duration * int`, `int * duration` | duration | |
| `duration / int` | duration | truncates toward zero |
| `duration / duration` | error | |
| `duration * duration` | error | |
| `duration ** x`, `x ** duration` | error | |
| `ts ± int` | error | **breaking**, legal today as a nanosecond offset |
| `int - ts` | error | unchanged from PR #3281 |
| `ts + ts`, `ts * ts`, `ts / ts`, `ts ** ts` | error | unchanged from PR #3281 |

Division truncates toward zero, matching C++ integer division and pandas `Timedelta // n`. Consequence to accept
knowingly: `t1 + (t2 - t1) / 2` lands one nanosecond before the true midpoint on odd-length intervals.

Division by zero raises. A literal `0` divisor is rejected when the expression is built; a divisor *column*
containing a zero raises during processing, per element.

Overflow raises, per element. Durations span roughly ±292 years at nanosecond resolution, so this is reachable by
multiplication well before it is reachable by addition.

`classify_time_arithmetic` in `operation_types.hpp` is the seam. Note its existing `TimeArithmeticKind::DURATION`
currently means "`ts - ts`, yielding `int64`" and will change meaning rather than merely gaining siblings, so every
use of that enumerator needs revisiting rather than extending.

## 4. Comparisons, membership, ternary

- Duration compares only with duration. `duration_col > 5` is an error; `> pd.Timedelta(nanoseconds=5)` is the
  spelling. This mirrors the timestamp rule PR #3281 established.
- Duration never compares with a timestamp.
- `isin`/`isnotin` accept duration value sets, with the same unit normalisation as section 7.
- **DEFAULT** ternary: `where(cond, duration, duration)` is legal and yields a duration; any mixture of duration
  with timestamp, numeric or string is an error.
- **DEFAULT** unary: `-duration` and `abs(duration)` are permitted. Both are meaningful, unlike on a timestamp,
  where PR #3281 rejects them.

## 5. Aggregation

`min` and `max` are supported, in both groupby and resample, and return a duration.

`sum` and `mean` are deferred. Both are meaningful over durations — unlike `sum` over a timestamp, which is
meaningless and is why `sorted_aggregation.cpp:399` excludes it — so this is a scoping choice, not a semantic one,
and the gating predicate should be written so that admitting them later is a one-line change rather than a
restructuring.

`first`, `last` and `count` follow whatever the existing time-type handling does, since none of them inspect the
value's meaning.

Until `sum` and `mean` land, a duration column reaching either must raise `SchemaException` naming the column
rather than falling through to `int64` treatment. Note that `sorted_aggregation.cpp:399` currently gates on
`is_time_type`, so a duration type that is correctly excluded from `is_time_type` (section 2) will not be admitted
by that clause at all — the duration cases must be added explicitly, and a test must pin each aggregator, or the
whole set fails closed and `min`/`max` silently do not work.

## 6. Casts

Both directions, using pandas' unit strings (`"ns"`, `"us"`, `"ms"`, `"s"`, `"m"`, `"h"`, `"D"`, and the
spelled-out forms pandas accepts):

```python
q["int_col"].to_duration("nanoseconds")   # integer column -> duration
q["dur_col"].to_int("nanoseconds")        # duration -> integer count
```

The forward cast is not optional. Rejecting `ts ± int` removes `q["ts_col"] - q["int_col"]`, which works today and
is advertised in the PR #3281 description; without a cast there is no way to apply a per-row offset held in an
integer column, since a `pd.Timedelta` literal only covers the scalar case.

The inverse cast is the migration path for `ts - ts` no longer returning `int64`.

**DEFAULT** on `to_int`'s name; `to_nanoseconds()` or an `astype` spelling are alternatives.

## 7. Python surface

Accepted as duration scalars and value-set elements: `pd.Timedelta`, `np.timedelta64`, `datetime.timedelta`.

`np.timedelta64` arrays are normalised to nanoseconds regardless of their declared unit. This is the identical bug
fixed for `datetime64` in section 3 of PR #3281, where `np.datetime64` was absent from the recognised time types
and a `datetime64[s]` value set silently filtered on the wrong instant. Build it in from the start.

A value set may not mix durations with timestamps or with plain numbers.

Projection results come back as `timedelta64[ns]` columns.

## 8. Breaking changes

Three, on top of the two in PR #3281:

1. `ts - ts` returns a duration rather than `int64`. This is the dangerous one — `(t1 - t2) > 1000` keeps parsing
   and changes meaning. Mitigated by `to_int`.
2. `ts ± int` is rejected. Mitigated by `to_duration`.
3. `ts_col ± int_col` is rejected. Same mitigation.

All three need to be in the release notes with the migration spelled out, not merely listed.

## 9. Test plan

Beyond the table in section 3, one case per row:

- `t1 + (t2 - t1) / 2` returns a timestamp column, with a test pinning the odd-interval truncation so the rounding
  decision is recorded in a test rather than in this document alone.
- `t1 + (t2 - t1) / 2` where `t2 < t1`, to pin truncation-toward-zero on a negative duration.
- Overflow: `duration * large_int` raises rather than wrapping.
- Division by zero, both as a literal and as a divisor column containing a zero among non-zeros — the latter is
  what distinguishes a per-element check from a schema-time one.
- Round trip: `q["int_col"].to_duration("ns").to_int("ns")` is the identity.
- Non-nanosecond `np.timedelta64` value sets over every unit, mirroring
  `test_filter_datetime_isin_non_nanosecond_resolution`.
- `min` and `max` over a duration column, in both groupby and resample, returning a duration and asserting the
  output dtype. `sum` and `mean` over a duration raise `SchemaException`. PR #3281 showed that dtype assertions are
  exactly what was missing here, so `check_dtype` must be on.
- Writing a DataFrame containing a duration column raises `ArcticDbNotYetImplemented`.
- Output-schema tests: `ExpressionNode::compute` agrees with the read-time path on the result type for every row of
  the section 3 table. PR #3281 found a real divergence between those two paths, so this is not theoretical.

Per `CLAUDE.md`, each test is written and seen to fail before the corresponding change.

## 10. Out of scope

- Persisting duration columns, and therefore duration columns as an index.
- `sum` and `mean` over durations (section 5).
- A separate `timedelta64` resolution other than nanoseconds.
- The `Column["dt1"] ADD Column["dt2"]` AST label convention, which renders operator names rather than symbols in
  echoed queries. Pre-existing, shared with other messages, repo-wide to change.

## 11. Open question for PR #3281, before it is committed

Decisions 1 and 2 in section 8 make two things that PR is about to document as stable — `ts - ts → int64`, and
`ts ± integer` as the one permitted mixed case — temporary. Adding a line to the `QueryBuilder` docstring now
saying both are expected to change is cheap; retrofitting it after release is not. Unanswered.
