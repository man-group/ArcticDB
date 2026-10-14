# Plan: String column stats (truncated min/max)

## Context

ArcticDB column stats currently store per-row-slice min/max only for non-sequence
types; `MinMaxAggregatorData::aggregate` raises `E_UNSUPPORTED_COLUMN_TYPE` for
strings (`cpp/arcticdb/processing/unsorted_aggregation.cpp:72`). We want equality and
membership queries on string columns (`==`, `!=`, `isin`, `isnotin` — the only
string predicates the engine supports; ordering predicates raise in
`operation_types.hpp:444-450`) to prune row-slices that cannot match.

UTF-8 - Encode each character as between 1 and 4 "code points".
So a UTF-8 character is between 1 and 4 bytes (it's variable width).
For "ASCII" characters like "b", the UTF-8 encoding is 1 byte, and it matches ASCII.
For "weird" characters like an emoji, the encoding uses more bytes - up to 4 bytes total.
UTF-32 is like UTF-8 but actually fixed width - each character is 4 bytes.
Can't remember how the metadata is stored in UTF-8 (ie how wide is everything)

Particular test: what happens if we truncate halfway through a code point eg three chinese characters that take 3 bytes, and we truncate at 8 bytes, check we 
can still read the stats segment and that pruning works

Design decisions:

- **Always store stats as `UTF_DYNAMIC64` (UTF-8), transcoding at generation.** Regardless
  of the source column's string type, transcode each column's chosen min/max to UTF-8
  before storing: `UTF_FIXED` (UTF-32) is converted; `ASCII_FIXED` and `UTF_DYNAMIC` are
  already UTF-8. Then truncate at 64 UTF-8 bytes. Rationale (see dynamic-schema note
  below): under dynamic schema one column's slices can mix `UTF_FIXED` and `UTF_DYNAMIC`,
  and the stats-segment merge (`merge_column_stats_segments`, `column_stats.cpp:42`)
  reconciles per-slice stat columns to a common type anyway; transcoding up front makes
  every per-slice stat homogeneous UTF-8 with a uniform truncation unit, so the merge
  needs no promotion and `maxT` inference stays consistent.
- **Support unicode, not just ASCII ** The engine's string `==`/`!=`/`isin`/`isnotin` on
  *dynamic* strings is pure bytewise with no normalization on write (composed vs
  decomposed stay distinct; CJK, 4-byte emoji, empty, and prefix cases all match pandas
  — see Tests). Our pruning comparison is also bytewise UTF-8, so it is correct for any
  UTF-8.
- **Fixed-width columns stay in scope; do not regress them.** Because stats are UTF-8 and
  compared bytewise against the UTF-8 query, `ASCII_FIXED` and (via read-time promotion)
  mixed-UTF columns prune correctly. For a pure-static `UTF_FIXED` column the engine's own
  equality path (`ascii_to_padded_utf32`) is already broken for non-ASCII (returns
  nothing); our UTF-8 stats never prune away a genuine match (ASCII queries prune
  correctly and gain benefit; non-ASCII queries stay "no worse" since the engine returns
  nothing regardless). Correctness bar: **results with stats enabled == results with
  stats disabled** for every string column kind (dynamic / `ASCII_FIXED` / `UTF_FIXED`),
  including that pre-existing broken case. Enforced by an equivalence test. (The
  `UTF_FIXED` non-ASCII equality bug is out of scope, captured by an xfail regression
  test.) **Coupling to watch:** with the UTF-8-first min/max fix (generation, point 2) the
  pure-`UTF_FIXED` stats are correct in their own right, not merely by the accident of the
  broken equality path. Whoever eventually fixes the xfail'd `UTF_FIXED` non-ASCII equality
  bug must re-run the stats-on == stats-off equivalence tests, since fixing the row filter
  changes the baseline these stats are validated against.
- **Byte-boundary truncation (decided).** Truncate at exactly 64 UTF-8 bytes, which may
  split a multi-byte codepoint. This is harmless for pruning (bytewise) and keeps the
  length-based `maxT = (len(M) == 64)` inference. A truncated stored value may be invalid
  UTF-8, so the introspection path (`read_column_stats_experimental`) decodes with
  `errors="replace"`. (We rejected codepoint-boundary truncation, which would have forced
  an explicit per-value truncated flag since length would no longer signal truncation.)
- Treat the stored **min as exact** and the stored **max as possibly truncated when its stored length equals
  the truncation width**. "Exact" here does not mean the min is never truncated — it is
  truncated to `N` bytes like the max. It means we can treat it as exact for the low-side
  comparison: a byte-prefix is bytewise `<=` the full value, so a truncated stored min is
  still a valid lower bound on every value in the slice, and `prune iff q < m` therefore
  never wrongly prunes (it only forgoes some pruning when `m <= q < true_min`). This never causes a wrong prune; it only forgoes some
  pruning opportunities. The alternative would be to store a special flag marking whether the
  statistic is truncated or not.
- New proto stat types `MIN_STR_V1`/`MAX_STR_V1` (distinct on-disk format from
  numeric `MIN_V1`/`MAX_V1` so that we have a slot to record the truncation width).
    Note: We might not need this if we do the special IS_TRUNCATED column, why else would readers need to know the truncation width?
    
- Record null counts for strings for `!=`/`isnotin` to work properly and test it

### Dynamic schema interaction

Under dynamic schema a column's string type can vary across row-slices: `UTF_FIXED` and
`UTF_DYNAMIC` may coexist and promote to `UTF_DYNAMIC64` at read
(`type_utils.cpp:178`, `merge_descriptors.cpp:72`), while `ASCII_FIXED` cannot mix with
any UTF type (read raises `E_DESCRIPTOR_MISMATCH`, so such a symbol is unreadable and
gets no stats). A single column-stats key therefore covers per-slice stats of possibly
differing source types. Transcoding every per-slice min/max to UTF-8 at generation
(above) makes all per-slice stat columns homogeneous `UTF_DYNAMIC64` with a uniform
64-byte truncation unit, so `merge_column_stats_segments` needs no type promotion and
the stored stats match the read-time reconciled column type (`UTF_DYNAMIC64`).

### Truncation-aware pruning rules

We need to be careful to handle the truncation at the boundaries when comparing. If we have

min = "aaaa" (truncated)
max = "bbbb" (truncated)

and then filter on col == "bbbba" we cannot prune it, since its truncated form matches the max stat
(the true max could be, for example, "bbbbz").

We do not store an explicit truncated flag (DuckDB stores `StringStatsType`); instead we
infer it from the stored byte length — a stat is possibly-truncated iff its stored length
equals `N` (the max width), exact if shorter. The only length the comparison needs is the
stored bound's own byte length, which a variable-length UTF-8 stat records for free.

Following DuckDB's **constant-truncation** comparison
(`string_stats.cpp:CompareStringStats`) rather than computing an incremented upper bound:
on the **max** side, when `M` is possibly-truncated (`len(M) == N`), compare only the
first `len(M)` bytes of the query — `q[0:len(M)]` vs `M`. An equal prefix means `q` could
equal the (longer) true max, so we cannot prune. The **min** side is treated as exact
(below), so no truncation adjustment is applied there.

Prune (`NONE_MATCH`) iff `q` is below or above the slice:
- **Low side** (min treated exact): prune iff `q < m`.
- **High side**: `maxT == false` ⇒ prune iff `q > M`; `maxT == true` ⇒ compare
  `q[0:len(M)]` vs `M` and prune iff that prefix is `> M` (an equal prefix leaves the
  slice `UNKNOWN` — the `max="bbbb"`, `q="bbbbc"` case).

Otherwise `UNKNOWN`. `ALL_MATCH` only when the slice is provably one value:
`len(m) < N && len(M) < N && m == M == q`. `!=`/`isnotin` are the complements;
`isin` prunes iff every set member is prune-able.

Note: this comparison is pure bytewise, so it is correct for UTF-8 as well as ASCII
(UTF-8 bytewise order equals codepoint order; splitting a codepoint at the truncation
boundary is harmless under constant-truncation). See the UTF-8 scope decision above.

## Outstanding Choices

It might make sense it store a dedicated stat for IS_TRUNCATED rather than doing the length
comparisons - can decide at implementation time.

## Files and changes

Do 2 PRs one for the generation (points 1, 2, 4a, 5 below) and one for using these stats for filtering
at read time.

### 1. Proto — `cpp/proto/arcticc/pb2/column_stats.proto`
- Add `MIN_STR_V1 = 3; MAX_STR_V1 = 4;` to `ColumnStatsType`.
- Add `uint32 string_truncation_bytes = 3;` to `ColumnStatsHeader` (0/absent ⇒ no
  string stats present; writers set 64).

### 2. Generation — `cpp/arcticdb/processing/unsorted_aggregation.cpp`
- Replace the string `raise` (line 72) with a sequence-type branch in
  `MinMaxAggregatorData::aggregate`:
  - Iterate values via `input_column.string_at_offset(offset, /*strip nulls*/ true)`
    (same accessor used at `operation_dispatch_binary.hpp:232`).
  - **Transcode each value to UTF-8 *before* comparing, then track `min_`/`max_` in
    UTF-8.** `string_at_offset` returns the raw string-pool bytes, so for `UTF_FIXED` those
    are 4-byte-per-codepoint UTF-32 in native byte order (little-endian on x86).
    Bytewise comparison of little-endian UTF-32 is *not* codepoint order once codepoints
    differ above the low byte (e.g. `é` U+00E9 → `E9 00 00 00` sorts above `Ā` U+0100 →
    `00 01 00 00`, but by codepoint `é < Ā`). So we must not select min/max on the raw
    source bytes and transcode the winner afterwards — that stores an invalid bound for
    non-ASCII `UTF_FIXED` data. Instead, for `UTF_FIXED` (`is_utf_type &&
    is_fixed_string_type`) convert each value from UTF-32 to UTF-8 as it is read
    (`ASCII_FIXED` and `UTF_DYNAMIC` are already UTF-8), and compare/track in UTF-8, whose
    bytewise order equals codepoint order. Then store `value.substr(0, N)` (truncating the
    final min/max equals truncating each — truncation is monotone). No ASCII check — all
    UTF-8 supported.
    - A pure-`UTF_FIXED` non-ASCII column masks the compare-then-transcode bug, because
      the engine equality path returns nothing for non-ASCII regardless (see design
      decisions), so stats-on == stats-off holds by accident. The **mixed dynamic-schema**
      case does not mask it: mixed `UTF_FIXED`/`UTF_DYNAMIC` promotes to `UTF_DYNAMIC64` at
      read and the row filter matches non-ASCII correctly, so a bad bound on a fixed slice
      would prune a genuinely-matching slice. **Test this exact case:** a mixed-schema
      column whose `UTF_FIXED` slice contains non-ASCII codepoints that differ above the
      low byte (so LE-UTF-32 order disagrees with codepoint order), queried `== q` for a
      value in that fixed slice, asserting stats-on == stats-off. Ensure the fixed slice
      really holds non-ASCII or the bug hides.
- Add string members / width constant (`kStringTruncationBytes = 64`). Store min/max as
  `std::optional<Value>` always typed `UTF_DYNAMIC64` (the transcoded, 64-UTF-8-byte
  prefix), so every per-slice stat is homogeneous UTF-8 regardless of source type. Value
  already supports sequence types (`value.hpp:30`).
- `MinMaxAggregatorData::finalize` (line 79): when the value is a sequence type, create
  the min/max columns as `UTF_DYNAMIC64` and write the truncated bytes via the segment
  string pool; tag entries `MIN_STR_V1`/`MAX_STR_V1`. When no values seen, emit nothing
  for the column (existing all-null path already yields absent stats).
- `ColumnStatsGenerationClause::process` (`clause.cpp:865`): set
  `merged_header.set_string_truncation_bytes(64)` when any string entry is present.

### 3. Read / load — `cpp/arcticdb/pipeline/column_stats_filter.cpp`
- `load_stats_by_column` (line 180): add a sequence-type branch that reads each stats
  string via `segment.string_at_offset(offset)` into a `Value{string_view, UTF_DYNAMIC64}`
  (stats are always stored as UTF-8).
- Thread `header.string_truncation_bytes()` into `ColumnStatsData` and out through
  `values_for_column`.

### 4. Comparator — `cpp/arcticdb/pipeline/column_stats_dispatch.hpp` (+ new helper)
- Add `std::optional<uint32_t> string_truncation_bytes` to `ColumnStatsValues`
  (`column_stats_filter.hpp:39`), populated by `values_for_column`.
- In `stats_comparator` (line 107): add an `is_sequence_type(StatsTag::data_type)`
  branch that calls a new `string_stats_comparator(min_sv, max_sv, N, q_sv, op)`
  implementing the equality rules above (only EQ/NE meaningful; other ops return
  `UNKNOWN`). Put the constant-truncation `string_range_verdict()` in a small
  `column_stats_string.hpp`/`.cpp` so it is unit-testable.
- `stats_membership_comparator` (`column_stats.cpp` / dispatch): add string handling
  for `isin`/`isnotin` over a string `ValueSet`, reusing the per-member equality
  verdict.

### 4a. Introspection decode — `read_column_stats_experimental`
- Byte-boundary truncation can leave a stored min/max as invalid UTF-8. The
  read-back/denormalization path that surfaces stats as a DataFrame must not raise on
  it — decode with `errors="replace"`. Confirm the exact spot during impl (`_store.py`
  `read_column_stats_experimental` and the C++ string denormalization in
  `python_strings.cpp`).

### 5. Python — `python/arcticdb/version_store/_store.py`
- `_get_eligible_column_stats_spec` (line 1318): add all string column types (dynamic
  and fixed-width) to the eligible set so `create_column_stats_experimental` includes
  them. No fixed-width exclusion — correctness rests on the stats-on == stats-off
  equivalence test (see design decisions).

## Tests (TDD — write failing first)

- Add ASV test coverage in the column stats benchmarks
- **Pre-req unicode verification — DONE, tests landed.** Added to
  `test_filtering.py`: `test_filter_string_equals_unicode`,
  `test_filter_string_not_equal_unicode`, `test_filter_string_isin_unicode`,
  `test_filter_string_unicode_normalization`, `test_filter_string_equals_blns`,
  `test_filter_string_isin_blns` (dynamic strings; BLNS via
  `read_big_list_of_naughty_strings`). All pass. Confirmed no normalization on write.
  Also added `test_filter_string_equals_unicode_fixed_width` (`xfail(strict=True)`)
  documenting the pre-existing fixed-width non-ASCII equality bug (returns nothing).
- **C++ unit** (`cpp/arcticdb/pipeline/test/`): `string_range_verdict` cases — exact
  prune (`q>M`, `q<m`), truncated-max band `UNKNOWN` (`M="bbbb"`, `q="bbbbc"` ⇒
  UNKNOWN), truncated-max above band prunes (`q="bbbc"`), min-edge (`m="bbbb"`,
  `q="bbbba"` low-side not pruned), `ALL_MATCH` single-value, and a multi-byte-UTF-8
  value truncated mid-codepoint (constant-truncation still sound; stored bytes may be
  invalid UTF-8).
- **Python** (`python/tests/unit/arcticdb/test_column_stats_optimisation.py`, feature
  flag `ColumnStats.UseForQueries=1`): unicode string column across multiple slices —
  `==`/`isin` prune the correct slices; a query in a truncated max band is **not**
  pruned yet returns correct rows; round-trip correctness with and without stats is
  identical; `read_column_stats_experimental` succeeds even when a max is truncated
  mid-codepoint. Also cover cases with truncated min and negation like:
  ```
  q = q[~(q["a"] == "blah")]
  ```
  where min == blah (truncated) and max == blah - our logic must give UNKNOWN.
- **Stats-on == stats-off equivalence (fixed-width safety).** For each string column
  kind (dynamic, `ASCII_FIXED`, `UTF_FIXED`) and a spread of `==`/`!=`/`isin` queries
  (ASCII and non-ASCII), assert the result with column stats enabled equals the result
  with them disabled — including the currently-broken fixed-width non-ASCII case. This
  is the guard that fixed-width support never makes matters worse.
- **Dynamic schema, mixed string types.** With dynamic schema on, build a symbol whose
  slices mix `UTF_FIXED` and `UTF_DYNAMIC` for the same column (e.g. write with
  `dynamic_strings=False` on ASCII-then-unicode data / append with `dynamic_strings=True`,
  so segments differ), then `create_column_stats` and query `==`/`isin` with ASCII and
  non-ASCII values. Assert: stats generation succeeds; the merged stats column is a
  single `UTF_DYNAMIC64` column (homogeneous after transcode); and stats-on results equal
  stats-off results. Also cover the all-`ASCII_FIXED` dynamic-schema column. (An
  `ASCII_FIXED` + UTF mix is unreadable by design — assert it raises rather than
  producing stats.)
