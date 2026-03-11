This PR covers two bugs found after converting `test_read_index.py` to use tiny segments. I want
to change the test to use tiny segments so we have better test coverage for our index filtering logic
before I add column stats. I also noticed bugs in how we prune data segments when reading only the index,
which I will fix in a subsequent PR.

First bug:

We were returning different indexes in test_read_index.py::test_row_range_across_row_slices depending on the segmentation
strategy. The tests failed after changing to tiny segments.

_compute_filter_start_end_row subtracts frame_data.offset (the start row of the first fetched segment)
from the row filter to get array-relative indices. For row_range=(3, 8) with tiny segments where the
first included segment starts at row 2, this gives row_range=(1, 6) instead of (3, 8).

Second bug, not related to slicing:

Reads with columns=[] and columns=None construct RangeIndex's differently.
  - columns=None path (_denormalize_single_index): builds RangeIndex(start=original_start, stop=original_start + n_rows * step).
  For row_range=(3, 8) returning 5 rows from RangeIndex(0, 10): → RangeIndex(0, 5).
  - columns=[] path (_postprocess_df_with_only_rowcount_idx): builds the full symbol index RangeIndex(0, 10) and slices it by position.
  For row_range=(3, 8): → RangeIndex(0, 10)[3:8] = RangeIndex(3, 8).
