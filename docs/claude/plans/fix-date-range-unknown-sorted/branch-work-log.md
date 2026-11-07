# fix-date-range-unknown-sorted work log

- Bug: unvalidated Arrow writes (`validate_index=False`) skipped the index sortedness scan, recording `UNKNOWN`. Reads treat `UNKNOWN` as sorted (legacy data), so `date_range` reads of unsorted Arrow data silently returned wrong rows instead of raising `UnsortedDataException` like pandas.
- Fix: `write`, `batch_write`, `append` and `batch_append` always scan Arrow index sortedness (`SortednessScan::SCAN_IF_UNKNOWN`), matching pandas, which always records sortedness. Read path unchanged, so `UNKNOWN` data keeps reading as before.
- Tests: `test_unvalidated_arrow_write_records_sortedness` and `test_unvalidated_unsorted_arrow_write_date_range_read_raises` in `test_arrow_writes.py`; updated get_info/get_description and Polars sorted-flag tests that asserted `UNKNOWN` for unvalidated Arrow writes.
