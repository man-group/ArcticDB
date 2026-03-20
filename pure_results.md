(main) aseaton@dlonapatcs502:~/source/ArcticDB/python DEV $  python ../run_benchmark_index.py --backends pure --sizes medium large --columns 100
=============================================================
Backend: pure | Rows: 1,000,000 | Columns: 100 | Segments: 10
=============================================================

--- Key Sizes ---
  Separate library:
    TABLE_INDEX           count=   1  compressed=2.74 KB
    COLUMN_STATS          count=   1  compressed=598 B
    TABLE_DATA            count=  10  compressed=807.36 MB
  Embedded library:
    TABLE_INDEX           count=   1  compressed=31.70 KB
    COLUMN_STATS          count=   0  compressed=0 B
    TABLE_DATA            count=  10  compressed=807.39 MB

--- Single Read (no filter, 10 runs) ---
  Separate library:                                  avg=0.214s  min=0.177s  max=0.384s  stddev=0.064s
  Embedded library:                                  avg=0.189s  min=0.167s  max=0.272s  stddev=0.031s

--- Single Read (filtered ~1%, 10 runs) ---
    Query: publication_date in [2020-01-05 22:48:00, 2020-01-06 01:11:59.999999999)
  Column stats OFF:                                  avg=0.248s  min=0.218s  max=0.338s  stddev=0.035s
  Column stats ON:                                   avg=0.176s  min=0.157s  max=0.205s  stddev=0.014s
  Index-embedded, column stats OFF:                  avg=0.266s  min=0.239s  max=0.334s  stddev=0.030s
  Index-embedded, column stats ON:                   avg=0.186s  min=0.163s  max=0.214s  stddev=0.014s

--- Batch Read (M=50, no filter, 10 runs) ---
  Separate library:                                  avg=7.054s  min=5.222s  max=7.984s  stddev=0.791s  per_symbol=0.1411s
  Embedded library:                                  avg=7.553s  min=7.118s  max=7.901s  stddev=0.250s  per_symbol=0.1511s

  (generating DataFrame for write/delete benchmarks...)

--- Single Write (10 runs) ---
  No column stats:                                   avg=1.456s  min=1.140s  max=3.635s  stddev=0.768s
  Separate column stats (write + create_column_stats): avg=1.506s  min=1.381s  max=1.746s  stddev=0.128s
  Embedded column stats:                             avg=1.790s  min=1.678s  max=1.969s  stddev=0.080s

--- Batch Write (M=50, 10 runs) ---
  No column stats:                                   avg=22.997s  min=22.339s  max=24.187s  stddev=0.673s  per_symbol=0.4599s
  Separate column stats (write + create_column_stats): avg=37.262s  min=35.600s  max=39.450s  stddev=1.467s  per_symbol=0.7452s
  Embedded column stats:                             avg=24.467s  min=23.353s  max=25.664s  stddev=0.634s  per_symbol=0.4893s

--- Single Delete (10 runs, re-writes before each) ---
  Separate library:                                  avg=0.050s  min=0.041s  max=0.066s  stddev=0.007s
  Embedded library:                                  avg=0.073s  min=0.041s  max=0.193s  stddev=0.055s

--- Batch Delete (M=50, 10 runs) ---
  Separate library:                                  avg=1.115s  min=0.998s  max=1.249s  stddev=0.089s  per_symbol=0.0223s
  Embedded library:                                  avg=1.018s  min=0.855s  max=1.313s  stddev=0.153s  per_symbol=0.0204s

===============================================================
Backend: pure | Rows: 10,000,000 | Columns: 100 | Segments: 100
===============================================================

--- Key Sizes ---
  Separate library:
    TABLE_INDEX           count=   1  compressed=6.40 KB
    COLUMN_STATS          count=   1  compressed=3.24 KB
    TABLE_DATA            count= 100  compressed=8073.63 MB
  Embedded library:
    TABLE_INDEX           count=   1  compressed=181.58 KB
    COLUMN_STATS          count=   0  compressed=0 B
    TABLE_DATA            count= 100  compressed=8073.90 MB

--- Single Read (no filter, 10 runs) ---
  Separate library:                                  avg=2.020s  min=1.739s  max=2.731s  stddev=0.316s
  Embedded library:                                  avg=1.970s  min=1.694s  max=2.489s  stddev=0.210s

--- Single Read (filtered ~1%, 10 runs) ---
    Query: publication_date in [2020-02-19 12:00:00, 2020-02-20 12:00:00)
  Column stats OFF:                                  avg=1.805s  min=1.504s  max=2.460s  stddev=0.257s
  Column stats ON:                                   avg=0.224s  min=0.171s  max=0.324s  stddev=0.052s
  Index-embedded, column stats OFF:                  avg=1.760s  min=1.638s  max=1.878s  stddev=0.071s
  Index-embedded, column stats ON:                   avg=0.252s  min=0.189s  max=0.328s  stddev=0.048s

--- Batch Read: SKIPPED (ncols=100, nrows=10,000,000 exceeds batch limits) ---

  (generating DataFrame for write/delete benchmarks...)

--- Single Write (10 runs) ---
  No column stats:                                   avg=5.542s  min=5.107s  max=6.004s  stddev=0.356s
  Separate column stats (write + create_column_stats): avg=6.583s  min=6.056s  max=7.465s  stddev=0.397s
  Embedded column stats:                             avg=6.180s  min=5.948s  max=6.454s  stddev=0.139s

--- Batch Write: SKIPPED (ncols=100, nrows=10,000,000 exceeds batch limits) ---

--- Single Delete (10 runs, re-writes before each) ---
  Separate library:                                  avg=0.166s  min=0.124s  max=0.263s  stddev=0.047s
  Embedded library:                                  avg=0.159s  min=0.121s  max=0.269s  stddev=0.045s

--- Batch Delete: SKIPPED (ncols=100, nrows=10,000,000 exceeds batch limits) ---

