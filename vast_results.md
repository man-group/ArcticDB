We test against two libraries with different column stats structures,

"separate library" means what we do now - write the COLUMN_STATS in their own key type.

"embedded library" means folding column stats in to the TABLE_INDEX key.

We create sample data with 1M and 10M rows, and 100 columns. The data is just random float64s, plus a single
"publication_date" column that's monotonically increasing.

We run 7 benchmarks, each against both libraries and with column stats enabled and disabled:

- Read a single symbol with no query builder
- Read a single symbol, filtering publication_date down to 1% of its values
- Batch read 50 symbols, with no query builder
- Write the dataframe
- Batch write 50 copies of the dataframe
- Delete a single symbol
- Delete a batch of 50 symbols

The write timings with stats enabled include the time taken to call `create_column_stats` on the "separate" library.

Running with thread counts set to 16 CPU and 24 IO - simulating a fairly small machine where extra IO round trips
are not just parallelized away.

These results (and the prototype) are a bit rough but they do show any worrying performance impact
from keeping the COLUMN_STATS in their own key.

The write impact of column stats in the separate library is exaggerated here because we could change to write
stats in parallel with writing the index key.

We stop at 100 columns because the prototype crashes with more than one column slice!

Other advantages of the separate key:

- No need to make TABLE_INDEX mutable
- No backwards compat concerns with the new TABLE_INDEX format
- No penalty for readers doing reads without `QueryBuilder` filters who cannot benefit from the stats
- Easy to create stats for existing data (including old versions of existing data)

Conclusion:

- Keep stats in their own key type

Sample data created with `create_benchmark_data_index.py`

```
$ python ../create_benchmark_data_index.py --backends vast --sizes medium large --columns 100
```

Then benchmarks run with:

```
(main) aseaton@dlonapatcs502:~/source/ArcticDB/python DEV $ python ../run_benchmark_index.py --backends vast --sizes medium large --columns 100
=============================================================
Backend: vast | Rows: 1,000,000 | Columns: 100 | Segments: 10
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
  Separate library:                                  avg=0.236s  min=0.197s  max=0.360s  stddev=0.047s
  Embedded library:                                  avg=0.196s  min=0.163s  max=0.269s  stddev=0.034s
--- Single Read (filtered ~1%, 10 runs) ---
    Query: publication_date in [2020-01-05 22:48:00, 2020-01-06 01:11:59.999999999)
  Column stats OFF:                                  avg=0.244s  min=0.216s  max=0.276s  stddev=0.019s
  Column stats ON:                                   avg=0.176s  min=0.160s  max=0.196s  stddev=0.011s
  Index-embedded, column stats OFF:                  avg=0.232s  min=0.209s  max=0.261s  stddev=0.014s
  Index-embedded, column stats ON:                   avg=0.171s  min=0.153s  max=0.211s  stddev=0.020s
--- Batch Read (M=50, no filter, 10 runs) ---
  Separate library:                                  avg=6.420s  min=4.869s  max=6.940s  stddev=0.604s  per_symbol=0.1284s
  Embedded library:                                  avg=6.956s  min=6.114s  max=7.610s  stddev=0.517s  per_symbol=0.1391s
  (generating DataFrame for write/delete benchmarks...)
--- Single Write (10 runs) ---
  No column stats:                                   avg=6.048s  min=0.828s  max=50.565s  stddev=15.654s
  Separate column stats (write + create_column_stats): avg=1.934s  min=0.983s  max=9.168s  stddev=2.543s
  Embedded column stats:                             avg=7.954s  min=1.635s  max=44.704s  stddev=13.442s
--- Batch Write (M=50, 10 runs) ---
  No column stats:                                   avg=50.763s  min=19.142s  max=70.931s  stddev=21.218s  per_symbol=1.0153s
  Separate column stats (write + create_column_stats): avg=74.703s  min=35.503s  max=129.559s  stddev=25.939s  per_symbol=1.4941s
  Embedded column stats:                             avg=44.740s  min=24.475s  max=69.197s  stddev=20.124s  per_symbol=0.8948s
--- Single Delete (10 runs, re-writes before each) ---
  Separate library:                                  avg=0.411s  min=0.224s  max=1.299s  stddev=0.333s
  Embedded library:                                  avg=0.153s  min=0.108s  max=0.205s  stddev=0.030s
--- Batch Delete (M=50, 10 runs) ---
  Separate library:                                  avg=6.103s  min=4.259s  max=8.110s  stddev=1.231s  per_symbol=0.1221s
  Embedded library:                                  avg=7.886s  min=4.205s  max=11.305s  stddev=2.541s  per_symbol=0.1577s
===============================================================
Backend: vast | Rows: 10,000,000 | Columns: 100 | Segments: 100
===============================================================
--- Key Sizes ---
  Separate library:
    TABLE_INDEX           count=   1  compressed=6.40 KB
    COLUMN_STATS          count=   1  compressed=3.24 KB
    TABLE_DATA            count= 100  compressed=8073.63 MB
  Embedded library:
    TABLE_INDEX           count=   1  compressed=181.57 KB
    COLUMN_STATS          count=   0  compressed=0 B
    TABLE_DATA            count= 100  compressed=8073.90 MB
--- Single Read (no filter, 10 runs) ---
  Separate library:                                  avg=1.860s  min=1.651s  max=2.341s  stddev=0.228s
  Embedded library:                                  avg=1.901s  min=1.700s  max=2.036s  stddev=0.103s
--- Single Read (filtered ~1%, 10 runs) ---
    Query: publication_date in [2020-02-19 12:00:00, 2020-02-20 12:00:00)
  Column stats OFF:                                  avg=1.716s  min=1.611s  max=1.860s  stddev=0.084s
  Column stats ON:                                   avg=0.301s  min=0.239s  max=0.379s  stddev=0.044s
  Index-embedded, column stats OFF:                  avg=1.698s  min=1.590s  max=1.775s  stddev=0.062s
  Index-embedded, column stats ON:                   avg=0.237s  min=0.191s  max=0.276s  stddev=0.031s
--- Batch Read: SKIPPED (ncols=100, nrows=10,000,000 exceeds batch limits) ---
  (generating DataFrame for write/delete benchmarks...)
--- Single Write (10 runs) ---
  No column stats:                                   avg=32.378s  min=3.040s  max=64.316s  stddev=28.785s
  Separate column stats (write + create_column_stats): avg=15.513s  min=5.073s  max=63.529s  stddev=17.510s
  Embedded column stats:                             avg=15.995s  min=4.319s  max=64.225s  stddev=17.650s
--- Batch Write: SKIPPED (ncols=100, nrows=10,000,000 exceeds batch limits) ---
--- Single Delete (10 runs, re-writes before each) ---
  Separate library:                                  avg=2.019s  min=0.852s  max=3.497s  stddev=1.059s
  Embedded library:                                  avg=1.190s  min=0.587s  max=2.911s  stddev=0.860s
--- Batch Delete: SKIPPED (ncols=100, nrows=10,000,000 exceeds batch limits) ---
```