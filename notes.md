# Stats Creation

- Memory hungry

https://github.com/man-group/ArcticDB/pull/3069 - rows_per_segment=2,

Write a dataframe of , 1_000 rows - split up in to 500 chunks (TABLE_DATA keys)

Stats row for each of those chunks => 500 row COLUMN_STATS

With normal row slicing, you would see a 500 row COLUMN_STATS segment for a,

500 * 100_000 row df => 50M rows


With what's going on - number of TABLE_DATA keys may or may not matter

### Experiments

- We can start with `lib.write` as a baseline measurement

- Interesting cases:

Rows: 1M rows, 10M rows, If necessary, 1B rows, 10B rows
Columns: 1K cols, 10K cols
Slicing: Stuff mentioned above, could try not standard row slicing
Data: Random float64 columns

- Plan:

For each of these cases:

- Time a write and memory of the write (larger ones may need a write and then several appends since the DF may not fit in memory)
- Time and memory use of column stats creation

Come up with a spreadsheet of these measurements

How to measure?

Memory - Python APIs to get the peak RSS of a process, or you can run the script under `/usr/bin/time -v` <= peak RSS

```create_data.py
For each of the experiments, write a symbol with the data for that experiment to LMDB
Include some small examples for testing

sym = f"{rows}_{cols}"
```

$ python create_data.py --rows 10 --cols 1000
$ python create_data.py --rows 1 --cols 1000

write a bash script to generate everything.


```run_bench.py
import psutil
process = psutil.Process()
print(f"Memory start: {process.memory_info().rss}")  # in bytes

ts_a = time.time()
# create stats
ts_b = time.time()
print(f"Time: {ts_b - ts_a}")

print(f"Memory end: {process.memory_info().rss}")  # in bytes
```

Write a bash script (or get Claude to) that runs run_bench.py over the different experiments (important to repeat measurements, so run a given example 20 times in a row, make sure you drop the stats between experiments)
and summarize the results for you.

Start with small examples to make sure the whole harness works.

After that look at massif and perf profiles
