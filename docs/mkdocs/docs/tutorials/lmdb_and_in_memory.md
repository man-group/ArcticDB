# LMDB and In-Memory storage backends

ArcticDB can use a file-based LMDB backend, or a RAM-based in-memory storage, as alternatives to object storage such as S3 and Azure.

Two simple examples are shown below.

Note that this example writes to `test.lmdb` in the working directory.

```py
from arcticdb import Arctic
import pandas as pd

ac_lmdb = Arctic('lmdb://test.lmdb')
lib = ac_lmdb.get_library('lib', create_if_missing=True)
lib.write('symbol', pd.DataFrame({'a': [1, 2, 3]})
print(lib.read('symbol').data)
```

which gives:

```py
   a
0  1
1  2
2  3
```

The equivalent instantiation for an in-memory store uses the URI `'mem://'` as in:

```py
ac_mem = Arctic('mem://')
```

The storage returned is owned by the `ac_mem` instance and lives for the lifetime of this Python object. Behind the
scenes, a [Folly Concurrent Hash Map](https://github.com/facebook/folly/blob/main/folly/concurrency/ConcurrentHashMap.h)
is used as the key/value store. For test cases and experimentation, the in-memory backend is a good option.

On the other hand, if the datasets are too large for RAM, or multiple processes want concurrent access to the same data
then LMDB is recommended.

## How to handle concurrent writers?

Again, it should be notes that ArcticDB achieves its highest performance and scale when configured with an object
storage backed (e.g. S3). Nevertheless, applications may want to concurrently write to LMDB stores. In-memory stores
are not appropriate for this use case.

The following Python code uses [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html)
to spawn 50 processes that concurrently write to different symbols. (Non-staged parallel writes to the same
symbol are [not supported](/tutorials/parallel_writes/)).

```py
# Code tested on Linux
from arcticdb import Arctic
import pandas as pd
from multiprocessing import Process, Queue
import numpy as np

lmdb_dir = 'data.lmdb'
num_processes = 50
data_KB = 1000
ncols = 10

nrows = int(data_KB * 1e3 / ncols / np.dtype(float).itemsize)

ac = Arctic(f'lmdb://{lmdb_dir}')
ac.create_library('lib')
lib = ac['lib']

timings = Queue()
def connect_and_write_symbol(symbol, lmdb_dir, timings_):
    ac = Arctic(f'lmdb://{lmdb_dir}')
    lib = ac['lib']
    start = pd.to_datetime('now')
    lib.write(
        symbol,
        pd.DataFrame(
            np.random.randn(nrows, ncols),
            columns=[f'c{i}' for i in range(ncols)]
        )
    )
    timings_.put((start, pd.to_datetime('now')))

symbol_names = {f'symbol_{i}' for i in range(num_processes)}
concurrent_writers = {
    Process(target=connect_and_write_symbol, args=(symbol, lmdb_dir, timings))
    for symbol in symbol_names
}

# Start all processes
for proc in concurrent_writers:
    proc.start()
# Wait for them to complete
for proc in concurrent_writers:
    proc.join()

assert set(lib.list_symbols()) == symbol_names

timings_list = []
while not timings.empty():
    timings_list.append(timings.get())

pd.DataFrame(timings_list, columns=['start', 'end']).to_csv('timings.csv')
```

Plotting the lifetimes of each process with [matplotlib](https://matplotlib.org/) we get:

![](/images/LMDBConcurrency.png)

Explanation of graph: Each line segment represents the execution of a process writing to the shared LMDB backend.
File locks are repeatedly obtained and released by LMDB throughout the calls to `lib.write(..)`.

The LMDB [documentation homepage](http://www.lmdb.tech/doc/index.html) states that multi-threaded concurrency is also
possible. However as explained [on this page](http://www.lmdb.tech/doc/starting.html) we should not call
`mdb_env_open()` multiple times from a single process. Hence, since this is called in the `Arctic` instantiation,
the above code could not be transferred to a multi-threaded application.

## Can I speed up LMDB?

One possible speed up is to use LMDB over [tmpfs](https://www.kernel.org/doc/html/latest/filesystems/tmpfs.html).

On Linux, the steps to setting this up are:

```bash
$ mkdir ./tmpfs_mount_point
$ sudo mount -t tmpfs -o size=1g tmpfs tmpfs_mount_point
```

And we can inspect that it is there with:

```
$ df -h
Filesystem               Size  Used Avail Use% Mounted on
tmpfs                    1.0G     0  1.0G   0% /somedir/tmpfs_mount_point
```

From ArcticDB you can connect to this filesystem with LMDB as you would usually.

### Is it any faster?

See the timing results below and the Appendix for the code to generate this data. The differences are not huge with
tmpfs out-performing disk only for symbol writing operations. Nevertheless, tmpfs is clearly a better option for an
ephemeral LMDB backend. We can also see that the in-memory store is significantly faster across the board as would
be expected.

![](/images/LMDBtmpfsDiskMemSpeeds.png)

Note: the ranges are 95% confidence intervals based on five repeats for each data size. The hardware limits are for
writing to disk (not tmpfs) using the `np.save` and `np.load` functions. No data was sought for the RAM's hardware
limit.

### Attempts to measure hardware limits using `fio`

Profiling hardware limits using `fio` rather than `numpy` is also possible, but varies wildly with the parameters used,
in particular the number of jobs (processes) to use. With the commands below, the results were too unreliable to
be of any use. The read and write numbers were both around 500 MB/s, but in increasing the `--numjobs` option,
these numbers could be inflated drastically to upwards of 10 GB/s.

Read test command:

```bash
fio --filename=./test.dat --size=2G --rw=read --bs=500M --ioengine=libaio --numjobs=1 --iodepth=1000 --name=sequential_reads --direct=0 --group_reporting
```

and for writing:

```bash
fio --filename=./test.dat --size=2G --rw=write --bs=4k --ioengine=libaio --numjobs=1 --iodepth=1000 --name=sequential_writes --direct=0 --group_reporting
```

For an explanation of the parameters see [the `fio` docs](https://fio.readthedocs.io/en/latest/fio_doc.html#threads-processes-and-job-synchronization).

Thoughts on using `fio` more effectively: the block size `--bs` parameter should match ArcticDB's segment size. E.g.
for 100,000 rows by 10 columns (as is the case here), then `--bs=8MB` is appropriate. For the `--size`, this should
match the total size of the data being written/read to/from the symbol. The `--numjobs` should probably be one, since
this clones separate processes which write to their own files and aggregates the total write speed and ArcticDB is limited
to one storage backend. For `--iodepth`, this should match the number of I/O threads that ArcticDB is configured
with:

```python
from arcticdb_ext import set_config_int
set_config_int("VersionStore.NumIOThreads", <number_threads>)
```

However, even with these options, the read speed is still reported by `fio` as around 500 MB/s which ArcticSB seems to
out-perform! More work needs to be done here in determining an appropriate hardware limit.

### Appendix: Profiling script

```python linenums="1"
# Scipt to profile LMDB on disk vs tmpfs, and compare
# also with the in-memory ArcticDB backend
from arcticdb import Arctic
import pandas as pd
import time
import numpy as np
import shutil, os

num_processes = 50
ncols = 10
nrepeats_per_data_point = 5

# Note that these are not deleted when the script finishes
disk_dir = 'disk.lmdb'
# Need to manually mount ./k as a tmpfs file system
tmpfs_dir = 'k/tmpfs.lmdb'
# Temporary file to gauge hardware speed limits
temp_numpy_file = 'temp.npy'
csv_out_file = 'tmpfs_vs_disk_timings.csv'

timings = {
    'Storage': [],
    'Load (bytes)': [],
    'Speed (megabytes/s)': []
}

for data_B in np.linspace(start=200e6, stop=1000e6, num=9, dtype=int):
    nrows = int(data_B / ncols / np.dtype(float).itemsize)
    array = np.random.randn(nrows, ncols)
    data = pd.DataFrame(array, columns=[f'c{i}' for i in range(ncols)])
    assert data.values.nbytes == data_B

    start = time.time()
    np.save(temp_numpy_file, array)
    elapsed = time.time() - start
    write_speed_MB_s = data_B / 1e6 / elapsed

    start = time.time()
    np.load(temp_numpy_file)
    elapsed = time.time() - start
    read_speed_MB_s = data_B / 1e6 / elapsed
    print(f'For {data_B}, Numpy Read speed {read_speed_MB_s} MB/s, write {write_speed_MB_s} MB/s')

    for _ in range(nrepeats_per_data_point):
        for test_dir in (disk_dir, tmpfs_dir, 'mem'):
            print(f'Timing {test_dir} with load {data_B} B')

            if test_dir == 'mem':
                ac = Arctic(f'mem://')
            else:
                if os.path.exists(test_dir):
                    # Free up space from last test
                    shutil.rmtree(test_dir)

                ac = Arctic(f'lmdb://{test_dir}')

            if 'lib' not in ac.list_libraries():
                ac.create_library('lib')

            lib = ac['lib']
            start = time.time()
            lib.write('symbol', data)
            elapsed = time.time() - start
            write_speed_MB_s = data_B / 1e6 / elapsed
            print('Time to write', elapsed, 's')

            start = time.time()
            lib.read('symbol')
            elapsed = time.time() - start
            read_speed_MB_s = data_B / 1e6 / elapsed
            print('Time to read', elapsed, 's')

            storage_name = {disk_dir: 'disk', tmpfs_dir: 'tmpfs', 'mem': 'mem'}[test_dir]
            # Record the writing speed
            timings['Load (bytes)'].append(data_B)
            timings['Storage'].append(storage_name + ' (write)')
            timings['Speed (megabytes/s)'].append(write_speed_MB_s)
            # Record the reading speed
            timings['Load (bytes)'].append(data_B)
            timings['Storage'].append(storage_name + ' (read)')
            timings['Speed (megabytes/s)'].append(read_speed_MB_s)

pd.DataFrame(timings).to_csv(csv_out_file)
```