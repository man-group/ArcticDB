# LMDB and In-Memory storage backends

As alternatives to object storage such as S3 and Azure, ArcticDB can use a file-based LMDB
backend, or a RAM-based in-memory storage.

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

From Python you can connect to this filesystem with LMDB as you would usually.

### Is it any faster?

See the timing results below. The differences are not massive, but clearly `tmpfs` is a good ephemeral LMDB store
solution.

![](/images/LMDBtmpfsDiskMemSpeeds.png)

Note: the ranges are roughly confidence intervals. Five repeats were made for each data size.
