# Parallel Writes

[As mentioned](../faq.md#how-does-arcticdb-handle-concurrent-writers), ArcticDB fundamentally does not support concurrent writers to a single symbol - *unless the data is concurrently written as staged data*!

Staged data is not available to read, and requires a process to finalize all staged data prior to it being available for reading. **Each unit of staged data must not overlap with any other unit of staged data** - 
as a result **staged data must be timeseries indexed**. The below code uses Spark to concurrently write to a single symbol in parallel, before finalizing the data:

``` py linenums="1"
import pyspark
import arcticdb as adb

# This example assumes the below variables (host, bucket, access, secret) are validly set
ac = adb.Arctic(f"s3://{HOST}:{BUCKET}?access={ACCESS}&secret={SECRET})

def _load(work):
    # This method is run in parallel via Spark.
    host, bucket, access, secret, symbol, library, file_path = work
    ac = adb.Arctic(f"s3://{host}:{bucket}?access={access}&secret={secret}")

    library = ac[library]

    df = pd.read_csv(file_path)
    df = df.set_index(df.columns[0])
    df.index = df.index.to_datetime()

    # When staged, the written data is not available to read until finalized.
    library.write(symbol, df, staged=True)

symbol = "my_data"
library = "my_library"

conf = SparkConf().setAppName('appName').setMaster('local')
sc = SparkContext(conf=conf)

# Assumes there are a set of CSV files in the current directory to load from
data = [(host, bucket, access, secret, symbol, library, f) for f in glob.glob("*.csv")]
dist_data = sc.parallelize(data)

if library not in ac.list_libraries():
    ac.create_library(library)

library = ac[library]

ret = dist_data.map(_load)
ret.collect()

library.finalize_staged_data(symbol)

data = library.read(symbol)
```