# ArcticDB Fundamentals

This tutorial will walk through the fundamentals of ArcticDB:

1. Accessing libraries
2. Writing data
3. Reading data
4. Modifying data

To start, let's import `arcticdb`:

```python
import arcticdb as adb
```

## Accessing libraries

Connect to your storage:

```python
# Connect using defined keys
ac = adb.Arctic('s3s://s3.eu-west-2.amazonaws.com:arctic-test-aws?access=<access key>&secret=<secret key>')
# Leave AWS SDK to work out auth details 
ac = adb.Arctic('s3s://s3.eu-west-2.amazonaws.com:arctic-test-aws?aws_auth=true)
```

For more information on how the AWS SDK configures authentication without utilising defined keys, please see the [AWS documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

Access a library using either the `[library_name]` notation or the `get_library` method:

```python
lib = ac['library']
# ...equivalent to...
lib = ac.get_library['library']
```

Let's see what data is already present:

```python
>>> lib.list_symbols()
['sym_2', 'sym_1', 'symbol']
```

## ArcticDB API

ArcticDB's API is built around four main primitives that each operate over a single symbol.

1. *write*: Creates a new version consisting solely of the item passed in.
2. *append*: Creates a new version consisting of the item appended to the previously-written data.
3. *update*: Creates a new version consisting the previous data patched with the provided item.
4. *read*: Retrieves the given version (if no version is provided the latest version is used).

These primitives aren't exhaustive, but cover most use cases. We'll show the usage of these primitives.

## Writing data

Let's start by generating some data. The below snippet generates some random data with a datetime index:

```python
import numpy as np
import pandas as pd
NUM_COLUMNS=10
NUM_ROWS=100_000
df = pd.DataFrame(np.random.randint(0,100,size=(NUM_ROWS, NUM_COLUMNS)), columns=[f"COL_{i}" for i in range(NUM_COLUMNS)], index=pd.date_range('2000', periods=NUM_ROWS, freq='h'))
```

Let's take this data and write it to ArcticDB:

```python
>>> lib.write("my_data", df)
VersionedItem(symbol=my_data,library=test_fundamentals_1,data=<class 'NoneType'>,version=0,metadata=None,host=local)
```

## Reading data

To read the data, simply use the `read` primitive:

```python
>>> data = lib.read("my_data")
>>> data
VersionedItem(symbol=my_data,library=test_fundamentals_1,data=<class 'pandas.core.frame.DataFrame'>,version=0,metadata=None,host=local)
```

Note that you get back a `VersionedItem` - it allows us to retrieve the version of the written data:

```python
>>> data.version
0
```

As this is the first write to this symbol, the version is `0`. To retrieve the data:

```python
>>> data.data
```

## Slicing and filtering

See [the getting started guide for more information](../index.md#slicing-and-filtering).

## Modifying data

Let's append some data. First, note that the data we've written ends in 2011:

```python
>>> data.data.tail()
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  COL_8  COL_9
2011-05-29 11:00:00     44     94     70     32     91      4     35     19     74     53
2011-05-29 12:00:00     79     51     67     48      8     83     46     54     86     38
2011-05-29 13:00:00     60     98     74      4     81     86     64     78     13     32
2011-05-29 14:00:00     27     24     16      6     84     99     11     94     29      4
2011-05-29 15:00:00     81     76     52     93     31     91     64      2     26     78
```

That's simply because the data we generated started on January 1st, 2000 at 00:00 at consists of 100,000 rows, incrementing one hour at a time. When `append`-ing data, the data you are appending must begin 
after the existing data ends. As a result, let's generate some data that begins in 2012:

```python
df_to_append = pd.DataFrame(np.random.randint(0,100,size=(NUM_ROWS, NUM_COLUMNS)), columns=[f"COL_{i}" for i in range(NUM_COLUMNS)], index=pd.date_range('2012', periods=NUM_ROWS, freq='h'))
```

Now let's append!

```python
>>> lib.append("my_data", df_to_append)
VersionedItem(symbol=my_data,library=test_fundamentals_1,data=<class 'NoneType'>,version=1,metadata=None,host=local)
```

`append` has created a new version of the data. When reading version 0, the data will end in 2011. When reading version 1, the data will end in 2023.

### *Update*

If *append* can only append date that begins after the existing data ends, then it begs the question - how do we mutate data?

The answer is that we use the `update` primitive. `update` overwrites (creating a new version - nothing is lost!) existing symbol data with the data that is passed in. 
Note that the entire range between the first and last index entry in the existing data is replaced in its entirety with the data that is passed in, adding additional index entries if
required. This means `update` is a contiguous operation - see the documentation of `update` for more information.

### Time travel!

ArcticDB is bitemporal - all new versions are timestamped! Let's pull in the first version of the data, prior to the `append`:

```python
>>> lib.read("my_date", as_of=0).data.tail()
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  COL_8  COL_9
2011-05-29 11:00:00     44     94     70     32     91      4     35     19     74     53
2011-05-29 12:00:00     79     51     67     48      8     83     46     54     86     38
2011-05-29 13:00:00     60     98     74      4     81     86     64     78     13     32
2011-05-29 14:00:00     27     24     16      6     84     99     11     94     29      4
2011-05-29 15:00:00     81     76     52     93     31     91     64      2     26     78
```

Note that it ends in 2011 - it's like the `append` never happened. `as_of` can take a timestamp (`datatime.datetime` or `pd.Timestamp`) as well.
