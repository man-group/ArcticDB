# Metadata

ArcticDB enables you to store arbitrary binary-blobs alongside symbols and versions.

The below example shows a basic example of writing and reading metadata (in this case a Python dictionary):

```python
import arcticdb as adb
# This example assumes the below variables (host, bucket, access, secret) are validly set
ac = adb.Arctic(f"s3://{HOST}:{BUCKET}?access={ACCESS}&secret={SECRET})
library = "my_library"

if library not in ac.list_libraries():
    ac.create_library(library)

library = ac[library]

metadata = {
    "This": "is",
    "a": "Python",
    "Dictionary": "!"
}

lib.write("meta", data=pd.DataFrame(), metadata=metadata)  # or write_metadata can be used - will still create a new version, but doesn't require `data` to be passed in

assert lib.read("meta").metadata == metadata
assert lib.read_metadata("meta").metadata == metadata  # Same as read, but doesn't return data from storage
```

New versions of symbols do not "inherit" the metadata of a previous version. Metadata needs to be specified explicitly
each time that you create a new version of the symbol:

```python
lib.write("new_sym", data=pd.DataFrame(), metadata=metadata)
lib.write("new_sym", data=pd.DataFrame())

assert lib.read("new_sym").metadata is None
assert lib.read("new_sym", as_of=0).metadata == metadata
```

### Serialization Format

We use `msgpack` serialization for metadata when possible. We support the built-in `msgpack` types and also:

- Pandas timestamps `pd.Timestamp`
- Python datetime `datetime.datetime`
- Python timedelta `datetime.timedelta`

Documentation of supported `msgpack` structures is available [here](https://github.com/msgpack/msgpack/blob/master/spec.md).
Arrays and maps correspond to Python lists and dicts.

When this `msgpack` serialization of the metadata fails due to unsupported types we fall back to pickling the metadata.
Pickling can have [serious downsides](https://nedbatchelder.com/blog/202006/pickles_nine_flaws.html) as it may not be possible to
unpickle data written with one set of library versions from a client with a different set of library versions.

Because of this, we log a warning when metadata gets pickled. You can disable the warning by setting an environment
variable `ARCTICDB_PickledMetadata_loglevel_str` to `DEBUG`. The log message looks like:

```
Pickling metadata - may not be readable by other clients
```

The metadata may be up to 4GB in size.

### Practical example - using metadata to track vendor timelines

One common example for metadata is to store the vendor-provided date alongside the version. For example, let's say we are processing three files - `data-2004-01-01.csv`, `data-2004-01-02.csv` and `data-2004-01-03.csv`. Each file name contains a date which we'd like to be able to store along side the version information in ArcticDB.

We can do this using the following code:

```python
import glob
import datetime
import arcticdb as adb

# This example assumes the below variables (host, bucket, access, secret) are validly set
ac = adb.Arctic(f"s3://{HOST}:{BUCKET}?access={ACCESS}&secret={SECRET})
library = "my_library"

if library not in ac.list_libraries():
    ac.create_library(library)

library = ac[library]

file_names = glob.glob('*.csv')  # returns ['data-2004-01-01.csv', 'data-2004-01-02.csv', 'data-2004-01-03.csv']

for i, name in enumerate(file_names):
    data = pd.read_csv('name')
    date = datetime.datetime.strptime(name[5:][:-4], '%Y-%m-%d')

    if i == 0:
        lib.write("symbol", data, metadata={"vendor_date": date})
    else:
        lib.append("symbol", data, metadata={"vendor_date": date})
```

We'll now use this to read the data along the vendor-provided timeline - that is, we'll retrieve the data as if we had written each file on the day it was generated. We'll read all metadata entries for all versions of the symbol and select the date that is most recent with respect to a given date (in this case 2004-01-02):

```python
# Continuing from the previous code snippet
metadata = [
    (v["version"], lib.read_metadata("symbol", as_of=v["version"]).metadata.get("vendor_date"))
    for v in lib.list_versions("symbol")
]
sorted_metadata = sorted(metadata, key=lambda x: x[1])

version_to_read_index = bisect_right([x[1] for x in sorted_metadata], datetime.datetime(2004, 1, 2))
lib.read("symbol", as_of=sorted_metadata[version_to_read_index - 1][0])
```

Note that if the data is written across multiple symbols, then ArcticDB Snapshots can be used to achieve the same result. 
