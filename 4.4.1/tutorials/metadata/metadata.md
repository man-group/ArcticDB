# Metadata

ArcticDB enables you to store arbitrary binary-blobs alongside symbols and versions. The data is pickled when using the Python API. Note that there is a 4GB limit to the size of a single blob. 

The below example shows a basic example of writing and reading metadata (in this case a pickled Python dictionary):

```Python
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
