# Versions

Arctic symbols are versioned. Versions are created for every storage change for the symbol, regardless of whether the
DataFrame content has changed.  Operations like `append`/`update`/`write`/`delete` which do change the content of the
DataFrame will create a new version of the symbol.  Some other operations like `defragment`, which are user methods that
write to storage but don't change the resulting DataFrame will also create new versions.  Finally out-of-band database
optimisation operations, such as those available in the enterprise toolkit, may also create new versions.

The primary reference to a specific version of a symbol is a positive integer and can accessed with the `as_of` parameter
available in methods such as `read`/`read_metadata`/`read_info`/`read_versions`.  The most recent version of a symbol is
default.  The version number is incremented by 1 for each new version of a symbol, even for symbol deletions.

## Reading and writing versions


``` py linenums="1"
import arcticdb as adb
# This example assumes the below variables (host, bucket, access, secret) are validly set
ac = adb.Arctic(f"s3://{HOST}:{BUCKET}?access={ACCESS}&secret={SECRET})



```

## Deleting versions

ArcticDB is designed to be a versioned database, as such, versions of the data are normally kept.  However, you can discard
versions, either as you go, individually, or by deleting the whole symbol.

``` py linenums="1"
```

## Discovering versions


## Using snapshots

See the [snaphots page](snapshots.md) and the [snapshots notebook](/notebooks/ArcticDB_demo_snapshots.md) for examples.