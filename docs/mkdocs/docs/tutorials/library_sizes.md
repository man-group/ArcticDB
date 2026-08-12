# Library Sizes

ArcticDB includes some tools to analyze the amount of storage used by your libraries.

The API documentation for these features is [here](../api/admin_tools.md).

We break this down by internal key types, which are documented in `arcticdb.KeyType`. To get the total space used by
your library, just sum across the key types.

To use these tools, you can run code like,

```py
from arcticdb import Arctic, KeyType
from arcticdb.version_store.admin_tools import sum_sizes

lib = Arctic("<your URI>").get_library("<your library>")
admin_tools = lib.admin_tools()

sizes = admin_tools.get_sizes()  # scan all the sizes in the library, can be slow
sum_sizes(sizes.values())  # total size of the library
sizes[KeyType.TABLE_DATA].bytes_compressed  # how much storage is consumed by data segments?
sizes[KeyType.TABLE_DATA].count  # how many data segments are in your library?

by_symbol = admin_tools.get_sizes_by_symbol()  # scan all the sizes in the library, grouped by symbol
size_for_sym = by_symbol["sym"]
sum_sizes(size_for_sym.values())  # total size of the symbol
size_for_sym[KeyType.TABLE_INDEX].bytes_compressed  # how much storage is consumed by index structures?
size_for_sym[KeyType.TABLE_INDEX].count  # how many indexes does this symbol have?

for_symbol = admin_tools.get_sizes_for_symbol("<your symbol>")  # scan sizes for one particular symbol, faster than the APIs above
sum_sizes(for_symbol.values())  # total size of the symbol
for_symbol[KeyType.VERSION].bytes_compressed  # how much storage is consumed by our versioning metadata layer?
for_symbol[KeyType.VERSION].count  # how many version keys are in the library?
```

Most of the space used by a library should be in its `TABLE_DATA` keys since this is where your data is actually kept.
The other key types are metadata tracked by ArcticDB, primarily to index and version your data. More information about
our data layout is available [here](../technical/on_disk_storage.md).

## Choosing which key types to scan

`get_sizes` scans ten key types by default — the ones that hold your data and the metadata used to index and version
it. `arcticdb.KeyType` has more members than that: historical types no longer written, transient ones such as locks,
and types used only by enterprise replication and backups. Pass `key_types` to scan a different set:

```py
from arcticdb import KeyType

admin_tools.get_sizes(key_types=[KeyType.TABLE_DATA])  # just the data segments
admin_tools.get_sizes(key_types=list(KeyType))  # a complete breakdown
```

Every key type you ask for appears in the result, with a zero size and count when the library has none of them.

The cost grows with the number of key types you ask for, not just with the amount of data you have, and how steeply
depends on your storage:

- **S3 and NFS-backed libraries** take the sizes straight from a listing of each key type's prefix, without reading
  the objects. A key type your library has none of costs one round trip, so a complete breakdown is cheap but not
  free — on a storage where listing is slow it can still take noticeably longer than the default.
- **Every other backend**, LMDB and Azure included, has no such shortcut: it reads and decodes every object of each
  key type you ask about. `key_types=list(KeyType)` there means reading the entire library, `TABLE_DATA` and all.

Because the default set is where essentially all of your bytes live, prefer it unless you specifically need to account
for the remainder.
