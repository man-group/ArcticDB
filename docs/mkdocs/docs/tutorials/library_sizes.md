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