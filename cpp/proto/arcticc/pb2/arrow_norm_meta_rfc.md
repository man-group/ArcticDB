# Arrow normalization metadata

## Arrow specific metadata

### Per column

Each arrow column will have several properties:
- Timezone (e.g. None, London, New_York)
- String format (e.g. LARGE_STRING, CATEGORICAL)

This dictates a straightforward:
```
message ArrowArray {
    string tz = 1;            // It makes sense to reuse the Pandas format
    string string_format = 2; // A `oneof` or `uint8` might be more efficient but I feel like string is more future proof without the need to further modify protobufs. 
}
```

### Arrow Table vs Array primitives

A single arrow array can be represented as a single `ArrowArray` message. 
It is also fundamentally unindexed (a timeseries index would require a second column). 

A table is a collection of columns, so we'll use a dictionary from column name to `ArrowArray` metadata.
An alternative would be to index by column index which would be more efficient but would make the problem of merging dynamic schema normalization metadatas harder.

```
message ArrowTable {
    bool has_index = 1;
    map<string, ArrowArray> columns = 2;
}
```

## Arrow -> Pandas

Here we discuss the basic flow:
```
lib.write(arrow)
lib.read(output_format="pandas")
```

- Arrow arrays -> Pandas Series with RangeIndex
- Arrow table without index -> Pandas DataFrame with RangeIndex
- Arrow table with index -> Pandas DataFrame with TimeseriesIndex
- string format metadata is lost
- timezone metadata is non-trivial
  - Option A: Apply timezone metadata only to index column. Gives a straight translation from arrow to pandas metadata
  - Option B: Apply all timezone columns. Will require pandas denoramalization code to work directly with Arrow metadata
  - I am prefering option B as it is more correct and less surprising for users.

## Pandas -> Arrow

Here we discuss the basic flow:
```
lib.write(pandas)
lib.read(output_format="polars")
```

- Pandas Series with RangeIndex -> Arrow array
- Pandas Series with other index -> Arrow Table
- Pandas DataFrame -> Arrow Table

## Combining pandas with arrow via append/update or concat

Here we discuss flows like:
```
lib.write(arrow)
lib.append(pandas)
lib.read()
```
or
```
lib.write(arrow)
lib.write(pandas)
lib.read_batch_concat([arrow, pandas])
```

### How should we handle index mismatch?
- Pandas RangeIndex is compatible only with `has_index=False`
- Pandas TimeseriesIndex is compabitible only with `has_index=True`
- Pandas MultiIndex is compatible only with `has_index=True`

### How should we handle unnamed indices and duplicate columns?
We have specified a conversion from unnamed pandas indices to arrow on read. However making append work with that convention will be challenging:
```
lib.write(pandas_df_with_multiindex) # Multindex level names [None, None], column name "col"
lib.append(arrow_table)              # Column names ["__index_level_0__", "__index_level_1__", "col"]
```
The above example will be challenging to make work because the two `StreamDescriptor` would be different.

This becomes even more complicated if we consider duplicated columns and dynamic schema.
```
lib.write(pandas_df)     # Index [None], columns ["index"] 
lib.append(arrow_table)  # Columns: ["_index_", "index", "__index__"], "__index__" is a new column.
lib.append(pandas_df)    # Index [None], columns ["index", "__index___", "_index_"], which column corresponds to which?
```
In the above it is very unclear which column should be considered the new one in the last append.

I'm in favor with the first version of this to:
- Gracefully handle unnamed indices without duplicates. I.e. allow combining unnamed pandas index or multiindex with an arrow table which has the correct column names
- Raise when trying to combine a pandas dataframe with duplicate column names with arrow. If people care about this we can fix in the future.


### How should we handle timezone mismatch?
- [Existing pandas behavior](https://github.com/man-group/ArcticDB/blob/b9a8b861c950377c15104bd135c7635a827dc673/python/tests/unit/arcticdb/version_store/test_symbol_concatenation.py#L659-L660) is to allow any mismatch and clear the timezone on the result
- I'm personally in favor of raising an exception always whenever timezones are different (specified or not), but that will need to be future 7.0.0 change. So, for now we'll allow combining any timezone with any other timezone.

### How should we store a mix of pandas specific metadata and arrow specific metadata?

There are pandas specifics like `PandasIndex` or `PandasMultiindex` messages. There are also arrow specifics like `StringFormat`. This means that a combination of pandas and arrow will require keeping a combination of arrow specific and pandas specific metadata.

#### Option A: Store the entire PandasDataFrame metadata in the Arrow metadata
```
message ArrowSeries {
    ArrowArray arrow_meta = 1;
    optional PandasDataFrame pandas_meta = 2; # only range index is allowed here
}

message ArrowTable {
    bool has_index = 1;
    map<string, ArrowArray> columns = 2;
    optional PandasDataFrame pandas_meta = 3;
}
```

Downsides:
- Allows for inconsistent state (e.g. arrow timezone vs pandas index timezone)
- Persists the pandas normalization metadata complexity forever

Upsides:
- Easy to incorporate with existing denormalization code for pandas

#### Option B: Store only relevant pandas metadata in the Arrow metadata

```
message PandasSpecificMetadata {
    # Have not thought about the precise contents in depth but should have something like:
    # - Index metadata without timezones
    # - Column name metadata (e.g. is_none)
}


message ArrowTable {
    bool has_index = 1;
    map<string, ArrowArray> columns = 2;
    PandasSpecificMetadata pandas_meta = 3;
}
```

Downsides:
- Requires a big rewrite of pandas normalization metadta handling
- We'll forever need to support 2 different format for pandas metadata

Upsides:
- All states are valid - no incosistencies or duplication
- Allows the creation of a new cleaner format for pandas normalization metadata.
- Some time in the future we'll be able to deprecate the old pandas normalization metadata which is quite messy

#### Option C: Incorporate the arrow metadata inside Pandas metadata

```
message PandasDataFrame {
    Pandas common = 1;
    PandasMultiColumn multi_columns = 2;
    bool has_synthetic_columns = 3;
    map<string, ArrowArray> arrow_meta = 4;
}
```
and we can maybe rename `PandasDataFrame` to `DataFrame`

Downsides:
- Risky, requires touching pandas normalization metadata protobuf, although just adding a field should be fine
- Also allows for inconsistent state
- Preserves the messy PandasDataFrame metadata even for arrow only flows

Upsides:
- Can use the same normalization metadata format for both Pandas and Arrow
- Arrow tables written with new arcticdb will be readable by older clients

#### Ivo's take

I'd go with option A.

I don't like option B mainly because it adds extra complexity on the on storage structure.

I don't like option C mainly because arrow only workflows will keep dragging pandas metadata forever.

I have made the suggested changes to `descriptors.proto` to more easily see them in context.