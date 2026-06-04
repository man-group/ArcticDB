import pandas as pd
import pdb
import pyarrow as pa

# pd.options.future.infer_string = True

from arcticdb import Arctic

ac = Arctic("lmdb:///tmp/arcticdb_pandas3_strings")
lib = ac.get_library("test", create_if_missing=True)

df = pd.DataFrame({
    "my_strings": [f"s{i}" for i in range(10)],
    "my_strings2": [f"s2{i}" for i in range(10)],
    "my_ints": list(range(10)),
})

df_arrow = pa.Table.from_pandas(df)

print("Input dtypes:")
for col in df.columns:
    print(f"  {col}: {df[col].dtype!r}")
print(df)

print(f"num chunks: {df['my_strings2'].array._pa_array.num_chunks}")
lib.write("sym", df)
result = lib.read("sym").data

print("\nRoundtripped dtypes:")
for col in result.columns:
    print(f"  {col}: {result[col].dtype!r}")
print(result)
