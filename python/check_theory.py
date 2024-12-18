import pandas as pd
from arcticdb_ext.storage import KeyType
from arcticdb import Arctic

## Replace with your's
ac = Arctic("s3://172.17.0.2:9000:aseaton?access=3SePAqKdc1O7JgeDIJob&secret=zhtHzQtQt7UZJVUHk3QtpShSeRYZozwEl0pVeq8A")
lib = ac["tst"]
symbol = "staged"  # any of the symbols in the list_symbols output
##

lt = lib._dev_tools.library_tool()

append_keys = lt.find_keys(KeyType.APPEND_DATA)

sym_info = lib.get_description(symbol)

start, end = sym_info.date_range

all_ok = True
print("Starting checks")
for ak in append_keys:
    start_staged = pd.Timestamp(ak.start_index)
    end_staged = pd.Timestamp(ak.end_index)

    if start_staged < start or end_staged > end:
        print(f"Found incomplete {ak} that was not saved")
        all_ok = False

if all_ok:
    print(f"All staged segments were saved down")

# Use lib.delete_staged_data to remove any left over incompletes
