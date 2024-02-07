# Snapshots

ArcticDB enables multi-symbol snapshotting. Snapshots enable multiple symbols to be versioned together via a human readable string name. 

In practise this is useful to tie derived data with the source data.

``` py linenums="1"
import arcticdb as adb
# This example assumes the below variables (host, bucket, access, secret) are validly set
ac = adb.Arctic(f"s3://{HOST}:{BUCKET}?access={ACCESS}&secret={SECRET})

library= "my_library"

if library not in ac.list_libraries():
    ac.create_library(library)

library = ac[library]

# Assumes there are CSV files containing pricing data and factor data. Each time we've written ALL new factor files
# to their symbol, we'll take a snapshot across all symbols.
for i, f in enumerate(sorted(glob.glob('*.csv'), key=lambda f: f.split('_')[1].split('.')[0])):
    df = pd.read_csv(f)
    if 'FACTORS' in f:
        library.write(f, df)
        # SNAP_{i} will forever point to all symbols that exist at this time at their current latest version
        library.snapshot(f"SNAP_{i}")
    else:
        df = df.set_index(df.columns[0])
        df.index = df.index.to_datetime()

        library.append(pricing_symbol, df, write_if_missing=True)

snapshots = library.list_snapshots()
symbols = library.list_symbols(snapshot_name=list(snapshots.keys())[0])
```

To generate this data, the following code can be used:

``` py linenums="1"
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def run(num_files, num_symbols):
    starting_date = datetime.today() - timedelta(weeks=num_files)
    starting_date = datetime(starting_date.year, starting_date.month, starting_date.day)

    for file_num in range(num_files):
        index_size = 7 * 24
        this_file_starting_date = starting_date + timedelta(weeks=file_num)

        df = pd.DataFrame(np.random.randint(0,index_size,size=(index_size, num_symbols)), columns=['SYM_%d' % i for i in range(num_symbols)])

        df.index = pd.date_range(this_file_starting_date, periods=index_size, freq="H")

        df.to_csv(f"PRICING_{this_file_starting_date}.csv")
        if file_num % 3 == 0:
            df = pd.DataFrame(np.random.randint(0, 5,size=(5, num_symbols)), columns=['SYM_%d' % i for i in range(num_symbols)])
            df['FACTORS'] = ['FACTOR_1', 'FACTOR_2', 'FACTOR_3', 'FACTOR_4', 'FACTOR_5']
            df.to_csv(f"FACTORS_{this_file_starting_date}.csv")

        print(f"Written {file_num + 1} / {num_files}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--num-files', type=int, default=15)
    parser.add_argument('--symbols-per-file', type=int, default=500)

    args = parser.parse_args()
    
    run(args.num_files, args.symbols_per_file)
```