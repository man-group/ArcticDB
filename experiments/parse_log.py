import re
from datetime import datetime
from collections import defaultdict

log_file = "out.txt"

key_strings = [
    "vref",
    "tdata",
    "tindex",
    "ver",
    "vj",
    "snap",
    "sl",
    "tall",
    "tomb",
    "cref",
    "cstats",
    "tref",
    "log",
    "logc",
    "off",
    "bref",
    "met",
    "aref",
    "mref",
    "lref",
    "ttomb",
    "app",
    "pref",
    "sref",
    "sg",
    "gen",
]

search_times = defaultdict(list)
with open(log_file, "r") as file:
    search_time = None
    search_bucket = None
    for line in file:
        if (
            "Searching for objects in bucket" in line
            or "Looking for" in line
            or "Set s3 key" in line
        ):
            try:
                temp = datetime.strptime(line.split()[1], "%H:%M:%S.%f")
                search_time = temp
            except Exception as e:
                print(line)
                print(e)
            search_bucket = re.search(r"(bucket|object|key) (.*)", line).group(2)
        elif (
            "Received object list" in line or "Returning" in line or "Wrote" in line
        ) and search_time is not None:
            receive_time = datetime.strptime(line.split()[1], "%H:%M:%S.%f")
            time_diff = receive_time - search_time
            search_times[search_bucket].append(time_diff.total_seconds())
            search_time = None
            search_bucket = None

new_search_times = {
    "vref": [],
    "tdata": [],
    "tindex": [],
    "ver": [],
    "vj": [],
    "snap": [],
    "sl": [],
    "tall": [],
    "tomb": [],
    "cref": [],
    "cstats": [],
    "tref": [],
    "log": [],
    "logc": [],
    "off": [],
    "bref": [],
    "met": [],
    "aref": [],
    "mref": [],
    "lref": [],
    "ttomb": [],
    "app": [],
    "pref": [],
    "sref": [],
    "sg": [],
    "gen": [],
}
for key in search_times:
    for key_string in new_search_times:
        if key_string in key:
            new_search_times[key_string].extend(search_times[key])
            break
    else:
        print(key)
        exit(0)
# print(new_search_times)

# Calculate average and total times
average_times = {
    bucket: sum(times) / len(times)
    for bucket, times in new_search_times.items()
    if len(times) > 0
}
total_times = {
    bucket: sum(times) for bucket, times in new_search_times.items() if len(times) > 0
}

# print(f"For file: {log_file}")
# print("Average times:")
# for key in average_times:
#     print(f"{key}: {average_times[key]}")

from tabulate import tabulate

table_data = []
total_time_reqs = 0
for key in total_times:
    total_time = round(total_times[key], 2)
    total_time_reqs += total_time
    items = len(new_search_times[key])
    avg_time = round(total_time / items, 2)
    max_time = round(max(new_search_times[key]), 2)
    min_time = round(min(new_search_times[key]), 4)
    # get 95th percentile
    new_search_times[key].sort()
    percentile_95 = new_search_times[key][int(0.95 * len(new_search_times[key]))]
    percentile_99 = new_search_times[key][int(0.99 * len(new_search_times[key]))]
    table_data.append(
        [
            key,
            total_time,
            items,
            avg_time,
            max_time,
            min_time,
            percentile_95,
            percentile_99,
        ]
    )

# Sort by total time
table_data.sort(key=lambda x: x[1], reverse=True)

print(f"Total time for all requests: {total_time_reqs:.2f} seconds")
print(
    tabulate(
        table_data,
        headers=[
            "Bucket",
            "Total Time",
            "Items",
            "Average Time",
            "Max Time",
            "Min Time",
            "95th percentile",
            "99th percentile",
        ],
    )
)
