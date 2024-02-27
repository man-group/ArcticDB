import re
from datetime import datetime

# Define a dictionary to store the data
data_dict = {}

# Open the file
with open("out.txt", "r") as file:
    for line in file:
        # Extract the timestamp, operation, key and data size (if present)
        match = re.match(
            r"(\d{8} \d{2}:\d{2}:\d{2}\.\d{6}) \d+ D arcticdb.storage \| (\w+).*\'(.*)\'(?:, with (\d+) bytes of data)?",
            line,
        )
        if match:
            timestamp, operation, key, data_size = match.groups()
            timestamp = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
            print(timestamp)
            # If the key is not in the dictionary, add it
            if key not in data_dict:
                data_dict[key] = {
                    "start_time": timestamp,
                    "end_time": None,
                    "data_written": 0,
                }
            print(timestamp, operation, key, data_size)
            # Update the end time and data written
            if operation in ["Wrote", "Returning"]:
                data_dict[key]["end_time"] = timestamp
            if operation in ["Set", "Wrote"]:
                data_dict[key]["data_written"] += int(data_size) if data_size else 0

sorted_keys = sorted(data_dict, key=lambda k: data_dict[k]["start_time"])

# Calculate the time taken and data written for each key
for key, value in data_dict.items():
    time_taken = (
        (value["end_time"] - value["start_time"]).microseconds
        if value["end_time"]
        else None
    )
    print(value["start_time"])
    print(value["end_time"])
    print(
        f'Key: {key}\nTime taken: {time_taken} seconds\nData written: {value["data_written"]} bytes\n'
    )
