#!/bin/bash

LOGFILE="${1:-opened_files.log}"

PROCESS_NAME="pytest"

# Clear or create the log file
: > "$LOGFILE"

while true; do
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    output="$timestamp"

    for pid in $(pgrep -f 'pytest|multiprocessing'); do
        count=$(lsof -p "$pid" 2>/dev/null | wc -l)
        output+=" $count"
    done

    echo -e "$output" >> "$LOGFILE"
    sleep 10
done

