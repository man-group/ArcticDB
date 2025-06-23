#!/bin/bash

LOGFILE="${1:-opened_files.log}"

PROCESS_NAME="pytest"

# Clear or create the log file
: > "$LOGFILE"

while true; do
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    count=$(lsof -c "$PROCESS_NAME" 2>/dev/null | wc -l)
    echo "$timestamp $count" >> "$LOGFILE"
    sleep 10
done

