#!/bin/bash

LOGFILE="${1:-opened_files.log}"
PROCESSESFILE="${2:-active_processes.log}"

PROCESS_NAME="pytest"

# Clear or create the log file
: > "$LOGFILE"
: > "$PROCESSESFILE"

while true; do

    # Monitor opened files 
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    output="$timestamp"

    for pid in $(pgrep -f 'pytest|multiprocessing'); do
        count=$(sudo ls  /proc/$pid/fd 2>/dev/null | wc -l)
        # try with sudo if count is 0
        if [ "$count" -eq 0 ]; then
            echo "Use fallback method"
            count=$(sudo lsof -p "$pid" 2>/dev/null | wc -l)
        fi        
        output+=" $count"
    done

    echo -e "$output" >> "$LOGFILE"

    # Processes dump to file
    echo $timestamp >> "$PROCESSESFILE"
    ps -ef >> "$PROCESSESFILE"
    echo "======================================" >> "$PROCESSESFILE"

    sleep 10
done

