#!/bin/bash

# This file should be produced by monitor_opened_files.sh
# It will be processed and maximum number for each process will be
# displayed as result
LOGFILE="${1:-opened_files.log}"

for i in {3..20}; do
    if [[ -f "$LOGFILE" ]]; then
        AWK='{ print $'$i' }'
        max=$(awk "$AWK" "$LOGFILE" | sort -nr | head -n 1)
        echo "Maximum open files during pytest process $i run: $max"
    else
        echo "Log file not found!"
    fi

done
