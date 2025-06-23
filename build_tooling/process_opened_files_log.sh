#!/bin/bash

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
