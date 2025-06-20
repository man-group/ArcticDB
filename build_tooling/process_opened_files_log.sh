#!/bin/bash

LOGFILE="opened_files.log"

if [[ -f "$LOGFILE" ]]; then
    max=$(awk '{print $3}' "$LOGFILE" | sort -nr | head -n 1)
    echo "Maximum open files during pytest run: $max"
else
    echo "Log file not found!"
fi
