#!/bin/bash

pushd "$(pwd)" >/dev/null

tooling_dir="$(dirname $BASH_SOURCE)"
cd $tooling_dir/../python

RE='(?:[\s]+|[\n]+)' 
FORBIDDEN_CONTENT=("import${RE}test" "from${RE}test")
SEARCH_DIR="arcticdb/"
ERROR_FOUND=0

for CONTENT in "${FORBIDDEN_CONTENT[@]}"; do
    MATCHES=$(grep -rlPz "$CONTENT" "$SEARCH_DIR") 
    if [ -n "$MATCHES" ]; then
        echo "ERROR: Forbidden package '$CONTENT' is imported!"
        echo "$MATCHES"  
        ERROR_FOUND=1
    fi
done

popd >/dev/null

if [ "$ERROR_FOUND" -eq 1 ]; then
    echo "ERRORS DETECTED!"
    exit 1
else
    echo "SUCCESS: No forbidden imports found."
fi
