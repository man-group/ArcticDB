#!/bin/bash

# Script: list_unique_tests.sh
# Description: Lists unique pytest test names (without parameterized fixture values)
#              for the given pytest -m marker expression(s).

if [ $# -eq 0 ]; then
    echo "Usage: $0 <pytest_mark_expression>"
    echo "Example: $0 \"pipeline and real_s3\""
else
    # Join all arguments into a single marker expression
    MARK_EXPR="$*"

    # Collect and deduplicate test names
    tests=$(pytest --co -q -m "$MARK_EXPR" \
        | sed 's/\[.*\]//' \
        | sort -u)

    # Print tests
    echo "$tests"

    # Count them
    count=$(echo "$tests" | grep -c '^')
    echo "Total unique tests: $count"
fi


