"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os


def read_big_list_of_naughty_strings():
    script_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = "{}/blns.txt".format(script_directory)

    with open(file_path, "r", errors="ignore") as file:
        lines = file.readlines()

    filtered_lines = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
    return filtered_lines
