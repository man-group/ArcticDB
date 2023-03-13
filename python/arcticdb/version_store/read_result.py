"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


class ReadResult:
    def __init__(self, version, frame_data, norm, udm, mmeta, keys):
        self.version = version
        self.frame_data = frame_data
        self.norm = norm
        self.udm = udm
        self.mmeta = mmeta
        self.keys = keys
