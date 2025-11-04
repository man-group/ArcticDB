"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb_ext.version_store import PandasOutputFrame
from arcticdb.version_store._normalization import FrameData


class NodeReadResult:
    def __init__(self, sym, frame_data, norm):
        self.sym = sym
        self.frame_data = (
            FrameData(*frame_data.extract_numpy_arrays()) if isinstance(frame_data, PandasOutputFrame) else frame_data
        )
        self.norm = norm


class ReadResult:
    def __init__(self, version, frame_data, norm, udm, mmeta, keys, node_read_results=None):
        self.version = version
        self.frame_data = (
            FrameData(*frame_data.extract_numpy_arrays()) if isinstance(frame_data, PandasOutputFrame) else frame_data
        )
        self.norm = norm
        self.udm = udm
        self.mmeta = mmeta
        self.keys = keys
        if node_read_results is not None:
            self.node_read_results = [
                NodeReadResult(*node_read_result) for node_read_result in node_read_results
            ]
        else:
            self.node_read_results = None
