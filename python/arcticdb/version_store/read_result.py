"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""


class ReadResult:
    def __init__(self, version, frame_data, norm, udm, mmeta, keys):
        self.version = version
        self.frame_data = frame_data
        self.norm = norm
        self.udm = udm
        self.mmeta = mmeta
        self.keys = keys
