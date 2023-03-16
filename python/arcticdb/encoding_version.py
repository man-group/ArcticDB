import enum


@enum.unique
class EncodingVersion(enum.IntEnum):
    V1 = 0
    V2 = 1
