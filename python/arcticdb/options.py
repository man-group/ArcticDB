"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import Optional

from arcticdb.encoding_version import EncodingVersion


DEFAULT_ENCODING_VERSION = EncodingVersion.V1


class LibraryOptions:
    """
    Configuration options that can be applied when libraries are created.

    Attributes
    ----------

    dynamic_schema: bool
        See `__init__` for details.
    dedup: bool
        See `__init__` for details.
    rows_per_segment: int
        See `__init__` for details.
    columns_per_segment: int
        See `__init__` for details.
    """

    def __init__(
        self,
        *,
        dynamic_schema: bool = False,
        dedup: bool = False,
        rows_per_segment: int = 100_000,
        columns_per_segment: int = 127,
        encoding_version: Optional[EncodingVersion] = None,
    ):
        """
        Parameters
        ----------

        dynamic_schema: bool, default False
            Controls whether the library supports dynamically changing symbol schemas.

            The schema of a symbol refers to the order of the columns and the type of the columns.

            If False, then the schema for a symbol is set on each `write` call, and cannot then be
            modified by successive updates or appends. Each successive update or append must contain the same column set
            in the same order with the same types as the initial write.

            When disabled, ArcticDB will tile stored data across both the rows and columns. This enables highly efficient
            retrieval of specific columns regardless of the total number of columns stored in the symbol.

            If True, then updates and appends can contain columns not originally seen in the
            most recent write call. The data will be dynamically backfilled on read when required for the new columns.
            Furthermore, Arctic will support numeric type promotions should the type of a column change - for example,
            should column A be of type int32 on write, and of type float on the next append, the column will be returned as
            a float to Pandas on read. Supported promotions include (narrow) integer to (wider) integer, and integer to float.

            When enabled, ArcticDB will only tile across the rows of the data. This will result in slower column
            subsetting when storing a large number of columns (>1,000).

        dedup: bool, default False
            Controls whether calls to write and write_batch will attempt to deduplicate data segments against the
            previous live version of the specified symbol.

            If False, new data segments will always be written for the new version of the symbol being created.

            If True, the content hash, start index, and end index of data segments associated with the previous live
            version of this symbol will be compared with those about to be written, and will not be duplicated in the
            storage device if they match.

            Keep in mind that this is most effective when version n is equal to version n-1 plus additional data at the
            end - and only at the end! If there is additional data inserted at the start or into the the middle, then
            all segments occuring after that modification will almost certainly differ. ArcticDB creates new segments at
            fixed intervals and data is only de-duplicated if the hashes of the data segments are identical. A one row
            offset will therefore prevent this de-duplication.

            Note that these conditions will also be checked with write_pickle and write_pickle_batch. However, pickled
            objects are always written as a single data segment, and so dedup will only occur if the written object is
            identical to the previous version.

        rows_per_segment: int, default 100_000
            Together with columns_per_segment, controls how data being written, appended, or updated is sliced into
            separate data segment objects before being written to storage.

            By splitting data across multiple objects in storage, calls to read and read_batch that include the
            date_range and/or columns parameters can reduce the amount of data read from storage by only reading those
            data segments that contain data requested by the reader.

            For example, if writing a dataframe with 250,000 rows and 200 columns, by default, this will be sliced into
            6 data segments:
            1 - rows 1-100,000 and columns 1-127
            2 - rows 100,001-200,000 and columns 1-127
            3 - rows 200,001-250,000 and columns 1-127
            4 - rows 1-100,000 and columns 128-200
            5 - rows 100,001-200,000 and columns 128-200
            6 - rows 200,001-250,000 and columns 128-200

            Data segments that cover the same range of rows are said to belong to the same row-slice (e.g. segments 2
            and 5 in the example above). Data segments that cover the same range of columns are said to belong to the
            same column-slice (e.g. segments 2 and 3 in the example above).

            Note that this slicing is only applied to the new data being written, existing data segments from previous
            versions that can remain the same will not be modified. For example, if a 50,000 row dataframe with a single
            column is written, and then another dataframe also with 50,000 rows and one column is appended to it, there
            will still be two data segments each with 50,000 rows.

            Note that for libraries with dynamic_schema enabled, columns_per_segment does not apply, and there is always
            a single column-slice. However, rows_per_segment is used, and there will be multiple row-slices.

        columns_per_segment: int, default 127
            See rows_per_segment

        encoding_version: Optional[EncodingVersion], default None
            The encoding version to use when writing data to storage.
            v2 is faster, but still experimental, so use with caution.
        """
        self.dynamic_schema = dynamic_schema
        self.dedup = dedup
        self.rows_per_segment = rows_per_segment
        self.columns_per_segment = columns_per_segment
        self.encoding_version = encoding_version

    def __eq__(self, right):
        return (
            self.dynamic_schema == right.dynamic_schema
            and self.dedup == right.dedup
            and self.rows_per_segment == right.rows_per_segment
            and self.columns_per_segment == right.columns_per_segment
            and self.encoding_version == right.encoding_version
        )

    def __repr__(self):
        return (
            f"LibraryOptions(dynamic_schema={self.dynamic_schema}, dedup={self.dedup},"
            f" rows_per_segment={self.rows_per_segment}, columns_per_segment={self.columns_per_segment},"
            f" encoding_version={self.encoding_version if self.encoding_version is not None else 'Default'})"
        )
