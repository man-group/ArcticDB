"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""


class LibraryOptions:
    """
    Configuration options that can be applied when libraries are created.

    Attributes
    ----------

    dynamic_schema: bool
        See `__init__` for details.
    """

    def __init__(
            self,
            *,
            dynamic_schema: bool = False,
            dedup: bool = False,
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

            Note that these conditions will also be checked with write_pickle and write_batch_pickle. However, pickled
            objects are always written as a single data segment, and so dedup will only occur if the written object is
            identical to the previous version.
        """
        self.dynamic_schema = dynamic_schema
        self.dedup = dedup

    def __repr__(self):
        return f"LibraryOptions(dynamic_schema={self.dynamic_schema}, dedup={self.dedup})"
