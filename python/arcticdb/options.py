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

    def __init__(self, *, dynamic_schema: bool = False):
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
        """
        self.dynamic_schema = dynamic_schema

    def __repr__(self):
        return f"LibraryOptions(dynamic_schema={self.dynamic_schema})"
