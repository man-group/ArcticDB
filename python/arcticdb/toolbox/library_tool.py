"""
Copyright 2023 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from typing import Optional, Union, List, Dict, Any
import pandas as pd

from arcticdb.version_store._normalization import FrameData
from arcticdb_ext.codec import decode_segment
from arcticdb_ext.storage import KeyType
from arcticdb_ext.stream import SegmentInMemory
from arcticdb_ext.tools import LibraryTool as LibraryToolImpl
from arcticdb_ext.version_store import AtomKey, PythonOutputFrame, RefKey
from arcticdb.version_store._normalization import denormalize_dataframe

VariantKey = Union[AtomKey, RefKey]

_KEY_PROPERTIES = {
    key_type: {k: v for k, v in vars(key_type).items() if isinstance(v, property)} for key_type in (AtomKey, RefKey)
}


def key_to_props_dict(key: VariantKey) -> Dict[str, Any]:
    return {k: v.fget(key) for k, v in _KEY_PROPERTIES[type(key)].items()}


def props_dict_to_atom_key(d: Dict[str, Any]) -> AtomKey:
    args = tuple(d[k] for k in _KEY_PROPERTIES[AtomKey])
    return AtomKey(*args)


class LibraryTool(LibraryToolImpl):
    @staticmethod
    def key_types() -> List[KeyType]:
        return list(KeyType.__members__.values())

    @staticmethod
    def dataframe_to_keys(
        df: pd.DataFrame, id: Union[str, int], filter_key_type: Optional[KeyType] = None
    ) -> List[AtomKey]:
        keys = []
        for index, row in df.iterrows():
            key_type = KeyType(row["key_type"])
            if filter_key_type is None or key_type == filter_key_type:
                keys.append(
                    AtomKey(
                        id,
                        int(row.version_id),
                        int(row.creation_ts),
                        int(row.content_hash),
                        int(index.timestamp()),
                        row.end_index.value,
                        key_type,
                    )
                )

        return keys

    def find_keys_for_symbol(self, key_type: KeyType, id: Union[str, int]) -> Union[List[AtomKey], List[RefKey]]:
        return self.find_keys_for_id(key_type, id)

    def read_to_segment_in_memory(self, key: VariantKey) -> SegmentInMemory:
        return decode_segment(self.read_to_segment(key))

    def read_to_dataframe(self, key: VariantKey) -> pd.DataFrame:
        """
        Reads the segment associated with the provided key into a Pandas DataFrame format. Any strings in the segment
        are replaced with Nones.

        Parameters
        ----------
        key : VariantKey
           The key in storage to read.

        Returns
        -------
        pandas.DataFrame
            Pandas DataFrame representing the information contained in the segment associated with the given key.

        Examples
        -------
        >>> lib.write("test_symbol", pd.DataFrame({'column': [1, 2]}, index=pd.date_range("2023-01-01", periods=2)))
        >>> lib_tool = lib.library_tool()
        >>> lib_tool.read_to_dataframe(lib_tool.find_keys(KeyType.VERSION)[0])
          start_index                     end_index  version_id stream_id          creation_ts         content_hash  index_type  key_type
        0  2023-01-01 2023-01-02 00:00:00.000000001           0      None  1681399019580103187  3563433649738173789          84         3
        """
        return denormalize_dataframe(self.read_to_read_result(key))

    def read_to_keys(
        self, key: VariantKey, id: Optional[Union[str, int]] = None, filter_key_type: Optional[KeyType] = None
    ) -> List[AtomKey]:
        """
        Reads the segment associated with the provided key into a Pandas DataFrame format, and then converts each row in
        this DataFrame into an AtomKey if all the necessary columns are present.

        Parameters
        ----------
        key : VariantKey
           The key in storage to read.

        id: Optional[Union[str, int]], default=None
            As string symbol names are not read into the DataFrame format, they cannot be processed automaticallyv from
            the stream_id column, and must be provided. If omitted, the id from the key argument will be used.

        filter_key_type: Optional[KeyType], default=None
            Only include keys in the returned list with the specified type. By default, all key types are included.

        Returns
        -------
        List[AtomKey]
            A list of the AtomKeys contained in the rows of the segment read from the provided key.

        Examples
        -------
        >>> lib.write("test_symbol", pd.DataFrame({'column': [1, 2]}, index=pd.date_range("2023-01-01", periods=2)))
        >>> lib_tool = lib.library_tool()
        >>> index_key = lib_tool.read_to_keys(lib_tool.find_keys(KeyType.VERSION)[0])[0]
        >>> index_key
        i:test_symbol:0:0x9f50cbe48ce5c223@1681399944743766831[1672531200000000000,1672617600000000001]
        >>> index_key.type
        KeyType.TABLE_INDEX
        >>> index_key.id
        test_symbol
        >>> index_key.version_id
        0
        >>> pd.Timestamp(index_key.creation_ts, unit="ns")
        2023-04-13 15:39:39.694442004
        >>> pd.Timestamp(index_key.start_index, unit="ns")
        2023-01-01 00:00:00
        >>> pd.Timestamp(index_key.end_index, unit="ns")
        2023-01-02 00:00:00.000000001
        >>> index_key.content_hash
        8243267225673136445
        """
        df = self.read_to_dataframe(key)
        return self.dataframe_to_keys(df, id if id is not None else key.id, filter_key_type)
