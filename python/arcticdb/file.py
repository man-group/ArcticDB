from typing import Any, Optional, Tuple
from arcticdb.version_store._store import VersionedItem

from arcticdb.version_store.read_result import ReadResult
from arcticdb_ext.version_store import read_dataframe_from_file, write_dataframe_to_file

from arcticdb.version_store._normalization import CompositeNormalizer, normalize_metadata, FrameData, denormalize_user_metadata


def _normalize_stateless(
        dataframe: Any,
        metadata: Any = None,
        *,
        pickle_on_failure: bool = False,
        dynamic_strings: bool = True,
        coerce_columns: Optional[Any] = None,
        dynamic_schema: bool = False,
        empty_types: bool = False,
        normalizer: Any,
        **kwargs,
) -> Tuple[Any, Any, Any]:
    udm = normalize_metadata(metadata)
    item, norm_meta = normalizer.normalize(
        dataframe,
        pickle_on_failure=pickle_on_failure,
        dynamic_strings=dynamic_strings,
        coerce_columns=coerce_columns,
        dynamic_schema=dynamic_schema,
        empty_types=empty_types,
        **kwargs,
    )

    return udm, item, norm_meta


def _denormalize_stateless(item: Any, norm_meta: Any, normalizer: Any) -> Any:
    return normalizer.denormalize(item, norm_meta)


def _to_file(symbol: str, data: Any, file_path: str, metadata: Optional[Any] = None, **kwargs) -> VersionedItem:
    """
    Write `data` (a DataFrame, Series or ndarray) to a file using the new C++ method.

    The function uses a stateless normalization function to convert the data
    into Arctic's native representation before writing, and calls the C++ binding:

         write_dataframe_to_file(stream_id, path, item, norm, user_meta)

    Parameters:
      symbol    : String identifier used as the stream_id.
      data      : The data to be written.
      file_path : Path to a file where the data is stored.
      metadata  : Optional metadata associated with the data.
      kwargs    : Additional options for normalization (e.g. pickle_on_failure, dynamic_strings).

    Returns a VersionedItem representing the written symbol.
    """
    normalizer = CompositeNormalizer()
    udm, item, norm_meta = _normalize_stateless(
        dataframe=data,
        metadata=metadata,
        pickle_on_failure=kwargs.get("pickle_on_failure", False),
        dynamic_strings=kwargs.get("dynamic_strings", True),
        coerce_columns=kwargs.get("coerce_columns"),
        dynamic_schema=kwargs.get("dynamic_schema", False),
        empty_types=kwargs.get("empty_types", False),
        normalizer=normalizer,
        norm_failure_options_msg="Error in to_file normalization",
        **kwargs,
    )
    write_dataframe_to_file(symbol, file_path, item, norm_meta, udm)
    return VersionedItem(
        symbol=symbol,
        library=file_path,
        data=None,
        version=0,
        metadata=metadata,
        host="file",
        timestamp=0,
    )


def _from_file(symbol: str, file_path: str, read_query: Optional[Any] = None, read_options: Optional[Any] = None,
              **kwargs) -> VersionedItem:
    """
    Read a dataframe from a file using the new C++ method.

    The C++ binding is:

         read_dataframe_from_file(stream_id, path, read_query, read_options)

    Parameters:
      symbol      : The stream identifier.
      file_path   : Path to the file from which to read.
      read_query  : An optional read query object (if needed).
      read_options: Optional read options; if not provided, defaults will be used.
      kwargs      : Additional keyword parameters (unused here).

    Returns a VersionedItem whose data attribute is filled with the denormalized Python object.
    """
    if read_options is None:
        from arcticdb_ext.version_store import PythonVersionStoreReadOptions
        read_options = PythonVersionStoreReadOptions()

    if read_query is None:
        from arcticdb_ext.version_store import PythonVersionStoreReadQuery
        read_query = PythonVersionStoreReadQuery()

    read_result = ReadResult(*read_dataframe_from_file(symbol, file_path, read_query, read_options))
    normalizer = CompositeNormalizer()
    frame_data = FrameData.from_cpp(read_result.frame_data)
    meta = denormalize_user_metadata(read_result.udm, normalizer)
    data = _denormalize_stateless(frame_data, read_result.norm, normalizer)
    return VersionedItem(
        symbol=symbol,
        library=file_path,
        data=data,
        version=read_result.version.version,
        metadata=meta,  #
        host="file",
        timestamp=read_result.version.timestamp,
    )
