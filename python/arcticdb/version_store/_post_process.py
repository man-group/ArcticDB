
from arcticdb_ext.version_store import IndexRange as _IndexRange
from arcticdb_ext.version_store import RowRange as _RowRange
from arcticdb_ext.version_store import SignedRowRange as _SignedRowRange
from arcticdb.version_store._normalization import denormalize_user_metadata
def adapt_read_res(read_result, library_path, env, normalizer, custom_normalizer):
    frame_data = FrameData.from_cpp(read_result.frame_data)

    meta = denormalize_user_metadata(read_result.udm, normalizer)
    data = normalizer.denormalize(frame_data, read_result.norm)
    if read_result.norm.HasField("custom"):
        data = custom_normalizer.denormalize(data, read_result.norm.custom)

    return VersionedItem(
        symbol=read_result.version.symbol,
        library=library_path,
        data=data,
        version=read_result.version.version,
        metadata=meta,
        host=env
    )

def post_process_filters(read_result, read_query, query_builder):
    if read_query.row_filter is not None and (query_builder is None or query_builder.needs_post_processing()):
        # post filter
        start_idx = end_idx = None
        if isinstance(read_query.row_filter, _RowRange):
            start_idx = read_query.row_filter.start - read_result.frame_data.offset
            end_idx = read_query.row_filter.end - read_result.frame_data.offset
        elif isinstance(read_query.row_filter, _IndexRange):
            ts_idx = read_result.frame_data.value.data[0]
            if len(ts_idx) != 0:
                start_idx = ts_idx.searchsorted(datetime64(read_query.row_filter.start_ts, "ns"), side="left")
                end_idx = ts_idx.searchsorted(datetime64(read_query.row_filter.end_ts, "ns"), side="right")
        else:
            raise ArcticNativeException("Unrecognised row_filter type: {}".format(type(read_query.row_filter)))
        data = []
        for c in read_result.frame_data.value.data:
            data.append(c[start_idx:end_idx])
        read_result.frame_data = FrameData(data, read_result.frame_data.names, read_result.frame_data.index_columns)

