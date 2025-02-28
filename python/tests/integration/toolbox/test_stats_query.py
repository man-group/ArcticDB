from arcticdb.toolbox.stats_query import StatsQueryTool

def test_query_stats(s3_version_store_v1):
    s3_version_store_v1.write("a", 1)
    query_stats_tools_start = StatsQueryTool()
    s3_version_store_v1.list_symbols()
    query_stats_tools_end = StatsQueryTool()
    stats = query_stats_tools_end - query_stats_tools_start
    """
    Expected output; time values are not deterministic
    arcticdb_call stage key_type     storage_op parallelized count  time_count_20  time_count_510
    0  list_streams  None     None           None         None  None              0               1
    1  list_streams  list     None           None         None  None              0               1
    2  list_streams  list        l  ListObjectsV2        False     1              2               0
    3  list_streams  list        r  ListObjectsV2        False     1              1               0
    """
    assert len(stats) == 4
    assert stats["count"].sum() == 2
    assert "parallelized" in stats.columns.to_list()