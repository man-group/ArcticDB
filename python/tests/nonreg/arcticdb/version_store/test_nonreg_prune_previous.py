import gc, time
import pytest


@pytest.mark.skip(reason="Takes too much time. Unskip to test if the memory usage and speed of prunung are affected.")
def test_prune_previous_memory_usage(lmdb_version_store_very_big_map):
    lib = lmdb_version_store_very_big_map
    sym = "test_prune_previous_memory_usage"
    num_versions = 8000
    for idx in range(num_versions):
        lib.append(sym, pd.DataFrame({"col": np.arange(idx, idx + 1)}), write_if_missing=True)
    assert len(lib.list_versions(sym)) == num_versions
    gc.collect()
    start = time.time()
    lib.prune_previous_versions(sym)
    print(f"Prune took {time.time() - start}s")
    assert len(lib.list_versions(sym)) == 1
