from arcticdb import Arctic

from storage_common import *

if __name__ == "__main__":
    # TODO: Add support for other storages
    uri = get_real_s3_uri()
    BRANCH_NAME = os.getenv("ARCTICDB_PERSISTENT_STORAGE_BRANCH_NAME")

    ac = Arctic(uri)
    for lib in LIBRARIES:
        for lib_type in ["seed", "test"]:
            lib_name = f"{lib_type}_{BRANCH_NAME}_{lib}"
            lib_name = normalize_lib_name(lib_name)
            print(f"Deleting {lib_name}")
            ac.delete_library(lib_name)
