from arcticdb import Arctic
from arcticdb.version_store._store import NativeVersionStore

import argparse
import logging
import sys

root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)


def repair_library_if_necessary(ac, lib_name: str, run: bool) -> bool:
    """Returns True if library required repair."""
    storage_override = ac._library_adapter.get_storage_override()
    lib = NativeVersionStore(
        ac._library_manager.get_library(lib_name, storage_override),
        repr(ac._library_adapter),
        lib_cfg=ac._library_manager.get_library_config(lib_name, storage_override),
    )
    if ac._library_manager.is_library_config_ok(lib_name, throw_on_failure=False):
        return False
    else:
        logging.warning(f"Config NOT OK for [{lib_name}]")
        if run:
            logging.info(
                f"Updating library [{lib_name}]. You should rotate the credentials for write users on this storage"
                " account."
            )

            try:
                ac._library_manager.write_library_config(
                    lib._lib_cfg, lib_name, ac._library_adapter.get_masking_override()
                )
            except Exception:
                logging.exception(
                    "Unexpected error, exiting. Not updating remaining libraries. Please report on ArcticDB Github."
                )
                raise
            logging.info(f"Updated library [{lib_name}]")
        return True


def run(*, uri, run):
    logging.info(f"Update tool starting")
    ac = Arctic(uri)
    libraries = ac.list_libraries()
    logging.info(f"Found [{len(libraries)}] libraries in Arctic")
    touched_libs = 0
    for lib in libraries:
        touched_libs += repair_library_if_necessary(ac, lib, run)

    if run:
        if touched_libs:
            logging.info(f"[{touched_libs}] libraries updated successfully.")
        else:
            logging.info(f"No libraries required update.")

    elif touched_libs:
        logging.info(f"[{touched_libs}] libraries in total require update.")
    else:
        logging.info(f"No libraries require update.")

    logging.info("Finished")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Upgrade ArcticDB storage. This script checks for and performs (with "
            "`--run`) any data or config changes needed to support new versions of ArcticDB."
        )
    )
    parser.add_argument(
        "--uri",
        required=True,
        help="URI for Arctic instance to fix, see https://docs.arcticdb.io/api/arcticdb#arcticdb.Arctic.__init__",
    )
    parser.add_argument(
        "--run",
        default=False,
        action="store_true",
        help="Set to perform the storage update. This modifies the contents of your storage system.",
    )

    args = parser.parse_args()

    run(uri=args.uri, run=args.run)


if __name__ == "__main__":
    main()
