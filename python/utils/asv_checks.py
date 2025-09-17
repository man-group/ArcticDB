from datetime import datetime
import hashlib
import logging
import os
import re
import subprocess
import sys
from typing import List


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ASV Linter")


def error(mes):
    logger.error("-" * 80)
    logger.error(f"ERROR :{mes}")
    logger.error("-" * 80)


def run_command(command: List[str], cwd: str, ok_errors_list: List[str] = None) -> int:
    """
    executes a command in specified directory.
    if 'ok_error' passed, the string will be searched in stderr
    and if found will not mark execution as error but count as ok
    """
    result = subprocess.run(command, capture_output=True, text=True, cwd=cwd)

    output = result.stdout
    err_output = result.stderr
    error_code = result.returncode

    if err_output is not None:
        error(err_output)
        if error_code == 0:
            logger.info("ABOVE ERRORS DO NOT AFFECT FINAL ERROR CODE = 0")

    if output is not None:
        logger.info("Standard Output:")
        logger.info(output)

    if error_code != 0:
        logger.error(f"Error Code Returned: {error_code}")
        if ok_errors_list is not None:
            for ok_error in ok_errors_list:
                err_output.replace(ok_error, "")
                err_output = err_output.strip()
                if err_output == "":
                    error_code = 0
                    break
            else:
                error(f"Unknown errors: {err_output}")

    return error_code


def compute_file_hash(file_path):
    """Compute the SHA-256 hash of the given file."""
    logger.info(f"Calculating has of file: {file_path}")
    log_file_info(file_path)
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


def log_file_info(file_path):
    if os.path.exists(file_path):
        file_stat = os.stat(file_path)

        attributes = {
            "Size": file_stat.st_size,
            "Permissions": oct(file_stat.st_mode)[-3:],  # File permissions
            "Owner UID": file_stat.st_uid,
            "Group GID": file_stat.st_gid,
            "Last Accessed": datetime.fromtimestamp(file_stat.st_atime),
            "Last Modified": datetime.fromtimestamp(file_stat.st_mtime),
            "Created": datetime.fromtimestamp(file_stat.st_ctime),
        }

        attrs = ""
        for key, value in attributes.items():
            attrs += f"{key}: {value}\n"
        logger.info(f"File attributes '{file_path}' \n{attrs}")
    else:
        logger.warning(f"File '{file_path}' \n does not exist.")


def file_unchanged(filepath, last_check_time):
    try:
        modified_time = os.path.getmtime(filepath)
        return modified_time == last_check_time
    except FileNotFoundError:
        return False


def get_project_root():
    file_location = os.path.abspath(__file__)
    return file_location.split("/python/utils")[0]


def perform_asv_checks() -> int:
    """
    Performs several checks and returns result 0 if no problem or 1 if there are problems
    """

    err = 0

    path = get_project_root()

    sys.path.insert(0, f"{path}/python")
    sys.path.insert(0, f"{path}/python/tests")

    benchmark_config = f"{path}/python/.asv/results/benchmarks.json"
    orig_hash = compute_file_hash(benchmark_config)

    logger.info("_" * 80)
    logger.info(
        """IMPORTANT: The tool checks CURRENT ACTUAL versions of asv benchmark tests along with the one in benchmarks.json file.
            That means that if there are files that are not submitted yet (tests and benchmark.json),
            they would need to be in order for completion of current PR.
            benchmarks.json is updated with a version number calculated as a hash
            of the python test method. Thus any change of this method triggers different
            version. Hence you would need to update json file also.
            It happens automatically if you run following commandline:
             > asv run --bench just-discover --python=same  """
    )
    logger.info("_" * 80)

    logger.info("\n\nCheck 1: Executing check for python code of asv tests")
    if run_command(["asv", "check", "--python=same"], path) != 0:
        error("Please address all reported errors and submit code in the PR.")
        err = 1
    else:
        logger.info("Relax, no worries. Code is fine!")

    logger.info("\n\nCheck 2: Check that benchmarks.json has up to date latest versions of tests.")
    if (
        run_command(
            command=["asv", "run", "--bench", "just-discover", "--python=same"],
            cwd=path,
            ok_errors_list=["Couldn't load asv.plugins._mamba_helpers"],
        )
        != 0
    ):
        error("There was error getting latest benchmarks. See log")
        err = 1
    else:
        new_hash = compute_file_hash(benchmark_config)
        if new_hash == orig_hash:
            logger.info("Great, there are no new versions of asv test either!")
        else:
            logger.warning(f"Old file hash: [{orig_hash}]")
            logger.warning(f"New file hash: [{new_hash}]")
            error(
                f"""\n\n There are changes in asv test versions. 
Open file {benchmark_config} compare with previous version and             
make sure you submit the file  in git repo"""
            )
            err = 1

    return err


res = perform_asv_checks()
if res != 0:
    error("Errors detected - check output above")
    sys.exit(res)
else:
    logger.info("SUCCESS! All checks pass")
