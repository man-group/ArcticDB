import hashlib
import os
import subprocess
import sys
from typing import List


def error(mes):
    print(f"ERROR :{mes}", file=sys.stderr )


def run_command(command: List[str], cwd: str, ok_error: str = None) -> int:
    """
        executes a command in specified directory.
        if 'ok_error' passed, the string will be searched in stderr
        and if found will not mark execution as error but count as ok
    """
    result = subprocess.run(command, capture_output=True, text=True, cwd=cwd)

    output = result.stdout
    err_output = result.stderr
    error_code = result.returncode

    if error_code != 0:
        print(f"Error Code: {error_code}") 

    if not output is None:
        print("Output:")
        print(output)

    if not err_output is None:
        error(err_output)
        if not ok_error is None:
            if (ok_error in err_output):
                print("Above error is ok!")
                error_code = 0

    return error_code

def compute_file_hash(file_path): 
    """Compute the SHA-256 hash of the given file.""" 
    sha256_hash = hashlib.sha256() 
    with open(file_path, "rb") as f: 
        for byte_block in iter(lambda: f.read(4096), b""): 
            sha256_hash.update(byte_block) 
    
    return sha256_hash.hexdigest()

def file_unchanged(filepath, last_check_time):
  try:
    modified_time = os.path.getmtime(filepath)
    return modified_time == last_check_time
  except FileNotFoundError:
    return False


def get_project_root():
    file_location = os.path.abspath(__file__)
    return file_location.split("/python/")[0]


def perform_asv_checks() -> int:
    """
    Performs several checks and returns result 0 if no problem or 1 if there are problems
    """

    err = 0

    path = get_project_root()

    bencmark_config = f"{path}/python/.asv/results/benchmarks.json"
    orig_hash = compute_file_hash(bencmark_config)

    print("_" * 80)
    print("""IMPORTANT: The tool checks CURRENT versions of asv tests along with asv.conf.json")
            That means that if there are files that are not submitted yet,
            they would need to be in order for completion of current PR""")
    print("_" * 80)

    print("\n\nCheck 1: Executing check for python cod of asv tests")
    if run_command(["asv", "check", "--python=same"], path) != 0:
        error("Please address all reported errors and submit code in the PR.")
        err = 1
    else:
        print("Relax, no worries. Code is fine!")


    print("\n\nCheck 2: Check that benchmarks.json has up to date latest versions of tests.")
    if run_command(command = ["asv", "run", "--bench", "just-discover", "--python=same"], 
                   cwd = path, 
                   ok_error ="Couldn't load asv.plugins._mamba_helpers") != 0:
        error("There was error getting latest benchmarks. See log")        
        err = 1
    else: 
        if compute_file_hash(bencmark_config) == orig_hash:
            print("Great, there are no new versions of asv test either!")
        else:
            error(f"""\n\n There are changes in asv test versions. 
Open file {bencmark_config} compare with previous version and             
make sure you submit the file  in git repo""")
            err = 1

    return err

res = perform_asv_checks()
if res != 0:
    error("Errors detected - check output above")
    sys.exit(res)
else: 
    print("SUCCESS! All checks pass")