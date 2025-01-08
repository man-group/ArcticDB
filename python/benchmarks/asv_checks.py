import os
import subprocess
import sys
from typing import List


def run_command(command: List[str], cwd) -> int:
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
        print("Error:", file=sys.stderr)
        print(err_output, file=sys.stderr)

    return error_code


def file_unchanged(filepath, last_check_time):
  try:
    modified_time = os.path.getmtime(filepath)
    return modified_time == last_check_time
  except FileNotFoundError:
    return False


def get_project_root():
    file_location = os.path.abspath(__file__)
    return file_location.split("/python/benchmarks")[0]


def perform_asv_checks():

    path = get_project_root()

    asv_config = f"{path}/asv.conf.json"
    time = os.path.getatime(asv_config)

    print("_" * 80)
    print("""IMPORTANT: The tool checks CURRENT versions of asv tests along with asv.conf.json")
            That means that if there are files that are not submitted yet,
            they would need to be in order for completion of current PR""")
    print("_" * 80)

    print("\n\nCheck 1: Executing check for python cod of asv tests")
    if run_command(["asv", "check", "--python=same"], path) != 0:
        print("Please address all reported errors and submit code in the PR.", file=sys.stderr)
    else:
        print("Relax, no worries. Code is fine!")


    print("\n\nCheck 2: Check that asv.conf.json has up to date latest versions of tests.")
    if run_command(["asv", "run", "--bench", "just-discover", "--python=same"], path) != 0:
        if not file_unchanged(asv_config, time):
            print(f"""\n\n There are changes in asv test versions. 
                  Make sure you submit the file {asv_config} in git repo""", file=sys.stderr)
    else:
        print("Great, there are no new versions of asv test either!")


perform_asv_checks()