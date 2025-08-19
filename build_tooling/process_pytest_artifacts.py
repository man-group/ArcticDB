import subprocess
import json
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from arcticdb.storage_fixtures.s3 import real_s3_from_environment_variables
import time
from pathlib import Path
import zipfile
import pandas as pd
from arcticdb import Arctic
import click
from typing import Dict, List
import xml.etree.ElementTree as ET
import csv


def fetch_page(page, per_page=100):
    """Fetch a single page of workflow runs"""
    cmd = [
        "gh",
        "api",
        "-H",
        "Accept: application/vnd.github+json",
        "-H",
        "X-GitHub-Api-Version: 2022-11-28",
        f"/repos/man-group/ArcticDB/actions/runs?per_page={per_page}&page={page}",
    ]

    output = subprocess.run(cmd, capture_output=True, text=True, check=True)

    data = json.loads(output.stdout)
    runs = data.get("workflow_runs", [])

    # Filter for build.yml workflow
    runs = [run for run in runs if run.get("path") == ".github/workflows/build.yml"]

    print(f"Fetched page {page}: {len(runs)} runs")
    return runs


def get_total_runs_and_pages(per_page=100):
    """Get total number of runs and pages by checking the first page response"""
    cmd = [
        "gh",
        "api",
        "-H",
        "Accept: application/vnd.github+json",
        "-H",
        "X-GitHub-Api-Version: 2022-11-28",
        f"/repos/man-group/ArcticDB/actions/runs?per_page={per_page}&page=1",
    ]

    output = subprocess.run(cmd, capture_output=True, text=True, check=True)

    data = json.loads(output.stdout)
    total_count = data.get("total_count", 0)

    # Calculate total pages
    total_pages = (total_count + per_page - 1) // per_page  # Ceiling division

    print(f"Total runs: {total_count}, Total pages: {total_pages}")
    return total_count, total_pages


class Run:
    def __init__(self, run):
        self.run_id = run["id"]
        self.branch = run["head_branch"]
        self.timestamp = pd.to_datetime(run["run_started_at"])
        self.commit_hash = run["head_commit"]["id"]
        self.run_status = run.get("status", "Unknown")
        self.run_conclusion = run.get("conclusion", "Unknown")
        self.artifacts_url = run.get("artifacts_url", "")

    @property
    def unix_timestamp(self):
        """Get UNIX timestamp (seconds since epoch)"""
        return int(self.timestamp.timestamp())

    @property
    def unix_timestamp_ms(self):
        """Get UNIX timestamp in milliseconds"""
        return int(self.timestamp.timestamp() * 1000)

    def __str__(self):
        return f"Run(run_id={self.run_id}, branch={self.branch}, timestamp={self.timestamp.strftime('%Y-%m-%d %H:%M:%S')} (UNIX: {self.unix_timestamp}), commit_hash={self.commit_hash})"

    def __repr__(self):
        return self.__str__()


def create_run_from_github_actions():
    import os

    run_id = os.environ.get("GITHUB_RUN_ID")
    branch = os.environ.get("GITHUB_REF_NAME")
    commit_hash = os.environ.get("GITHUB_SHA")

    start_time = os.environ.get("GITHUB_RUN_STARTED_AT")
    if not start_time:
        start_time = pd.Timestamp.now().isoformat()

    if not all([run_id, branch, commit_hash]):
        raise ValueError(
            f"Missing required GitHub Actions environment variables. "
            f"GITHUB_RUN_ID: {run_id}, GITHUB_REF_NAME: {branch}, GITHUB_SHA: {commit_hash}"
        )

    run_dict = {
        "id": run_id,
        "head_branch": branch,
        "run_started_at": start_time,
        "head_commit": {"id": commit_hash},
    }

    return Run(run_dict)


def get_artifacts_for_run(artifact_download_url):
    """Get artifacts for a specific workflow run"""
    cmd = [
        "gh",
        "api",
        "-H",
        "Accept: application/vnd.github+json",
        "-H",
        "X-GitHub-Api-Version: 2022-11-28",
        f"{artifact_download_url}",
    ]

    output = subprocess.run(cmd, capture_output=True, text=True, check=True)

    if output.returncode != 0:
        raise Exception(f"Error getting artifacts for run {artifact_download_url}: {output.stderr}")

    data = json.loads(output.stdout)
    artifacts = data.get("artifacts", [])

    # print(f"Found {len(artifacts)} artifacts for run {artifact_download_url}")
    return artifacts


def download_artifact(run: Run, artifact: dict, download_dir: Path) -> Path:
    """Download a single artifact"""
    artifact_id = artifact["id"]
    artifact_name = artifact["name"]

    # check if the file contains pytest
    if "pytest" not in artifact_name:
        # print(f"Artifact {artifact_name} does not contain pytest, skipping...")
        return None

    # Create directory for this artifact
    artifact_dir = download_dir / f"run_{run.run_id}_{artifact_name}"

    artifact_dir.mkdir(parents=True, exist_ok=True)

    # Download the zip file
    zip_path = artifact_dir / f"{artifact_name}.zip"

    if zip_path.exists():
        # print(f"Artifact {artifact_name} already exists, skipping...")
        return zip_path

    # print(f"Downloading {artifact_name} from {download_url}")

    # Use gh CLI to download (it handles authentication)
    # Redirect output to file instead of using --output flag
    with open(zip_path, "wb") as f:
        cmd = [
            "gh",
            "api",
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            "X-GitHub-Api-Version: 2022-11-28",
            f"/repos/man-group/ArcticDB/actions/artifacts/{artifact_id}/zip",
        ]

        subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, check=True)

    return zip_path


def extract_zip(zip_path, extract_dir):
    """Extract a zip file"""
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        # print(f"Extracted {zip_path} to {extract_dir}")

        # remove the zip file
        zip_path.unlink(missing_ok=True)

        return True
    except Exception as e:
        print(f"Error extracting {zip_path}: {e}")
        return False


def unify_xml_for_run(run_id, download_dir):
    """Unify all XML files for a specific run into a single XML file"""
    import xml.etree.ElementTree as ET
    from pathlib import Path
    import re

    def parse_artifact_info_from_path(file_path):
        """Extract artifact information from the file path"""
        # Example: run_16522631587_pytest-linux-cp38-{hypothesis,nonreg,scripts}-DefaultCache/extracted/pytest..xml
        path_parts = file_path.parts

        # Find the run directory
        run_dir = None
        for part in path_parts:
            if "pytest-" in part:
                run_dir = part
                break

        if not run_dir:
            return None, None

        # Extract artifact name and parse it
        # run_16522631587_pytest-linux-cp38-{hypothesis,nonreg,scripts}-DefaultCache
        artifact_name = run_dir.replace(f"run_{run_id}_", "")

        python_version = None
        py_match = re.search(r"cp(\d+)", artifact_name)
        test_data = artifact_name.split("-")
        test_type = test_data[3]
        cache_type = test_data[4] if len(test_data) > 4 else "DefaultCache"

        if py_match:
            python_version = f"{py_match.group(1)}"

        subversion = python_version[1:]

        assert subversion in ["6", "7", "8", "9", "10", "11", "12", "13"], python_version

        python_version = f"3.{subversion}"

        return python_version, test_type, cache_type

    xml_files = []

    for xml_file in download_dir.glob("**/*.xml"):
        if xml_file.is_file() and "pytest" in xml_file.name and str(run_id) in str(xml_file):
            xml_files.append(xml_file)

    if not xml_files:
        print(f"No XML files found for run {run_id}")
        return None

    print(f"Found {len(xml_files)} XML files for run {run_id}")

    # Create unified XML
    unified_root = ET.Element("testsuites")
    unified_root.set("name", f"run_{run_id}_unified")
    unified_root.set("run_id", str(run_id))

    total_tests = 0
    total_failures = 0
    total_errors = 0
    total_skipped = 0
    total_time = 0.0

    # Process each XML file
    for xml_file in xml_files:
        python_version, test_type, cache_type = parse_artifact_info_from_path(xml_file)

        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
        except ET.ParseError as e:
            print(f"    Error parsing {xml_file}: {e}")
            continue

        # Create testsuite for this file
        file_testsuite = ET.SubElement(unified_root, "testsuite")
        file_testsuite.set("name", xml_file.name)
        file_testsuite.set("source_file", str(xml_file))
        file_testsuite.set("python_version", python_version)
        file_testsuite.set("test_type", test_type)
        file_testsuite.set("cache_type", cache_type)

        # Copy attributes from original testsuite
        for key, value in root.attrib.items():
            if key not in ["name", "source_file", "python_version", "test_type", "cache_type"]:
                file_testsuite.set(key, value)

        # Process all testcases
        for testsuite in root.findall(".//testsuite"):
            for testcase in testsuite.findall(".//testcase"):
                # Create new testcase
                new_testcase = ET.SubElement(file_testsuite, "testcase")

                # Copy all attributes
                for key, value in testcase.attrib.items():
                    new_testcase.set(key, value)

                # Add source information
                new_testcase.set("source_run", str(run_id))
                new_testcase.set("python_version", python_version)
                new_testcase.set("test_type", test_type)
                new_testcase.set("cache_type", cache_type)

                # Copy child elements (failure, error, skipped, etc.)
                for child in testcase:
                    new_testcase.append(child)

                # Update statistics
                total_tests += 1

                if testcase.find("failure") is not None:
                    total_failures += 1
                elif testcase.find("error") is not None:
                    total_errors += 1
                elif testcase.find("skipped") is not None:
                    total_skipped += 1

                # Add time
                try:
                    test_time = float(testcase.get("time", 0))
                    total_time += test_time
                except (ValueError, TypeError):
                    pass

    # Set unified statistics
    unified_root.set("tests", str(total_tests))
    unified_root.set("failures", str(total_failures))
    unified_root.set("errors", str(total_errors))
    unified_root.set("skipped", str(total_skipped))
    unified_root.set("time", f"{total_time:.3f}")

    # Write unified XML
    unified_xml_path = download_dir / f"{run_id}.xml"
    tree = ET.ElementTree(unified_root)
    ET.indent(tree, space="  ")  # Pretty print

    with open(unified_xml_path, "w", encoding="utf-8") as f:
        tree.write(f, encoding="unicode", xml_declaration=True)

    print(f"Unified XML for run {run_id} written to {unified_xml_path}")
    print(f"  Total tests: {total_tests}, Failures: {total_failures}, Errors: {total_errors}, Skipped: {total_skipped}")

    return unified_xml_path


def create_csv_from_unified_xml(unified_xml_path, csv_path):
    try:
        tree = ET.parse(unified_xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing {unified_xml_path}: {e}")
        return None

    # Define CSV columns
    csv_columns = [
        "python_version",
        "test_type",
        "test_name",
        "status",
        "time",
        "message",
        "cache_type",
    ]

    csv_rows = []

    # Process each testsuite (file)
    for testsuite in root.findall(".//testsuite"):
        python_version = testsuite.get("python_version", "unknown")
        test_type = testsuite.get("test_type", "unknown")
        cache_type = testsuite.get("cache_type", "unknown")
        # Process each testcase
        for testcase in testsuite.findall(".//testcase"):
            test_name = testcase.get("name", "unknown")
            time = testcase.get("time", "0.0")

            # Determine status and message
            status = "passed"
            message = " "

            # Check for failures
            failure = testcase.find("failure")
            if failure is not None:
                status = "failed"
                message = failure.get("message", " ")

            # Check for errors
            error = testcase.find("error")
            if error is not None:
                status = "error"
                message = error.get("message", " ")

            # Check for skipped tests
            skipped = testcase.find("skipped")
            if skipped is not None:
                status = "skipped"
                message = skipped.get("message", " ")

            # Create CSV row
            row = {
                "python_version": python_version,
                "test_type": test_type,
                "test_name": test_name,
                "status": status,
                "time": time,
                "message": message,
                "cache_type": cache_type,
            }

            csv_rows.append(row)

    # Write CSV file
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerows(csv_rows)

    # Print summary statistics
    # status_counts = {}
    # for row in csv_rows:
    #     status = row["status"]
    #     status_counts[status] = status_counts.get(status, 0) + 1

    # print("Status summary:")
    # for status, count in status_counts.items():
    #     print(f"  {status}: {count}")

    return csv_path


def download_pytest_xmls_in_parallel(runs, download_dir, max_workers) -> List[str]:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_run = {}
        for run in runs:
            future = executor.submit(process_workflow_run, run, download_dir)
            future_to_run[future] = run

        runs_to_remove = []
        for future in as_completed(future_to_run):
            run = future_to_run[future]
            result = future.result()
            # print(f"Downloaded {len(downloaded_files)} files for run {run}")

            for run_id, downloaded_files in result.items():
                if not downloaded_files:
                    runs_to_remove.append(str(run_id))

        return runs_to_remove


def process_workflow_run(run: Run, download_dir: Path) -> Dict[str, Path]:
    """Process a single workflow run - get artifacts and download them"""

    if run.run_conclusion and run.run_conclusion.lower() == "cancelled":
        # print(f"Skipping run {run.run_id} - cancelled")
        return {run.run_id: None}

    # Only process completed runs
    if run.run_status and run.run_status.lower() != "completed":
        # print(f"Skipping run {run.run_id} - not completed")
        return {run.run_id: None}

    print(f"Processing run {run.run_id}: ({run.run_status}/{run.run_conclusion}) {run.timestamp}")

    downloaded_zips = []
    artifacts = get_artifacts_for_run(run.artifacts_url)
    for artifact in artifacts:
        zip_path = download_artifact(run, artifact, download_dir)
        if zip_path:
            downloaded_zips.append(zip_path)

    downloaded_files = []
    for zip_path in downloaded_zips:
        extract_dir = zip_path.parent / "extracted"
        extract_zip(zip_path, extract_dir)
        downloaded_files.append(extract_dir)

    return {str(run.run_id): downloaded_files}


def extract_csv_from_xmls(run_id, download_dir):
    # Unify XML files for this run
    unified_xml = unify_xml_for_run(run_id, download_dir)
    if not unified_xml:
        raise Exception(f"No unified XML found for run {run_id}")

    csv_path = unified_xml.with_suffix(".csv")

    csv_path = create_csv_from_unified_xml(unified_xml, csv_path)

    return csv_path


def get_all_workflow_runs_parallel(max_workers, max_pages=None):
    """Get all workflow runs using parallel requests"""
    all_runs = []

    # If max_pages is not provided, determine it dynamically
    if max_pages is None:
        print("Determining total number of pages dynamically...")
        total_runs, total_pages = get_total_runs_and_pages()
        max_pages = total_pages
        print(f"Found {total_runs} total runs across {total_pages} pages")

    # Fetch remaining pages in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {}

        for page_num in range(1, max_pages + 1):
            future = executor.submit(fetch_page, page_num)
            future_to_page[future] = page_num

        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            runs = future.result()
            if runs:
                all_runs.extend(runs)

    all_runs = [Run(run) for run in all_runs]
    return all_runs


def get_results_lib(arcticdb_library, arcticdb_client_override=None):
    if arcticdb_client_override:
        ac = Arctic(arcticdb_client_override)
    else:
        factory = real_s3_from_environment_variables(shared_path=True)
        factory.default_prefix = "pytest_results"
        ac = factory.create_fixture().create_arctic()

    lib = ac.get_library(arcticdb_library, create_if_missing=True)
    return lib


def save_csv_files_to_lib(run_ids, csv_files):
    lib = get_results_lib("pytest_results")

    for csv_file in csv_files:
        run_id = csv_file.stem
        print(f"Saving {csv_file} for {run_id}")
        df = pd.read_csv(csv_file)
        run = run_ids[str(run_id)]
        symbol = f"{run.branch}_{run.commit_hash}_{run.timestamp.strftime('%Y-%m-%d_%H-%M-%S')}_{run.run_id}"
        lib.write(symbol, df)

    # run a list_symbols to compact the symbol list, if needed
    for symbol in lib.list_symbols():
        print(symbol)


@click.command()
@click.option("--max-workers", type=int, default=5)
@click.option("--download-dir", type=str, default="gh_artifacts")
@click.option("--max-pages", type=int, default=None, help="Maximum number of pages to fetch (default: auto-detect)")
@click.option(
    "--use-github-actions",
    is_flag=True,
    default=False,
    help="Automatically use GitHub Actions environment variables to construct Run object",
)
def main(max_workers, download_dir, max_pages, use_github_actions):
    download_dir = Path(download_dir)

    if use_github_actions:
        try:
            run_obj = create_run_from_github_actions()
            print(f"Created Run object from GitHub Actions environment: {run_obj}")
            all_runs = {str(run_obj.run_id): run_obj}
        except Exception as e:
            print(f"Error creating Run object from GitHub Actions environment: {e}")
            print("Falling back to manual parameter mode...")
            use_github_actions = False
    else:
        # Get workflow runs
        print("Fetching workflow runs...")
        start_time = time.time()
        all_runs = get_all_workflow_runs_parallel(max_workers, max_pages)
        all_runs = {str(run.run_id): run for run in all_runs}
        end_time = time.time()

        print(f"\nTotal workflow runs fetched: {len(all_runs)}")
        print(f"Time taken: {end_time - start_time:.2f} seconds")

        print("Downloading pytest XMLs...")
        start_time = time.time()
        try:
            download_dir.mkdir(parents=True)
            runs_to_remove = download_pytest_xmls_in_parallel(all_runs.values(), download_dir, max_workers)

            print(f"Removing {len(runs_to_remove)} runs that were intentionally skipped")
            for run_id in runs_to_remove:
                del all_runs[run_id]
        except Exception as e:
            print(f"Error downloading pytest XMLs: {e}")
            shutil.rmtree(download_dir)
            raise

        end_time = time.time()
        print(f"Time taken: {end_time - start_time:.2f} seconds")

    print(f"\nProcessing {len(all_runs)} workflow runs...")

    all_csv_files = []
    for run_id in all_runs.keys():
        csv_file = extract_csv_from_xmls(run_id, download_dir)
        if csv_file:
            all_csv_files.append(csv_file)

    print(f"\nProduced {len(all_csv_files)} CSV files")
    print(f"All files saved to: {download_dir.absolute()}")

    save_csv_files_to_lib(all_runs, all_csv_files)

    # remove the download directory
    shutil.rmtree(download_dir)


if __name__ == "__main__":
    main()
