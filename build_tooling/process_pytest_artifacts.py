import subprocess
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from arcticdb.storage_fixtures.s3 import real_s3_from_environment_variables
import time
import os
from pathlib import Path
import zipfile
import pandas as pd
from arcticdb import Arctic
import click
from typing import Union, List


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

    output = subprocess.run(cmd, capture_output=True, text=True)

    if output.returncode != 0:
        print(f"Error on page {page}: {output.stderr}")
        return []

    data = json.loads(output.stdout)
    runs = data.get("workflow_runs", [])

    # Filter for build.yml workflow
    runs = [run for run in runs if run.get("path") == ".github/workflows/build.yml"]

    print(f"Fetched page {page}: {len(runs)} runs")
    return runs


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

    output = subprocess.run(cmd, capture_output=True, text=True)

    if output.returncode != 0:
        print(f"Error getting artifacts for run {artifact_download_url}: {output.stderr}")
        return []

    data = json.loads(output.stdout)
    artifacts = data.get("artifacts", [])

    print(f"Found {len(artifacts)} artifacts for run {artifact_download_url}")
    return artifacts


def download_artifact(artifact, download_dir):
    """Download a single artifact"""
    artifact_id = artifact["id"]
    artifact_name = artifact["name"]
    download_url = artifact["archive_download_url"]

    # check if the file contains pytest
    if "pytest" not in artifact_name:
        # print(f"Artifact {artifact_name} does not contain pytest, skipping...")
        return None

    # Create directory for this artifact
    artifact_dir = download_dir / f"run_{artifact['workflow_run']['id']}_{artifact_name}"

    # check if the directory already exists
    if artifact_dir.exists():
        # print(f"Artifact {artifact_name} already exists, skipping...")
        return None

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

        output = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE)

    if output.returncode != 0:
        print(f"Error downloading {artifact_name}: {output.stderr.decode()}")
        zip_path.unlink(missing_ok=True)  # Remove the file if download failed
        return None

    # print(f"Downloaded {artifact_name} to {zip_path}")
    return zip_path


def extract_zip(zip_path, extract_dir):
    """Extract a zip file"""
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        print(f"Extracted {zip_path} to {extract_dir}")

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

        # Extract Python version
        python_version = None
        py_match = re.search(r"cp(\d+)", artifact_name)
        test_type = artifact_name.split("-")[3]

        if py_match:
            python_version = f"{py_match.group(1)}"

        subversion = python_version[1:]

        assert subversion in ["6", "7", "8", "9", "10", "11", "12", "13"], python_version

        python_version = f"3.{subversion}"

        return python_version, test_type

    xml_files = []

    for xml_file in download_dir.glob("**/*.xml"):
        if xml_file.is_file() and "pytest" in xml_file.name and run_id in str(xml_file):
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
        python_version, test_type = parse_artifact_info_from_path(xml_file)

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
        if python_version:
            file_testsuite.set("python_version", python_version)
        if test_type:
            file_testsuite.set("test_type", test_type)

        # Copy attributes from original testsuite
        for key, value in root.attrib.items():
            if key not in ["name", "source_file", "python_version", "test_type"]:
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
                if python_version:
                    new_testcase.set("python_version", python_version)
                if test_type:
                    new_testcase.set("test_type", test_type)

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


def create_csv_from_unified_xml(unified_xml_path, csv_path=None):
    """Create a CSV file from a unified XML file"""
    import xml.etree.ElementTree as ET
    import csv

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
    ]

    csv_rows = []

    # Process each testsuite (file)
    for testsuite in root.findall(".//testsuite"):
        python_version = testsuite.get("python_version", "unknown")
        test_type = testsuite.get("test_type", "unknown")

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


def download_pytest_xmls_in_parallel(runs, download_dir, max_workers=5):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_run = {}
        for run in runs:
            future = executor.submit(process_workflow_run, run, download_dir)
            future_to_run[future] = run
        for future in as_completed(future_to_run):
            run = future_to_run[future]
            downloaded_files = future.result()
            print(f"Downloaded {len(downloaded_files)} files for run {run}")


def process_workflow_run(run: Union[dict, str], download_dir: Path) -> List[Path]:
    """Process a single workflow run - get artifacts and download them"""
    if isinstance(run, dict):
        run_id = run["id"]
        run_name = run.get("name", "Unknown")
        run_status = run.get("status", "Unknown")
        run_conclusion = run.get("conclusion", "Unknown")
        run_started_at = run.get("run_started_at", "Unknown")
    else:
        assert isinstance(run, str), f"Run must be a dict or a string, got {type(run)}"
        run_id = run
        run_name = "Unknown"
        run_status = "Completed"
        run_conclusion = "Unknown"
        run_started_at = "Unknown"

    if run_conclusion == "cancelled":
        print(f"Skipping run {run_id} - cancelled")
        return []

    print(f"\nProcessing run {run_id}: {run_name} ({run_status}/{run_conclusion}) {run_started_at}")

    # Only process completed runs
    if run_status.lower() != "completed":
        print(f"Skipping run {run_id} - not completed")
        return []

    downloaded_zips = []
    # Get artifacts for this run
    if isinstance(run, dict):
        artifacts = get_artifacts_for_run(run["artifacts_url"])
        for artifact in artifacts:
            zip_path = download_artifact(artifact, download_dir)
            if zip_path:
                downloaded_zips.append(zip_path)
    else:
        downloaded_zips = list(download_dir.glob("*.zip"))

    downloaded_files = []
    for zip_path in downloaded_zips:
        extract_dir = zip_path.parent / "extracted"
        extract_zip(zip_path, extract_dir)

    return downloaded_files


def extract_csv_from_xmls(run_id, download_dir):
    # Unify XML files for this run
    unified_xml = unify_xml_for_run(run_id, download_dir)

    csv_path = unified_xml.with_suffix(".csv")

    if not csv_path.exists():
        # Create CSV from unified XML
        csv_path = create_csv_from_unified_xml(unified_xml, csv_path)

    return csv_path


def get_all_workflow_runs_parallel(max_workers=5, max_pages=20):
    """Get all workflow runs using parallel requests"""
    all_runs = []

    # First, get the first page
    first_page = fetch_page(1, 100)
    if not first_page:
        return []

    all_runs.extend(first_page)

    # Fetch remaining pages in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {}

        for page_num in range(2, max_pages + 1):
            future = executor.submit(fetch_page, page_num, 100)
            future_to_page[future] = page_num

        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                runs = future.result()
                if runs:
                    all_runs.extend(runs)
                else:
                    print(f"Empty page {page_num}, reached the end")
                    break
            except Exception as e:
                print(f"Error processing page {page_num}: {e}")

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


def save_csv_files_to_lib(csv_files):
    lib = get_results_lib("pytest_results")

    for csv_file in csv_files:
        run_id = csv_file.stem
        print(f"Saving {csv_file} for {run_id}")
        df = pd.read_csv(csv_file)
        lib.write(run_id, df)


@click.command()
@click.option("--max-workers", type=int, default=5)
@click.option("--max-pages", type=int, default=20)
@click.option("--download-dir", type=str, default="gh_artifacts")
@click.option("--run-id", type=str, default=None)
def main(max_workers, max_pages, download_dir, run_id):
    # Configuration
    download_dir = Path(download_dir)

    if run_id is None:
        # Get all workflow runs
        if not download_dir.exists():
            print(f"Download directory {download_dir} does not exist")

            # Get workflow runs
            print("Fetching workflow runs...")
            start_time = time.time()
            all_runs = get_all_workflow_runs_parallel(max_workers=max_workers, max_pages=max_pages)
            end_time = time.time()

            print(f"\nTotal workflow runs fetched: {len(all_runs)}")
            print(f"Time taken: {end_time - start_time:.2f} seconds")

            print("Downloading pytest XMLs...")
            start_time = time.time()
            download_pytest_xmls_in_parallel(all_runs, download_dir, max_workers=max_workers)
            end_time = time.time()
            print(f"Time taken: {end_time - start_time:.2f} seconds")
        else:
            all_runs = [run.stem.split("_")[1] for run in download_dir.glob("*.xml")]
    else:
        # Get a single workflow run
        all_runs = [run_id]

    # Process runs and download artifacts
    print(f"\nProcessing {len(all_runs)} workflow runs...")

    # Process runs in parallel
    all_csv_files = []
    for run in all_runs:
        csv_file = extract_csv_from_xmls(run, download_dir)
        all_csv_files.append(csv_file)

    assert len(all_csv_files) == len(all_runs), (
        f"Number of CSV files should be equal to number of runs,"
        f"found {len(all_csv_files)} CSV files for {len(all_runs)} runs"
    )

    print(f"\nProduced {len(all_csv_files)} CSV files")
    print(f"All files saved to: {download_dir.absolute()}")

    save_csv_files_to_lib(all_csv_files)


if __name__ == "__main__":
    main()
