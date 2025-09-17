"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import os
import re
import subprocess
from argparse import ArgumentParser


def get_git_tags():
    result = subprocess.run(["git", "tag", "--list"], capture_output=True, text=True)

    def get_version_from_tag(tag):
        version = tag.split(".")
        major = int(version[0].replace("v", ""))
        minor = int(version[1])
        patch = int(version[2])
        return major, minor, patch

    # Filter the tags using a regular expression
    pattern = r"^v[0-9]+\.[0-9]+\.[0-9]+$"
    tags = [tag for tag in result.stdout.splitlines() if re.match(pattern, tag)]
    # We are only interested in tags with version 5.3.0 or higher
    # Because older versions are trying to use depricated Numpy versions
    filtered_tags = [tag for tag in tags if get_version_from_tag(tag) >= (5, 3, 0)]
    return filtered_tags


def get_commit_from_tag(tag):
    result = subprocess.run(
        ["git", "rev-list", "-n", "1", tag], capture_output=True, text=True
    )
    return result.stdout.strip()


parser = ArgumentParser(
    description="Add a list of commits that need to be benchmarked to the GitHub output"
)
parser.add_argument(
    "--run_all_benchmarks",
    help="Should we get the tags and commits for all benchmarks, or just the current one",
    action="store_true",
)

args = parser.parse_args()

if args.run_all_benchmarks:
    tags = get_git_tags()
    commits = [get_commit_from_tag(tag) for tag in tags]
else:
    # Get the current git commit hash
    tags = ["HEAD"]
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True
    )
    commits = [result.stdout.strip()]

short_commits = [commit[:8] for commit in commits]

print(
    "Git tags and their corresponding commits: " + str(list(zip(tags, short_commits)))
)

if "GITHUB_OUTPUT" in os.environ:
    with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
        print(f"commits={str(short_commits)}", file=fh)
