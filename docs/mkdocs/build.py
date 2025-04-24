"""
Builds the current-version and multi-versioned documentation for ArcticDB.

- 'mike' generates the multi-versioned docs on the 'docs-pages' branch (branch is defined in mkdocs.yml).
- This script doesn't push the branch to the remote, so follow up with a `git push origin docs-pages` if needed.
- This script then clones the resulting branch into 'versioned_site'.

Build effect on docs/mkdocs/
- arcticdb_ext/       - generated stubfiles
- overrides/main.html - modified to update version number
- site/               - current build documentation
- versioned_site/     - multi-versioned documentation (clone of docs-pages branch)
"""
import os
import argparse
import subprocess


def run(cmd, **kwargs):
    return subprocess.run(cmd, check=True, **kwargs)


parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('version', nargs='?', default='dev')
parser.add_argument('-l', '--latest', action='store_true',
                    help="Set the convenience property 'latest' on this version.")


if __name__ == '__main__':
    args = parser.parse_args()
    latest = 'latest' if args.latest else ''
    version = args.version

    assert os.getcwd().endswith('docs/mkdocs'), "Run in 'mkdocs' directory."

    # install dependencies
    run(['pip3', 'install', 'arcticdb[mkdocs_build]'])

    # generate python stub files into arcticdb_ext subdir
    run(['pybind11-stubgen', 'arcticdb_ext',  '-o', '.'])

    # fetch existing doc versions
    run(['git', 'fetch', 'origin', 'docs-pages', '--depth=1'])

    # record hash for source branch in commit message
    git_result = run(['git', 'rev-parse', '--short', 'HEAD'], capture_output=True)
    git_hash = git_result.stdout.strip().decode()
    commit_message = f"Deploying docs: {version} {git_hash}"
    if latest: commit_message += ' ' + latest
    print(commit_message)

    run(['mike', 'list'])
    cmd = ['mike', 'deploy', '--update-aliases', '--ignore-remote-status', '--message', f"'{commit_message}'", version]
    if latest: cmd += [latest]
    run(cmd)
    run(['mike', 'set-default', 'latest', '--ignore-remote-status'])
    run(['mike', 'list'])

    # clone the updated branch into subdir
    run(['git', 'clone', '--depth=1', '--branch=docs-pages', '--single-branch', '../..', 'versioned_site'])
