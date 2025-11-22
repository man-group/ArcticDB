"""Linting tools for ArcticDB.

Usage:

First bootstrap by installing the linting tools:

python build_tooling/format.py --install-tools

Then see the help section for how to run the linters:

python build_tooling/format.py --help

Or just run them on everything:

python build_tooling/format.py --in-place --type all

"""
import argparse
import pathlib
import sys
import subprocess


black_version = "24.8.0"
clang_format_version = "19.1.2"


def install_tools():
    black = subprocess.run(["pip", "install", f"black=={black_version}"]).returncode
    clang = subprocess.run(["pip", "install", f"clang-format=={clang_format_version}"]).returncode
    return black or clang


def lint_python(in_place: bool, specific_file: str = None):
    try:
        import black
        assert black.__version__ == black_version
    except ImportError:
        raise RuntimeError("black not installed. Run this script with --install-tools then try again")

    if specific_file:
        path = specific_file
    else:
        path = "python/"

    if in_place:
        return subprocess.run(["black", "-l", "120", path]).returncode
    else:
        return subprocess.run(["black", "-l", "120", "--check", path]).returncode


def lint_cpp(in_place: bool, specific_file: str = None):
    try:
        import clang_format
    except ImportError:
        raise RuntimeError("clang-format not installed. Run this script with --install-tools then try again")

    files = []
    if specific_file:
        files.append(specific_file)
    else:
        root = pathlib.Path("cpp", "arcticdb")
        for e in ("*.cpp", "*.hpp"):
            for f in root.rglob(e):
                files.append(str(f))

    args = ["clang-format"]
    if in_place:
        args.append("-i")
    else:
        args.append("--dry-run")
        args.append("-Werror")

    print(f"Running {args} over {len(files)} files")
    args += files

    return subprocess.run(args).returncode


def main(type: str, in_place: bool, specific_file: str):
    if type == "python":
        return lint_python(in_place, specific_file)
    elif type == "cpp":
        return lint_cpp(in_place, specific_file)
    else:
        return lint_python(in_place) or lint_cpp(in_place)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="ArcticDBLint",
        description="Linter for ArcticDB"
    )
    parser.add_argument(
        '--install-tools',
        action='store_true',
        help="Install the linters we need"
    )
    parser.add_argument(
        '-t',
        '--type',
        help="Type of files to format. {python,cpp,all}"
    )
    parser.add_argument(
        '-c',
        '--check',
        action='store_true',
        help="Just check that your code is compliant. Do not change files."
    )
    parser.add_argument(
        '-i',
        '--in-place',
        action='store_true',
        help="Apply linting rules to your working copy. Changes files."
    )
    parser.add_argument(
        "-f", 
        "--file", 
        help="Apply linting rules to a specific file."
    )
    args = parser.parse_args()

    if args.install_tools:
        sys.exit(install_tools())

    if args.check and args.in_place:
        raise RuntimeError("Cannot specify both --check and --in-place")
    if not args.check and not args.in_place:
        raise RuntimeError("Must specify exactly on of --check and --in-place")
    if not args.type:
        raise RuntimeError("Must specify --type")
    if args.type not in ("python", "cpp", "all"):
        raise RuntimeError("Invalid --type")

    return_code = main(
        type=args.type,
        in_place=args.in_place,
        specific_file=args.file,
    )

    sys.exit(return_code)
