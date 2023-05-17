import re

"""
Version workflow:
tag.yml -> update_version() -> _FILE -> Pybind arcticdb_ext.__version__ -> arcticdb.__version__ 
                                      ↘ resolve_version() -> setup.py -> wheel
"""
_FILE = "cpp/arcticdb/util/version_number.hpp"


def resolve_version():
    with open(_FILE) as f:
        pattern = re.compile(r'#define ARCTICDB_VERSION_STR "([^"]+)"')
        for line in f:
            match = pattern.match(line)
            if match:
                return match.group(1)

    raise RuntimeError(_FILE + " is malformed")


def update_version(new_ver_str: str):
    import semver

    new_ver = semver.Version.parse(new_ver_str)
    pattern = re.compile(r"\n#define ARCTICDB_MAJOR.*// END", re.DOTALL)

    with open(_FILE, "r+") as f:
        text = f.read()
        parts = pattern.split(text, maxsplit=1)
        assert len(parts) == 2, _FILE + " is malformed:\n" + text
        out = f"""{parts[0]}
#define ARCTICDB_MAJOR {new_ver.major}
#define ARCTICDB_MINOR {new_ver.minor}
#define ARCTICDB_PATCH {new_ver.patch}
#define ARCTICDB_VERSION_STR "{new_ver}"
// END{parts[1]}"""
        f.seek(0)
        f.write(out)
        f.truncate()


if __name__ == "__main__":
    import sys
    import subprocess

    ver = sys.argv[1]
    update_version(ver)
    assert resolve_version() == ver

    subprocess.check_call(["git", "add", _FILE])
