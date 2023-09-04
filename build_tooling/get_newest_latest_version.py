import requests
from distutils.version import LooseVersion

def get_latest_version(package_name):
    url = f"https://pypi.org/pypi/{package_name}/json"
    data = requests.get(url, verify=False).json()
    # ignore release candidates
    versions = [ver for ver in data["releases"].keys() if 'rc' not in ver]
    versions.sort(key=LooseVersion)
    latest_major = 0
    latest_version = None
    for ver in versions:
        major_ver = int(ver.split('.')[0])
        if major_ver > latest_major:
            latest_major = major_ver
            latest_version = ver
    return latest_version

if __name__ == "__main__":
    print(f"ARCTICDB_LATEST_OLDEST_RELEASE={get_latest_version('arcticdb')}", end='', flush=True)
