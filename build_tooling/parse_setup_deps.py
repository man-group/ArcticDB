"""Parse setup_requires, install_requires and extras_require[Testing] from setup.cfg."""

import configparser
import re
import sys


def parse_deps(path="setup.cfg"):
    cfg = configparser.ConfigParser()
    cfg.read(path)

    raw = (
        cfg.get("options", "setup_requires", fallback="").strip().splitlines()
        + cfg.get("options", "install_requires", fallback="").strip().splitlines()
        + cfg.get("options.extras_require", "Testing", fallback="").strip().splitlines()
    )
    for line in raw:
        spec = re.sub(r"\s*[#;].*", "", line).strip()
        if spec:
            print(spec)


if __name__ == "__main__":
    parse_deps(sys.argv[1] if len(sys.argv) > 1 else "setup.cfg")
