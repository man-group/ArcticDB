import sys
import pytest
from subprocess import run, PIPE

from arcticdb_ext.log import LogLevel
from tests.util.mark import MACOS


_LEVELS = tuple(LogLevel.__entries)


@pytest.mark.parametrize("level", _LEVELS)
def test_set_log_level(level):
    code = f"""import arcticdb; import sys
print("Our printout starts", file=sys.stderr, flush=True)
arcticdb.config.set_log_level('{level}')
arcticdb.log.version.debug('test DEBUG')
arcticdb.log.version.info('test INFO')
arcticdb.log.version.warn('test WARN')
arcticdb.log.version.error('test ERROR')
"""
    p = run([sys.executable], universal_newlines=True, input=code, stderr=PIPE, timeout=10)
    lines = p.stderr.splitlines()
    idx = _LEVELS.index(level)
    while lines.pop(0) != "Our printout starts":
        pass
    for level in _LEVELS[idx:]:
        assert lines.pop(0).endswith(f"{level[0]} arcticdb.version | test {level}"), p.stderr
