"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import traceback

from arcticdb_ext.log import configure
from arcticdb_ext.log import log as _log, LogLevel as _Lvl, LoggerId as _LoggerId, is_active as _is_active


class _Logger(object):
    def __init__(self, id):
        self._id = id

    def log(self, lvl, msg, *args, **kwargs):
        if not _is_active(self._id, lvl):
            return
        _log(self._id, lvl, msg.format(*args, **kwargs))

    def debug(self, msg, *args, **kwargs):
        self.log(_Lvl.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.log(_Lvl.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.log(_Lvl.WARN, msg, *args, **kwargs)

    warn = warning

    def error(self, msg, *args, **kwargs):
        self.log(_Lvl.ERROR, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        exc = traceback.format_exc()
        _log(self._id, _Lvl.ERROR, msg.format(*args, **kwargs) + "\n" + exc)


logger_by_name = {
    "codec": _Logger(_LoggerId.CODEC),
    "inmem": _Logger(_LoggerId.IN_MEM),
    "root": _Logger(_LoggerId.ROOT),
    "storage": _Logger(_LoggerId.STORAGE),
    "version": _Logger(_LoggerId.VERSION),
    "memory": _Logger(_LoggerId.MEMORY),
    "timings": _Logger(_LoggerId.TIMINGS),
    "lock": _Logger(_LoggerId.LOCK),
    "schedule": _Logger(_LoggerId.SCHEDULE),
    "symbol": _Logger(_LoggerId.SYMBOL),
    "snapshot": _Logger(_LoggerId.SNAPSHOT)
}

for key, value in logger_by_name.items():
    globals()[key] = value
