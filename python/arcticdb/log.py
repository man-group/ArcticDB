"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import traceback


def build_loggers(ext_log):
    """Return a logger registry bound to the given extension's log submodule.

    ``ext_log`` must expose ``log``, ``LogLevel``, ``LoggerId`` and ``is_active``
    (see ``register_log`` in ``python_bindings_common.cpp``).
    """
    _log = ext_log.log
    _Lvl = ext_log.LogLevel
    _is_active = ext_log.is_active
    _LoggerId = ext_log.LoggerId

    class _Logger:
        def __init__(self, id):
            self._id = id

        def log(self, lvl, msg, *args, **kwargs):
            if not self.is_active(lvl):
                return
            _log(self._id, lvl, msg.format(*args, **kwargs))

        def debug(self, msg, *args, **kwargs):
            self.log(_Lvl.DEBUG, msg, *args, **kwargs)

        def info(self, msg, *args, **kwargs):
            self.log(_Lvl.INFO, msg, *args, **kwargs)

        def warning(self, msg, *args, **kwargs):
            self.log(_Lvl.WARN, msg, *args, **kwargs)

        def is_active(self, lvl):
            return _is_active(self._id, lvl)

        warn = warning

        def error(self, msg, *args, **kwargs):
            self.log(_Lvl.ERROR, msg, *args, **kwargs)

        def exception(self, msg, *args, **kwargs):
            exc = traceback.format_exc()
            _log(self._id, _Lvl.ERROR, msg.format(*args, **kwargs) + "\n" + exc)

    return {
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
        "snapshot": _Logger(_LoggerId.SNAPSHOT),
    }


import arcticdb_ext.log as _ext_log

configure = _ext_log.configure
logger_by_name = build_loggers(_ext_log)

for _key, _value in logger_by_name.items():
    globals()[_key] = _value
