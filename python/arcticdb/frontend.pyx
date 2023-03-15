"""
This file ensures we can import multiple Cython-compiled modules from a single native extension.

Anything beginning with 'arcticc' is eligible to be imported from the native extension.

Sourced from:

https://stackoverflow.com/questions/30157363/collapse-multiple-submodules-to-one-cython-extension/52729181#52729181
"""

import sys
import importlib.machinery as machinery
import importlib.abc as abc


class RootLoader:
    def load_module(self, fullname):
        try:
            return sys.modules[fullname]
        except KeyError:
            m = sys.modules[fullname.split(".")[0]]
            sys.modules[fullname] = m
            return sys.modules[fullname]


class CythonPackageMetaPathFinder(abc.MetaPathFinder):
    def __init__(self, name_filter):
        super(CythonPackageMetaPathFinder, self).__init__()
        self.name_filter = name_filter

    def find_module(self, fullname, path):
        if fullname.startswith(self.name_filter):
            if fullname.endswith(".pb2"):
                # The C++ module imports compiled protobuf code.
                # This loader ensures the paths are found correctly.
                return RootLoader()

            return machinery.ExtensionFileLoader(fullname, __file__)


def bootstrap_cython_submodules():
    sys.meta_path.append(CythonPackageMetaPathFinder("arcticc."))
