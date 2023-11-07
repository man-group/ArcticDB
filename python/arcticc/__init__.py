# file from ArcticDB
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

# Use Man's API backwards compatibility layer if it is installed.
# This will not do anything outside of Man Group systems.
try:
    from man.arcticdb.arcticdb_renamer import *
except ImportError:
    pass
