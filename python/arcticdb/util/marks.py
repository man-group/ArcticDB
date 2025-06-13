
# Defined shorter logs on errors
import os


# The marks defined to be used in arcticdb package.

SHORTER_LOGS = os.getenv("ARCTICDB_SHORTER_LOGS", "0") == "1"
ARCTICDB_USING_CONDA = os.getenv("ARCTICDB_USING_CONDA", "0") == "1"