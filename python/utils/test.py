from datetime import datetime
import getpass
import os
import re
import threading


class SharedResource:

    def __init__(self, prefix):
        self._prefix = prefix
        self._suffix = (
            f"{os.uname().nodename}_{getpass.getuser()}_{os.getpid()}_{threading.get_ident()}_{datetime.now()}"
        )
        self._suffix = re.sub(r"[.: -]", "_", self._suffix)

    def name(self):
        return f"{self._prefix}__{self._suffix}"

    def __str__(self):
        return self.name()


class Constants:

    SOME_CONSTANT = SharedResource("SOME_CONSTANT")
    OTHER_CONSTANT = SharedResource("OTHER_CONSTANT")


print(f"Prefix {Constants.SOME_CONSTANT._prefix}")
print(f"Name {Constants.SOME_CONSTANT}")
