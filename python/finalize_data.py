import os
import resource
import time

from arcticdb_ext import set_config_int
from arcticdb import Arctic, StagedDataFinalizeMethod
from arcticdb.config import set_log_level

import arcticdb_ext
print(f"Using arcticdb_ext {arcticdb_ext.__file__}")

set_log_level(specific_log_levels={"storage": "DEBUG"})

s3_sleep_ms = int(os.environ["S3_SLEEP_MS"])  # eg 3_000
set_config_int("S3.WriteSleepMs", s3_sleep_ms)
queue_size = int(os.environ["QUEUE_SIZE"])  # eg 20
set_config_int("VersionStore.NumSegmentsLiveDuringCompaction", queue_size)

print(f"PID={os.getpid()}")
ac = Arctic("s3://172.17.0.2:9000:aseaton?access=BDmqp2RBbLjVWE7tljsh&secret=4v4v1aWkH6mYuPhrH85ppytH3fawqyNNBNc3LeD2")
lib = ac["tst"]
incompletes = lib.get_staged_symbols()
assert incompletes, "No incompletes were staged"
print(incompletes)
assert len(incompletes) == 1
symbol = incompletes[0]

start_time = time.time()
lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)
end_time = time.time()

typ = os.getenv("TYPE", "mem")
with open(f"results_{typ}_{os.getpid()}.txt", "a") as f:
    f.write(f"sleep_ms={s3_sleep_ms} queue_size={queue_size} time(s)={end_time - start_time}")
