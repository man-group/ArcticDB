import os
import resource
import time

from arcticdb_ext import set_config_int
from arcticdb import Arctic, StagedDataFinalizeMethod
from arcticdb.config import set_log_level

import arcticdb_ext
print(f"Using arcticdb_ext {arcticdb_ext.__file__}")

#set_log_level(specific_log_levels={"version": "DEBUG", "schedule": "DEBUG"})

s3_sleep_ms = int(os.environ["S3_SLEEP_MS"])  # eg 3_000
set_config_int("S3.WriteSleepMs", s3_sleep_ms)
num_blocking_threads = int(os.environ["NUM_THREADS"])  # eg 10
set_config_int("VersionStore.NumBlockingCPUThreads", num_blocking_threads)
queue_size = int(os.environ["QUEUE_SIZE"])  # eg 20
set_config_int("VersionStore.BlockingCPUQueueSize", queue_size)

print(f"PID={os.getpid()}")
#ac = Arctic("lmdb:///home/alex/big_disk/part/lmdb_one?map_size=100GB")
ac = Arctic("s3://172.17.0.2:9000:aseaton?access=3SePAqKdc1O7JgeDIJob&secret=zhtHzQtQt7UZJVUHk3QtpShSeRYZozwEl0pVeq8A")
lib = ac["tst"]
incompletes = lib.get_staged_symbols()
assert incompletes, "No incompletes were staged"
print(incompletes)
assert len(incompletes) == 1
symbol = incompletes[0]

arcticdb_ext.cpp_async.print_scheduler_stats(arcticdb_ext.log.LogLevel.INFO)

start_time = time.time()
lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)
end_time = time.time()

typ = os.getenv("TYPE", "mem")
with open(f"results_{typ}_{os.getpid()}.txt", "a") as f:
    f.write(f"sleep_ms={s3_sleep_ms} num_blocking_threads={num_blocking_threads} queue_size={queue_size} time(s)={end_time - start_time}")
