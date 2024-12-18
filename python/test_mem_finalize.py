import os
import resource
from arcticdb import Arctic, StagedDataFinalizeMethod
from arcticdb.config import set_log_level

set_log_level(specific_log_levels={"version": "DEBUG"})

print(f"PID={os.getpid()}")
#ac = Arctic("lmdb:///home/alex/big_disk/part/lmdb_one?map_size=100GB")
ac = Arctic("s3://172.17.0.2:9000:aseaton?access=3SePAqKdc1O7JgeDIJob&secret=zhtHzQtQt7UZJVUHk3QtpShSeRYZozwEl0pVeq8A")
lib = ac["tst"]
incompletes = lib.get_staged_symbols()
mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(f"Memory use before finalize {mem / 1e3}MB")
for i, symbol in enumerate(incompletes):
    print(f"Step {i + 1}: Finalizing symbol {symbol}")
    lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f"Step {i + 1}: Memory use after finalize {mem / 1e3}MB")
