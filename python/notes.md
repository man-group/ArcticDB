```
(310) ➜  python git:(aseaton/20241121/finalize-staged-data-investigation) ✗ cat test_mem_finalize.py 
import os
import resource
from arcticdb import Arctic, StagedDataFinalizeMethod

print(f"PID={os.getpid()}")

#ac = Arctic("lmdb:///home/alex/big_disk/part/lmdb_one?map_size=100GB")
ac = Arctic("s3://172.17.0.2:9000:aseaton?access=3SePAqKdc1O7JgeDIJob&secret=zhtHzQtQt7UZJVUHk3QtpShSeRYZozwEl0pVeq8A")
lib = ac["tst"]
symbol = "staged"

mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(f"Memory use before finalize {mem / 1e3}MB")
lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)
mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(f"Memory use after finalize {mem / 1e3}MB")
```


Before change:

```
(310) ➜  python git:(aseaton/20241121/finalize-staged-data-investigation) ✗ python test_mem_finalize.py                                           
PID=110918
Memory use before finalize 142.26MB
Memory use after finalize 3241.316MB
```

After skipping the check:

```
(310) ➜  python git:(aseaton/20241121/finalize-staged-data-investigation) ✗ python test_mem_finalize.py                                                   
PID=112593
Memory use before finalize 143.22MB
Memory use after finalize 1254.268MB
```

Next steps:
- Fix up failing tests
- Tests that the data keys get deleted when there is an error
- Test memory use improvement
- Test performance
