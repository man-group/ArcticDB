"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import time

import numpy as np

from arcticdb_ext import set_config_int, unset_config_int
from arcticdb import Arctic

ac = Arctic("lmdb:///tmp/list-versions")
libs = ac.list_libraries()

iterations = 2
print(f"Iterations: {iterations}")

set_config_int("VersionMap.ReloadInterval", 2 ** 63 - 1)

for lib in libs:
    print(f"Processing lib {lib}")
    timings = []
    l = ac[lib]

    # Get the library in an optimal state, so we look at it in a state where the set vs unordered set can make the most
    # difference.
    l.list_symbols()  # compact the symbol list cache if needed
    l.list_versions() # warm the version map cache

    for i in range(iterations):
        t_start = time.time()
        l.list_versions()
        timings.append(time.time() - t_start)
    print(f"Timings: mean={np.mean(timings)} stddev={np.std(timings)} raw={timings}")

"""Results with unordered_set:
Iterations: 2
Processing lib n_syms-1000_n_snaps-0
Timings: mean=0.04608273506164551 stddev=0.003504514694213867 raw=[0.04257822036743164, 0.049587249755859375]
Processing lib n_syms-1000_n_snaps-1
Timings: mean=0.05701935291290283 stddev=0.013175129890441895 raw=[0.07019448280334473, 0.04384422302246094]
Processing lib n_syms-100000_n_snaps-0
Timings: mean=4.658427476882935 stddev=0.005655050277709961 raw=[4.6640825271606445, 4.652772426605225]
Processing lib n_syms-100000_n_snaps-1
Timings: mean=20.312344312667847 stddev=0.25201940536499023 raw=[20.060324907302856, 20.564363718032837]
Processing lib n_syms-1000_n_snaps-1000
Timings: mean=0.36395788192749023 stddev=0.0035419464111328125 raw=[0.3604159355163574, 0.36749982833862305]
Processing lib n_syms-100000_n_snaps-1000
Timings: mean=59.98722445964813 stddev=0.4557112455368042 raw=[60.44293570518494, 59.53151321411133]

"""

"""Results with set:
Iterations: 2
Processing lib n_syms-1000_n_snaps-0
Timings: mean=0.046212077140808105 stddev=0.005065321922302246 raw=[0.05127739906311035, 0.04114675521850586]
Processing lib n_syms-1000_n_snaps-1
Timings: mean=0.05501556396484375 stddev=0.012893199920654297 raw=[0.06790876388549805, 0.04212236404418945]
Processing lib n_syms-100000_n_snaps-0
Timings: mean=4.568591117858887 stddev=0.007075786590576172 raw=[4.575666904449463, 4.5615153312683105]
Processing lib n_syms-100000_n_snaps-1
Timings: mean=20.799466967582703 stddev=1.068806767463684 raw=[19.73066020011902, 21.868273735046387]
Processing lib n_syms-1000_n_snaps-1000
Timings: mean=0.44523704051971436 stddev=5.447864532470703e-05 raw=[0.44518256187438965, 0.44529151916503906]
Processing lib n_syms-100000_n_snaps-1000
Timings: mean=70.94531798362732 stddev=0.38754820823669434 raw=[70.55776977539062, 71.33286619186401]
"""