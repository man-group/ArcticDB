import time
import os

os.environ["USER"] = "aseaton"
os.environ["MAN_ARCTICDB_USE_ARCTICDB"] = "true"

from arcticc import IS_ARCTICDB_ENABLED
assert IS_ARCTICDB_ENABLED

from ahl.mongo import NativeMongoose
from arcticdb.version_store.processing import QueryBuilder
from datetime import datetime as dt


m = NativeMongoose("mktdatas")
lib = m.get_library("security_master.varctic")

start = dt(2024, 7, 23)

sym = "(FUND-63940)(ADDITIONAL_OPTIONS-1)"

q = QueryBuilder()
q = q.date_range((start, start))
q = q[q["TemplateId"].isin(1)]
q = q[q["ReportFieldId"].isin(9926, 3505)]
q = q[q["ValuationSpecId"].isin(8)]
q = q[q["RiskTypeId"].isin(0)]

if __name__ == "__main__":
    for i in range(5):
        start = time.time_ns()
        res = lib.read(sym, query_builder=q).data
        end = time.time_ns()
        print(f"Time (s): {(end - start) / 1e9}")
