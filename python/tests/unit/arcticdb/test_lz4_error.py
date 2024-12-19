import os, sys
sys.path.append(os.getcwd())
os.environ["MAN_ARCTICDB_USE_ARCTICDB"] = "true"
from ahl.mongo.mongoose import NativeMongoose

lib = NativeMongoose('research')["aowens.read_batch_profiling_pure"]

print(len(lib.list_symbols()))