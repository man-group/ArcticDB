from arcticdb import Arctic
from arcticdb_ext import set_config_int

set_config_int("S3Storage.VerifySSL", 0)

ac = Arctic("s3://s3.eu-west-2.amazonaws.com:arcticdb-repl-test-202404-target-1?aws_auth=sts&aws_profile=aseaton_profile")

assert ac.list_libraries()

print(ac.list_libraries())

ac.delete_library("blah")
lib = ac.create_library("blah")

lib.list_symbols()
lib.write_pickle("sym", "123")

print(lib.list_symbols())
print(lib.read("sym").data)