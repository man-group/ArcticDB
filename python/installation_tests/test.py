

from arcticdb.arctic import Arctic

lib_name = "ff"
ac = Arctic("lmdb:///Temp/Test_LIB")
print("ARCTIC: ", ac)
ac.create_library(lib_name)
print("lib created")
lib = ac.get_library(lib_name)
print("LIB: ", lib)
ac.delete_library(lib_name)
