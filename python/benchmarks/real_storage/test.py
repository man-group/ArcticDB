from benchmarks.real_list_operations import VersionLibraries
from benchmarks.real_storage.libraries_creation import Storage


aws = VersionLibraries(Storage.AMAZON)
print(aws.get_arctic().list_libraries())

aws.get_arctic().delete_library("PERM_LIST_OPS_VERSION_6")
aws.get_arctic().delete_library("PERM_LIST_OPS_VERSION_5")
aws.get_arctic().delete_library("PERM_LIST_OPS_VERSION_25")
