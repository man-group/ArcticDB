include(CMakeFindDependencyMacro)
find_dependency(mongoc-1.0 REQUIRED)
find_dependency(bsoncxx REQUIRED)
include("${CMAKE_CURRENT_LIST_DIR}/mongocxx_targets.cmake")
