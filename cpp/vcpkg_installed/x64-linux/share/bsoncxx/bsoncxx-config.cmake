include(CMakeFindDependencyMacro)
find_dependency(bson-1.0 REQUIRED)

include("${CMAKE_CURRENT_LIST_DIR}/bsoncxx_targets.cmake")
