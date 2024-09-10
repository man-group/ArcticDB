include(CMakeFindDependencyMacro)
find_dependency(aws-cpp-sdk-core)
include("${CMAKE_CURRENT_LIST_DIR}/aws-cpp-sdk-cognito-identity-targets.cmake")
