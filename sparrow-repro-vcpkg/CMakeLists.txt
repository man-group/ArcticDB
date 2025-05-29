cmake_minimum_required(VERSION 3.10)

project(SparrowTest)

find_package(sparrow CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)


add_executable(test_sparrow test_sparrow.cpp)

target_link_libraries(test_sparrow PUBLIC sparrow::sparrow GTest::gtest)