#include <gtest/gtest.h>
#include <arcticdb/util/enumerate.hpp>
#include <arcticdb/log/log.hpp>

TEST(Enumerate, Vector) {
    std::vector<int> vec = {1, 2, 3, 4};

    using namespace arcticdb;

    for(auto&& [thing, index] : enumerate(vec)) {
        arcticdb::log::version().info("{} : {}", thing, index);
    }
}

