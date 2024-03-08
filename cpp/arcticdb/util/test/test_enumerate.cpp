#include <gtest/gtest.h>
#include <arcticdb/util/enumerate.hpp>
#include <arcticdb/log/log.hpp>

TEST(Enumerate, Vector) {
    std::vector<int> vec = {1, 2, 3, 4};

    using namespace arcticdb;
    int check = 1;
    size_t count = 0U;
    for(auto&& [index, thing] : enumerate(vec)) {
        ASSERT_EQ(check, thing);
        ASSERT_EQ(count, index);
        ++check;
        ++count;
    }
}

TEST(Enumerate, VectorString) {
    std::vector<std::string> vec = {"MENE", "MENE", "TEKEL", "UPHARSIN"};

    using namespace arcticdb;
    size_t count = 0U;
    for(const auto& item : enumerate(vec)) {
        ASSERT_EQ(*item, vec[count]);
        ASSERT_EQ(count, item.index);
        ++count;
    }
}

