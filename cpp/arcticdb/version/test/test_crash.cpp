#include <gtest/gtest.h>

TEST(CrashTest, ExampleCrash)
{
    int* a = nullptr;
    std::cout << *a;
}
