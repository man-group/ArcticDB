/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/configs_map.hpp>

using namespace arcticdb;

TEST(HashAccum, NotCommutative) {
    HashAccum h1;
    HashAccum h2;
    h1.reset();
    h2.reset();

    int val1 = 1;
    int val2 = 2;

    h1(&val1);
    h1(&val2);

    h2(&val2);
    h2(&val1);

    EXPECT_NE(h1.digest(), h2.digest());
}

TEST(HashComm, Commutative) {
    using namespace arcticdb;
    using namespace folly::hash;

    auto h = commutative_hash_combine(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    EXPECT_EQ(h, commutative_hash_combine(10, 9, 8, 7, 6, 5, 4, 3, 2, 1));
    EXPECT_EQ(h, commutative_hash_combine(10, 2, 3, 4, 5, 6, 7, 8, 9, 1));

    EXPECT_EQ(commutative_hash_combine(12345, 6789), commutative_hash_combine(6789, 12345));

    auto v1 = commutative_hash_combine(
            0,
            1629821389809000000,
            std::string("FIXT.1.1"),
            454.0,
            std::string("IOI"),
            3224.0,
            std::string("MA"),
            std::string("20210824-16:09:49.809"),
            std::string("GLGMKTLIST2"),
            std::string("IOIID-1629821389809"),
            std::string("REPLACE"),
            std::string("[N/A]"),
            std::string("437076CG5"),
            1.0,
            std::string("US437076CG52"),
            4,
            std::string("CORP"),
            std::string("USD_HGD"),
            20210826,
            std::string("BUY"),
            800000.0,
            std::string("USD"),
            std::string("SPREAD"),
            std::string("20210824-16:07:49"),
            std::string("20210824-16:05:49"),
            std::string("912810SX7"),
            1.0,
            std::string("MKTX"),
            std::string("C"),
            std::string("CONTRA_FIRM"),
            std::string("Spread"),
            120.0,
            std::string("MarketList-Orders"),
            std::string("Did Not Trade"),
            std::string("Holding_Bin"),
            std::string("OneStep"),
            std::string("4 2"),
            std::string("YES"),
            0.0,
            std::string("N"),
            std::string("N"),
            std::string("ILQD"),
            10467391.0,
            2.0
    );
    auto v2 = commutative_hash_combine(
            0,
            1629821389809000000,
            std::string("FIXT.1.1"),
            454.0,
            std::string("IOI"),
            3224.0,
            std::string("MA"),
            std::string("20210824-16:09:49.809"),
            std::string("GLGMKTLIST2"),
            std::string("IOIID-1629821389809"),
            std::string("REPLACE"),
            10467391.0,
            std::string("[N/A]"),
            std::string("437076CG5"),
            1.0,
            std::string("US437076CG52"),
            4,
            std::string("CORP"),
            std::string("USD_HGD"),
            20210826,
            std::string("BUY"),
            800000.0,
            std::string("USD"),
            std::string("SPREAD"),
            std::string("20210824-16:07:49"),
            std::string("20210824-16:05:49"),
            std::string("912810SX7"),
            1.0,
            std::string("MKTX"),
            std::string("C"),
            std::string("CONTRA_FIRM"),
            std::string("Spread"),
            120.0,
            std::string("MarketList-Orders"),
            std::string("Did Not Trade"),
            std::string("Holding_Bin"),
            std::string("OneStep"),
            std::string("4 2"),
            std::string("YES"),
            2.0,
            0.0,
            std::string("N"),
            std::string("N"),
            std::string("ILQD")
    );

    std::cout << v1 << std::endl;
    std::cout << v2 << std::endl;

    EXPECT_EQ(v1, v2);

    float f1 = 1234.5;
    float f2 = 78.65;

    float g1 = 1234.5;
    float g2 = 78.65;

    EXPECT_EQ(commutative_hash_combine(f1, f2), commutative_hash_combine(g2, g1));

    std::cout << commutative_hash_combine(9726480045861996436ULL, std::string("10442909")) << std::endl;
    std::cout << commutative_hash_combine(17243764687685379754ULL, std::string("[N/A]")) << std::endl;
    std::cout << commutative_hash_combine(9457389251931416297ULL, std::string("98956PAF9")) << std::endl;
    std::cout << commutative_hash_combine(2648263869243483102ULL, std::string("1")) << std::endl;
    std::cout << commutative_hash_combine(10692736793407104629ULL, std::string("US98956PAF99")) << std::endl;
    std::cout << commutative_hash_combine(7633205480517386763ULL, std::string("4")) << std::endl;
    std::cout << commutative_hash_combine(8481507868260942362ULL, std::string("CORP")) << std::endl;
    std::cout << commutative_hash_combine(4419353893253087336ULL, std::string("USD_HGD")) << std::endl;
    std::cout << commutative_hash_combine(10212419605264796417ULL, std::string("20210824")) << std::endl;

    std::cout << commutative_hash_combine(9726480045861996436ULL, std::string("10442909"), std::string("[N/A]"))
              << std::endl;

    auto h1 = commutative_hash_combine_generic(
            9726480045861996436ULL, folly::Hash{}, std::string("10442909"), std::string("[N/A]")
    );
    auto h2 = commutative_hash_combine_generic(9726480045861996436ULL, folly::Hash{}, std::string("10442909"));
    auto h3 = commutative_hash_combine_generic(h2, folly::Hash{}, std::string("[N/A]"));

    auto h4 = commutative_hash_combine_generic(9726480045861996436ULL, folly::Hash{}, std::string("[N/A]"));
    auto h5 = commutative_hash_combine_generic(h4, folly::Hash{}, std::string("10442909"));

    EXPECT_EQ(h1, h3);
    EXPECT_EQ(h1, h5);

    auto j1 = commutative_hash_combine(9726480045861996436ULL, std::string("10442909"), std::string("[N/A]"));
    auto j2 = commutative_hash_combine(9726480045861996436ULL, std::string("10442909"));
    auto j3 = commutative_hash_combine(j2, std::string("[N/A]"));

    auto j4 = commutative_hash_combine(9726480045861996436ULL, std::string("[N/A]"));
    auto j5 = commutative_hash_combine(j4, std::string("10442909"));

    EXPECT_NE(j1, j3);
    EXPECT_NE(j1, j5);
    EXPECT_NE(j3, j5);
}
