/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <folly/futures/Future.h>
#include <gtest/gtest.h>

/**
 * We rely on folly::window continuing to process the collection even after one of the futures throws.
 *
 * Since this is not clearly tested in Folly itself, we have this regression test in case they break it in future.
 *
 * At time of writing, the symbol size calculation APIs `test_symbol_sizes.py` rely on this.
 */
TEST(Window, ContinuesOnException) {
    using namespace folly;
    std::vector<int> ints(1000);
    for (int i = 0; i < 1000; i++) {
        ints.push_back(i);
    }

    std::vector<Promise<int>> ps(1000);

    auto res =
            reduce(window(
                           ints,
                           [&ps](int i) {
                               if (i % 4 == 0) {
                                   throw std::runtime_error("exception should not kill process");
                               }
                               return ps[i].getFuture();
                           },
                           2
                   ),
                   0,
                   [](int sum, const Try<int>& b) {
                       sum += b.hasException<std::exception>() ? 0 : 1;
                       return sum;
                   });

    for (auto& p : ps) {
        p.setValue(0);
    }

    // 100 / 4 = 250 of the futures threw, 750 completed successfully
    EXPECT_EQ(750, std::move(res).get());
}
