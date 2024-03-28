/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>
#include <memory>

namespace arcticdb {
    struct TestStore : ::testing::Test {
    protected:
        virtual std::string get_name() = 0;

        void SetUp() override {
            test_store_ = test_store(get_name());
        }

        void TearDown() override {
            test_store_->clear();
        }

        std::shared_ptr<arcticdb::version_store::PythonVersionStore> test_store_;
    };
}
