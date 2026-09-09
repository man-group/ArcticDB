/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <limits>
#include <optional>

namespace arcticdb::storage::s3 {

TEST(S3ApiEventLoopThreads, DefaultsToOneThread) {
    ScopedConfig unset({{"AWS.EventLoopThreads", std::nullopt}});
    ASSERT_EQ(event_loop_thread_count_from_config(), 1);
}

TEST(S3ApiEventLoopThreads, ReadsConfiguredValue) {
    ScopedConfig config("AWS.EventLoopThreads", 4);
    ASSERT_EQ(event_loop_thread_count_from_config(), 4);
}

TEST(S3ApiEventLoopThreads, ZeroSelectsTheSdkDefault) {
    ScopedConfig config("AWS.EventLoopThreads", 0);
    ASSERT_EQ(event_loop_thread_count_from_config(), 0);
}

TEST(S3ApiEventLoopThreads, NegativeValuesSelectTheSdkDefault) {
    ScopedConfig config("AWS.EventLoopThreads", -1);
    ASSERT_EQ(event_loop_thread_count_from_config(), 0);
}

TEST(S3ApiEventLoopThreads, SaturatesAtTheMaximumEventLoopGroupSize) {
    ScopedConfig config("AWS.EventLoopThreads", 1'000'000);
    ASSERT_EQ(event_loop_thread_count_from_config(), std::numeric_limits<uint16_t>::max());
}

TEST(S3ApiEventLoopThreads, ApiInstanceOverridesTheSdkClientBootstrap) {
    const auto& io_options = S3ApiInstance::instance()->options().ioOptions;
    ASSERT_TRUE(static_cast<bool>(io_options.clientBootstrap_create_fn));
}

TEST(S3ApiEventLoopThreads, FactoryProducesAValidClientBootstrap) {
    S3ApiInstance::instance();
    auto client_bootstrap = make_client_bootstrap_factory(1)();
    ASSERT_TRUE(client_bootstrap);
    ASSERT_TRUE(static_cast<bool>(*client_bootstrap));
}

} // namespace arcticdb::storage::s3
