/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>

#include <arcticdb/entity/metrics.hpp>

using namespace arcticdb;

TEST(Metrics, IncrementCounterDefault) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Counter, "name", "help");

    // when
    instance.incrementCounter("name", {{"a", "bcd"}});
    instance.incrementCounter("name", {{"a", "bcd"}});
    instance.incrementCounter("name", {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res.at(0).name, "name");
    auto metric = res.at(0).metric;
    ASSERT_EQ(metric.size(), 1);
    ASSERT_EQ(metric.at(0).counter.value, 3.0);
}

TEST(Metrics, IncrementCounterSpecific) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Counter, "name", "help");

    // when
    instance.incrementCounter("name", 3.0, {{"a", "bcd"}});
    instance.incrementCounter("name", 4.0, {{"a", "bcd"}});
    instance.incrementCounter("name", 5.0, {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res.at(0).name, "name");
    auto metric = res.at(0).metric;
    ASSERT_EQ(metric.size(), 1);
    ASSERT_EQ(metric.at(0).counter.value, 12.0);
}
