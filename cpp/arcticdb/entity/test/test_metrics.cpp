/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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

TEST(Metrics, OverwriteFamilyLabelsNotPossible) {
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Counter, "name", "help", {{"env", "dev"}});
    ASSERT_THROW(instance.incrementCounter("name", {{"env", "pre"}}), std::invalid_argument);
    ASSERT_THROW(instance.incrementCounter("name", {{"env", "pre"}, {"another", "label"}}), std::invalid_argument);
    instance.incrementCounter("name", {{"a", "bcd"}});
}

TEST(Metrics, RegisterTwice) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Counter, "name", "help");
    instance.registerMetric(prometheus::MetricType::Counter, "name", "help");
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

TEST(Metrics, RemoveCounter) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Counter, "name", "help");

    // when
    instance.incrementCounter("name", {{"a", "bcd"}});
    instance.incrementCounter("name", {{"a", "efg"}});
    instance.removeMetric<prometheus::Counter>("name", {{"a", "efg"}});
    instance.removeMetric<prometheus::Counter>("other_name", {}); // don't complain

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res.at(0).name, "name");
    auto metric = res.at(0).metric;
    ASSERT_EQ(metric.size(), 1);
    ASSERT_EQ(metric.at(0).label.at(0).name, "a");
    ASSERT_EQ(metric.at(0).label.at(0).value, "bcd");
    ASSERT_EQ(metric.at(0).counter.value, 1.0);
}

TEST(Metrics, RemoveCounterLabelsOutOfOrder) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Counter, "name", "help");

    // when
    instance.incrementCounter("name", {{"a", "bcd"}, {"e", "fgh"}});
    instance.removeMetric<prometheus::Counter>("name", {{"e", "fgh"}, {"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_TRUE(res.empty());
}

TEST(Metrics, RemoveCounterSpecific) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Counter, "name", "help");

    // when
    instance.incrementCounter("name", 3.0, {{"a", "bcd"}});
    instance.incrementCounter("name", 4.0, {{"a", "bcd"}});
    instance.incrementCounter("name", 2.0, {{"a", "efg"}});
    instance.removeMetric<prometheus::Counter>("name", {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res.at(0).name, "name");
    auto metric = res.at(0).metric;
    ASSERT_EQ(metric.size(), 1);
    ASSERT_EQ(metric.at(0).counter.value, 2.0);
}

TEST(Metrics, RemoveFamilyLabelsNotConsidered) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(
            prometheus::MetricType::Counter, "name", "help", {{"env", "dev"}, {"day_of_week", "monday"}}
    );

    // when
    instance.incrementCounter("name", {{"a", "bcd"}});
    instance.removeMetric<prometheus::Counter>("name", {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_TRUE(res.empty());
}

TEST(Metrics, Gauge) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Gauge, "name", "help");

    // when
    instance.setGauge("name", 4.0, {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res.at(0).name, "name");
    auto metric = res.at(0).metric;
    ASSERT_EQ(metric.size(), 1);
    ASSERT_EQ(metric.at(0).gauge.value, 4.0);
}

TEST(Metrics, RemoveGauge) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Gauge, "name", "help");

    // when
    instance.setGauge("name", 4.0, {{"a", "bcd"}});
    instance.removeMetric<prometheus::Gauge>("name", {{"a", "bcd"}});
    instance.removeMetric<prometheus::Gauge>("other_name", {}); // don't complain

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 0);
}

TEST(Metrics, RemoveMultipleGauges) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Gauge, "name", "help");
    instance.registerMetric(prometheus::MetricType::Gauge, "other_name", "help");

    // when
    instance.setGauge("name", 4.0, {{"a", "bcd"}});
    instance.setGauge("other_name", 4.0, {{"a", "bcd"}});
    instance.setGauge("name", 4.0, {{"b", "bcd"}});
    instance.setGauge("other_name", 4.0, {{"bcd", "a"}});
    instance.removeMetric<prometheus::Gauge>("other_name", {{"bcd", "a"}});
    instance.removeMetric<prometheus::Gauge>("name", {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 2);

    auto first_metric = res.at(0).metric;
    auto second_metric = res.at(1).metric;
    auto expected_one = prometheus::ClientMetric::Label("a", "bcd");
    auto expected_two = prometheus::ClientMetric::Label("b", "bcd");

    auto actual_label_one = first_metric.at(0).label.at(0);
    auto actual_label_two = second_metric.at(0).label.at(0);
    ASSERT_NE(actual_label_one, actual_label_two);
    ASSERT_TRUE(actual_label_one == expected_one || actual_label_one == expected_two);
    ASSERT_TRUE(actual_label_two == expected_one || actual_label_two == expected_two);
}

TEST(Metrics, RemoveGaugeLabelsChecked) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Gauge, "name", "help");

    // when
    instance.setGauge("name", 4.0, {{"a", "bcd"}});
    instance.removeMetric<prometheus::Gauge>("name", {{"a", "zzz"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
}

TEST(Metrics, RemoveGaugeNameFiltering) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Gauge, "name", "help");

    // when
    instance.setGauge("name", 4.0, {{"a", "bcd"}});
    instance.removeMetric<prometheus::Gauge>("not_this_name", {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
}

TEST(Metrics, Summary) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Summary, "name", "help");
    instance.registerMetric(prometheus::MetricType::Summary, "other_name", "help");

    // when
    instance.observeSummary("name", 4.0, {{"a", "bcd"}});
    instance.observeSummary("name", 6.0, {{"a", "bcd"}});
    instance.observeSummary("other_name", 3.0, {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 2);
    ASSERT_EQ(res.at(0).name, "name");
    auto metric = res.at(0).metric;
    ASSERT_EQ(metric.size(), 1);
    ASSERT_EQ(metric.at(0).summary.sample_sum, 10.0);
    ASSERT_EQ(metric.at(0).summary.sample_count, 2);
}

TEST(Metrics, RemoveSummary) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Summary, "name", "help");
    instance.registerMetric(prometheus::MetricType::Summary, "other_name", "help");

    // when
    instance.observeSummary("name", 4.0, {{"a", "bcd"}});
    instance.observeSummary("name", 6.0, {{"a", "bcd"}});
    instance.observeSummary("other_name", 3.0, {{"a", "bcd"}});
    instance.removeMetric<prometheus::Summary>("name", {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res.at(0).name, "other_name");
}

TEST(Metrics, RemoveSummaryAllLabelsChecked) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Summary, "name", "help");
    instance.registerMetric(prometheus::MetricType::Summary, "other_name", "help");

    // when
    instance.observeSummary("name", 4.0, {{"a", "bcd"}, {"e", "ijk"}});

    // The "e" label doesn't match so don't remove anything
    instance.removeMetric<prometheus::Summary>("name", {{"a", "bcd"}, {"e", "fgh"}});
    instance.removeMetric<prometheus::Summary>("name", {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
}

TEST(Metrics, Histogram) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Histogram, "name", "help");
    instance.registerMetric(prometheus::MetricType::Histogram, "other_name", "help");

    // when
    instance.observeHistogram("name", 4.0, {{"a", "bcd"}});
    instance.observeHistogram("name", 6.0, {{"a", "bcd"}});
    instance.observeHistogram("other_name", 3.0, {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 2);
    ASSERT_EQ(res.at(0).name, "name");
    auto metric = res.at(0).metric;
    ASSERT_EQ(metric.size(), 1);
    ASSERT_EQ(metric.at(0).histogram.sample_sum, 10.0);
    ASSERT_EQ(metric.at(0).histogram.sample_count, 2);
}

TEST(Metrics, RemoveHistogram) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Histogram, "name", "help");
    instance.registerMetric(prometheus::MetricType::Histogram, "other_name", "help");

    // when
    instance.observeHistogram("name", 4.0, {{"a", "bcd"}});
    instance.observeHistogram("name", 6.0, {{"a", "bcd"}});
    instance.observeHistogram("other_name", 3.0, {{"a", "bcd"}});
    instance.removeMetric<prometheus::Histogram>("name", {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res.at(0).name, "other_name");
}

TEST(Metrics, RemoveHistogramLabelsChecked) {
    // given
    PrometheusInstance instance{};
    instance.configure(MetricsConfig{"host", "port", "job", "instance", "local", MetricsConfig::Model::PUSH});
    instance.registerMetric(prometheus::MetricType::Histogram, "name", "help");
    instance.registerMetric(prometheus::MetricType::Histogram, "other_name", "help");

    // when
    instance.observeHistogram("name", 4.0, {{"a", "bcd"}});
    instance.observeHistogram("name", 6.0, {{"a", "efg"}});
    instance.observeHistogram("other_name", 3.0, {{"a", "bcd"}});
    instance.removeMetric<prometheus::Histogram>("name", {{"a", "bcd"}});

    // then
    auto res = instance.get_metrics();
    ASSERT_EQ(res.size(), 2);
    ASSERT_EQ(res.at(0).name, "name");
    auto metric = res.at(0).metric;
    ASSERT_EQ(metric.at(0).histogram.sample_sum, 6.0);
    ASSERT_EQ(metric.at(0).label.at(0).value, "efg");
}
