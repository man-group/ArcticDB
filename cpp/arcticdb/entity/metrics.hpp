/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <prometheus/counter.h>
#include <prometheus/gateway.h>
#include <prometheus/registry.h>
#include <prometheus/summary.h>
#include <prometheus/exposer.h>
#include <prometheus/family.h>
#include <prometheus/histogram.h>
#include <fmt/format.h>
#include <map>
#include <unordered_map>
#include <memory>
#include <utility>
#include <arcticdb/util/preconditions.hpp>
#include <folly/hash/Hash.h>

namespace arcticdb {

const std::string MONGO_INSTANCE_LABEL = "mongo_instance";
const std::string PROMETHEUS_ENV_LABEL = "env";
constexpr int SUMMARY_MAX_AGE = 30;
constexpr int SUMMARY_AGE_BUCKETS = 5;

class MetricsConfig {
  public:
    enum class Model { NO_INIT, PUSH, PULL };
    MetricsConfig() : model_(Model::NO_INIT) {}

    MetricsConfig(
            const std::string& host, const std::string& port, const std::string& job_name, const std::string& instance,
            const std::string& prometheus_env, const Model model
    ) :
        host(host),
        port(port),
        job_name(job_name),
        instance(instance),
        prometheus_env(prometheus_env),
        model_(model) {
        util::check(!host.empty(), "MetricsConfig: host is empty");
        util::check(!port.empty(), "MetricsConfig: port is empty");
        util::check(!job_name.empty(), "MetricsConfig: job_name is empty");
        util::check(!instance.empty(), "MetricsConfig: instance is empty");
        util::check(!prometheus_env.empty(), "MetricsConfig: instance is empty");
        util::check(!prometheus_env.empty(), "MetricsConfig: prometheus_env is empty");
    }

    std::string host;
    std::string port;
    std::string job_name;
    std::string instance;
    std::string prometheus_env;
    Model model_;
};

class PrometheusInstance {
  public:
    using Labels = prometheus::Labels;

    static std::shared_ptr<PrometheusInstance> instance();

    PrometheusInstance();

    static std::shared_ptr<PrometheusInstance> instance_;
    static std::once_flag init_flag_;

    static void init() { instance_ = std::make_shared<PrometheusInstance>(); }

    static void destroy_instance() { instance_.reset(); };

    void registerMetric(
            prometheus::MetricType type, const std::string& name, const std::string& help, const Labels& labels = {},
            const std::vector<double>& buckets_list = {}
    );

    // Remove the given metric from the registry, so that subsequent pulls or pushes will not include it.
    template<typename T>
    void removeMetric(const std::string& name, const Labels& labels) {
        std::scoped_lock lock{metrics_mutex_};
        static_assert(
                T::metric_type == prometheus::MetricType::Counter ||
                        T::metric_type == prometheus::MetricType::Histogram ||
                        T::metric_type == prometheus::MetricType::Gauge ||
                        T::metric_type == prometheus::MetricType::Summary,
                "Unimplemented metric type"
        );

        MetricFamilyMap<T> metrics_family;
        MetricMap<T> metrics;
        if constexpr (T::metric_type == prometheus::MetricType::Counter) {
            metrics_family = map_counter_;
            metrics = all_counters_;
        } else if constexpr (T::metric_type == prometheus::MetricType::Histogram) {
            for (const auto& [histogram_name, value] : map_histogram_) {
                metrics_family[histogram_name] = value.histogram_;
            }
            metrics = all_histograms_;
        } else if constexpr (T::metric_type == prometheus::MetricType::Gauge) {
            metrics_family = map_gauge_;
            metrics = all_gauges_;
        } else if constexpr (T::metric_type == prometheus::MetricType::Summary) {
            metrics_family = map_summary_;
            metrics = all_summaries_;
        }

        const auto it = metrics_family.find(name);
        if (it == metrics_family.end()) {
            return;
        }

        prometheus::Family<T>* family = it->second;

        const auto metric_to_remove = metrics.find({name, labels});
        if (metric_to_remove == metrics.end()) {
            return;
        }

        family->Remove(metric_to_remove->second);
        metrics.erase({name, labels});
    }

    // update pre-registered metrics with optional instance labels.  Each unique set of labels generates a new metric
    // instance
    void incrementCounter(const std::string& name, const Labels& labels = {});
    void incrementCounter(const std::string& name, double value, const Labels& labels = {});
    void setGauge(const std::string& name, double value, const Labels& labels = {});
    void setGaugeCurrentTime(const std::string& name, const Labels& labels = {});
    // set new value for histogram with optional labels
    void observeHistogram(const std::string& name, double value, const Labels& labels = {});
    // set new value for summary with optional labels
    void observeSummary(const std::string& name, double value, const Labels& labels = {});

    int push();

    void configure(const MetricsConfig& config);

    // Intended for testing.
    std::vector<prometheus::MetricFamily> get_metrics();

    MetricsConfig cfg_;

  private:
    struct HistogramInfo {
        HistogramInfo() = default;

        HistogramInfo(
                prometheus::Family<prometheus::Histogram>* histogram,
                prometheus::Histogram::BucketBoundaries buckets_list
        ) :
            histogram_(histogram),
            buckets_list_(std::move(buckets_list)) {}

        prometheus::Family<prometheus::Histogram>* histogram_ = nullptr;
        prometheus::Histogram::BucketBoundaries buckets_list_;
    };

    template<typename T>
    using MetricFamilyMap = std::unordered_map<std::string, prometheus::Family<T>*>;

    struct MetricKey {
        std::string name;
        Labels labels;

        bool operator==(const MetricKey&) const = default;
    };

    struct MetricKeyHash {
        std::size_t operator()(const MetricKey& key) const noexcept {
            auto labels_hash = folly::hash::commutative_hash_combine_range(key.labels.begin(), key.labels.end());
            return folly::hash::commutative_hash_combine(labels_hash, key.name);
        }
    };

    template<typename T>
    using MetricMap = std::unordered_map<MetricKey, T*, MetricKeyHash>;

    static std::string getHostName();

    std::shared_ptr<prometheus::Registry> registry_;
    std::shared_ptr<prometheus::Exposer> exposer_;

    MetricFamilyMap<prometheus::Counter> map_counter_;
    MetricMap<prometheus::Counter> all_counters_;

    MetricFamilyMap<prometheus::Gauge> map_gauge_;
    MetricMap<prometheus::Gauge> all_gauges_;

    std::unordered_map<std::string, HistogramInfo> map_histogram_;
    MetricMap<prometheus::Histogram> all_histograms_;

    MetricFamilyMap<prometheus::Summary> map_summary_;
    MetricMap<prometheus::Summary> all_summaries_;

    std::string mongo_instance_;
    std::shared_ptr<prometheus::Gateway> gateway_;
    bool configured_;

    std::mutex metrics_mutex_;
};

inline void log_prometheus_gauge(const std::string& metric_name, const std::string& metric_desc, size_t val) {
    PrometheusInstance::instance()->registerMetric(prometheus::MetricType::Gauge, metric_name, metric_desc, {});
    PrometheusInstance::instance()->setGauge(metric_name, static_cast<double>(val));
}

inline void log_prometheus_counter(const std::string& metric_name, const std::string& metric_desc, size_t val) {
    PrometheusInstance::instance()->registerMetric(prometheus::MetricType::Counter, metric_name, metric_desc, {});
    PrometheusInstance::instance()->incrementCounter(metric_name, static_cast<double>(val));
}

} // Namespace arcticdb

template<>
struct fmt::formatter<arcticdb::MetricsConfig> {

    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::MetricsConfig& k, FormatContext& ctx) const {
        return fmt::format_to(
                ctx.out(),
                "MetricsConfig: host={}, port={}, job_name={}, instance={}, prometheus_env={}, model={}",
                k.host,
                k.port,
                k.job_name,
                k.instance,
                k.prometheus_env,
                static_cast<int>(k.model_)
        );
    }
};