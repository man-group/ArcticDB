/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <prometheus/counter.h>
#include <prometheus/gateway.h>
#include <prometheus/registry.h>
#include <prometheus/summary.h>
#include <prometheus/exposer.h>
#include <prometheus/family.h>
#include <prometheus/histogram.h>

#ifndef _WIN32
#include <sys/param.h>
#include <unistd.h>
#endif

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/timer.hpp>
#include <map>
#include <unordered_map>
#include <memory>

namespace arcticdb {

const std::string MONGO_INSTANCE_LABEL = "mongo_instance";
const std::string PROMETHEUS_ENV_LABEL = "env";
const int SUMMARY_MAX_AGE = 30;
const int SUMMARY_AGE_BUCKETS = 5;

class PrometheusInstance {
public:
    static std::shared_ptr<PrometheusInstance> instance();

    PrometheusInstance();

    static std::shared_ptr<PrometheusInstance> instance_;
    static std::once_flag init_flag_;

    static void init()
    {
        instance_ = std::make_shared<PrometheusInstance>();
    }

    static void destroy_instance()
    {
        instance_.reset();
    };

    // register a metric with optional static labels
    void registerMetric(prometheus::MetricType type,
        const std::string& name,
        const std::string& help,
        const std::map<std::string, std::string>& staticLabels = {},
        const std::vector<double>& buckets_list = {});
    // update pre-registered metrics with optional instance labels.  Each unique set of labels generates a new metric instance
    void incrementCounter(const std::string& name, const std::map<std::string, std::string>& labels = {});
    void incrementCounter(const std::string& name, double value, const std::map<std::string, std::string>& labels = {});
    void setGauge(const std::string& name, double value, const std::map<std::string, std::string>& labels = {});
    void setGaugeCurrentTime(const std::string& name, const std::map<std::string, std::string>& labels = {});
    // set new value for histogram with optional labels
    void observeHistogram(const std::string& name, double value, const std::map<std::string, std::string>& labels = {});
    // Delete current histogram with optional labels
    void DeleteHistogram(const std::string& name, const std::map<std::string, std::string>& labels = {});
    // set new value for summary with optional labels
    void observeSummary(const std::string& name, double value, const std::map<std::string, std::string>& labels = {});

    int push();

private:
    struct HistogramInfo {
        prometheus::Family<prometheus::Histogram>* histogram;
        prometheus::Histogram::BucketBoundaries buckets_list;
    };

    static std::string getHostName();

    std::shared_ptr<prometheus::Registry> registry_;
    std::shared_ptr<prometheus::Exposer> exposer_;
    // cannot use ref type, so use pointer
    std::unordered_map<std::string, prometheus::Family<prometheus::Counter>*> map_counter_;
    std::unordered_map<std::string, prometheus::Family<prometheus::Gauge>*> map_gauge_;
    std::unordered_map<std::string, HistogramInfo> map_histogram_;
    prometheus::Histogram::BucketBoundaries buckets_list_;

    std::unordered_map<std::string, prometheus::Family<prometheus::Summary>*> map_summary_;
    // push gateway
    std::string mongo_instance_;
    std::shared_ptr<prometheus::Gateway> gateway_;
};

class PrometheusConfigInstance {
public:
    static std::shared_ptr<PrometheusConfigInstance> instance();

    using Proto = arcticdb::proto::utils::PrometheusConfig;
    Proto config;
    static std::shared_ptr<PrometheusConfigInstance> instance_;
    static std::once_flag init_flag_;

    static void init()
    {
        instance_ = std::make_shared<PrometheusConfigInstance>();
    }
};

inline void log_prometheus_gauge(const std::string& metric_name, const std::string& metric_desc, size_t val)
{
    PrometheusInstance::instance()->registerMetric(prometheus::MetricType::Gauge, metric_name, metric_desc, {});
    PrometheusInstance::instance()->setGauge(metric_name, static_cast<double>(val));
}

inline void log_prometheus_counter(const std::string& metric_name, const std::string& metric_desc, size_t val)
{
    PrometheusInstance::instance()->registerMetric(prometheus::MetricType::Counter, metric_name, metric_desc, {});
    PrometheusInstance::instance()->incrementCounter(metric_name, static_cast<double>(val));
}

} // Namespace arcticdb
