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
#include <fmt/format.h>

namespace arcticdb {

const std::string MONGO_INSTANCE_LABEL = "mongo_instance";
const std::string PROMETHEUS_ENV_LABEL = "env";
const int SUMMARY_MAX_AGE = 30;
const int SUMMARY_AGE_BUCKETS = 5;

class PrometheusConfig {
public:
    enum class Model {
        NO_INIT,
        PUSH,
        PULL
    };
    PrometheusConfig() : model_(Model::NO_INIT) {}

    PrometheusConfig(const std::string& host,
                     const std::string& port,
                     const std::string& job_name, 
                     const std::string& instance, 
                     const std::string& prometheus_env, 
                     const Model model)
            : host(host)
            , port(port)
            , job_name(job_name)
            , instance(instance)
            , prometheus_env(prometheus_env)
            , model_(model) {
                util::check(!host.empty(), "PrometheusConfig: host is empty");
                util::check(!port.empty(), "PrometheusConfig: port is empty");
                util::check(!job_name.empty(), "PrometheusConfig: job_name is empty");
                util::check(!instance.empty(), "PrometheusConfig: instance is empty");
                util::check(!prometheus_env.empty(), "PrometheusConfig: instance is empty");
                util::check(!prometheus_env.empty(), "PrometheusConfig: prometheus_env is empty");
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
    static std::shared_ptr<PrometheusInstance> instance();

    PrometheusInstance();

    static std::shared_ptr<PrometheusInstance> instance_;
    static std::once_flag init_flag_;

    static void init() {
        instance_ = std::make_shared<PrometheusInstance>();

    }

    static void destroy_instance() { instance_.reset(); };

    // register a metric with optional static labels
    void registerMetric( prometheus::MetricType type, const std::string& name, const std::string& help, const std::map<std::string, std::string>& staticLabels = {}, const std::vector<double>& buckets_list = {});
    // update pre-registered metrics with optional instance labels.  Each unique set of labels generates a new metric instance
    void incrementCounter(const std::string& name, const std::map<std::string, std::string>& labels = {});
    void incrementCounter(const std::string &name, double value, const std::map<std::string, std::string>& labels = {});
    void setGauge(const std::string &name, double value, const std::map<std::string, std::string>& labels = {});
    void setGaugeCurrentTime(const std::string &name, const std::map<std::string, std::string>& labels = {});
    // set new value for histogram with optional labels
    void observeHistogram(const std::string &name, double value, const std::map<std::string, std::string>& labels = {});
    // Delete current histogram with optional labels
    void DeleteHistogram(const std::string &name, const std::map<std::string, std::string>& labels = {});
    // set new value for summary with optional labels
    void observeSummary(const std::string &name, double value, const std::map<std::string, std::string>& labels = {});

    int push();

    void configure(const PrometheusConfig& config, const bool reconfigure = false);

    private:

        PrometheusConfig cfg_;

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
        bool configured_;

};

inline void log_prometheus_gauge(const std::string& metric_name, const std::string& metric_desc, size_t val) {
    PrometheusInstance::instance()->registerMetric(
            prometheus::MetricType::Gauge,
            metric_name, metric_desc,
            {});
    PrometheusInstance::instance()->setGauge(metric_name, static_cast<double>(val));
}

inline void log_prometheus_counter(const std::string& metric_name, const std::string& metric_desc, size_t val) {
    PrometheusInstance::instance()->registerMetric(
            prometheus::MetricType::Counter,
            metric_name, metric_desc,
            {});
    PrometheusInstance::instance()->incrementCounter(metric_name, static_cast<double>(val));
}

} // Namespace arcticdb

template<>
struct fmt::formatter<arcticdb::PrometheusConfig> {

    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::PrometheusConfig k, FormatContext &ctx) const {
        return  fmt::format_to(ctx.out(), "PrometheusConfig: host={}, port={}, job_name={}, instance={}, prometheus_env={}, model={}",
                               k.host, k.port, k.job_name, k.instance, k.prometheus_env, static_cast<int>(k.model_));
    }
};