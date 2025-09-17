/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/log/log.hpp>

#ifdef _WIN32
#include <Winsock.h> // for gethostname
#else
#include <sys/param.h>
#include <unistd.h>
#endif

using namespace prometheus;

namespace arcticdb {

std::shared_ptr<PrometheusInstance> PrometheusInstance::instance() {
    std::call_once(PrometheusInstance::init_flag_, &PrometheusInstance::init);
    return PrometheusInstance::instance_;
}

std::shared_ptr<PrometheusInstance> PrometheusInstance::instance_;
std::once_flag PrometheusInstance::init_flag_;

PrometheusInstance::PrometheusInstance() : configured_(false) {
    arcticdb::log::version().debug("PrometheusInstance created");
}

void PrometheusInstance::configure(const MetricsConfig& config) {
    if (configured_) {
        arcticdb::log::version().warn("Prometheus already configured; Existing setting will be used");
        if (config.host != cfg_.host) {
            arcticdb::log::version().warn(
                    "New Prometheus host is different from the existing: {} vs {}", config.host, cfg_.host
            );
        }
        if (config.port != cfg_.port) {
            arcticdb::log::version().warn(
                    "New Prometheus port is different from the existing: {} vs {}", config.port, cfg_.port
            );
        }
        if (config.job_name != cfg_.job_name) {
            arcticdb::log::version().warn(
                    "New Prometheus job_name is different from the existing: {} vs {}", config.job_name, cfg_.job_name
            );
        }
        if (config.instance != cfg_.instance) {
            arcticdb::log::version().warn(
                    "New Prometheus instance is different from the existing: {} vs {}", config.instance, cfg_.instance
            );
        }
        if (config.prometheus_env != cfg_.prometheus_env) {
            arcticdb::log::version().warn(
                    "New Prometheus env is different from the existing: {} vs {}",
                    config.prometheus_env,
                    cfg_.prometheus_env
            );
        }
        if (config.model_ != cfg_.model_) {
            arcticdb::log::version().warn(
                    "New Prometheus model is different from the existing: {} vs {}",
                    static_cast<int>(config.model_),
                    static_cast<int>(cfg_.model_)
            );
        }
        return;
    }

    cfg_ = config;

    if (cfg_.model_ == MetricsConfig::Model::PUSH) {
        // IMP: This is the GROUPING_KEY - every push overwrites the previous grouping key
        auto labels = prometheus::Gateway::GetInstanceLabel(getHostName());
        mongo_instance_ = cfg_.instance;
        labels.try_emplace(MONGO_INSTANCE_LABEL, mongo_instance_);
        labels.try_emplace(PROMETHEUS_ENV_LABEL, cfg_.prometheus_env);
        gateway_ = std::make_shared<prometheus::Gateway>(cfg_.host, cfg_.port, cfg_.job_name, labels);
        registry_ = std::make_shared<prometheus::Registry>();
        gateway_->RegisterCollectable(registry_);

        arcticdb::log::version().info("Prometheus Push created with settings {}", cfg_);

    } else if (cfg_.model_ == MetricsConfig::Model::PULL) {

        // create an http server ie "http://hostname:"+port()+"/metrics"
        std::string endpoint = cfg_.host + ":" + cfg_.port;

        if (exposer_.use_count() > 0) {
            exposer_->RemoveCollectable(registry_, "/metrics");
            exposer_.reset();
        }

        // default to 2 threads
        exposer_ = std::make_shared<prometheus::Exposer>(endpoint, 2);

        // create a metrics registry with component=main labels applied to all its
        registry_ = std::make_shared<prometheus::Registry>();

        // 2nd arg defaults to /metrics, make explicit or parameterise
        exposer_->RegisterCollectable(registry_, "/metrics");

        arcticdb::log::version().info("Prometheus endpoint created on {}/metrics", endpoint);
    } else {
        arcticdb::log::version().info("Prometheus not configured {}", cfg_);
    }

    configured_ = true;
}

void PrometheusInstance::registerMetric(
        prometheus::MetricType type, const std::string& name, const std::string& help, const Labels& labels,
        const std::vector<double>& buckets_list
) {
    if (registry_.use_count() == 0) {
        return;
    }

    std::scoped_lock lock{metrics_mutex_};
    if (type == prometheus::MetricType::Counter) {
        map_counter_[name] = &prometheus::BuildCounter().Name(name).Help(help).Labels(labels).Register(*registry_);
    } else if (type == prometheus::MetricType::Gauge) {
        map_gauge_[name] = &prometheus::BuildGauge().Name(name).Help(help).Labels(labels).Register(*registry_);
    } else if (type == prometheus::MetricType::Histogram) {
        prometheus::Family<prometheus::Histogram>* histogram =
                &prometheus::BuildHistogram().Name(name).Help(help).Labels(labels).Register(*registry_);
        map_histogram_[name] = HistogramInfo(histogram, buckets_list);
    } else if (type == prometheus::MetricType::Summary) {
        map_summary_[name] = &prometheus::BuildSummary().Name(name).Help(help).Labels(labels).Register(*registry_);
    } else {
        util::raise_rte("Unsupported metric type");
    }
}

void PrometheusInstance::incrementCounter(const std::string& name, double value, const Labels& labels) {
    if (registry_.use_count() == 0)
        return;

    std::scoped_lock lock{metrics_mutex_};
    if (const auto it = map_counter_.find(name); it != map_counter_.end()) {
        Counter* counter = &it->second->Add(labels);
        all_counters_.insert({{name, labels}, counter});
        counter->Increment(value);
    } else {
        arcticdb::log::version().warn("Unregistered counter metric {}", name);
    }
}

void PrometheusInstance::incrementCounter(const std::string& name, const Labels& labels) {
    if (registry_.use_count() == 0)
        return;

    std::scoped_lock lock{metrics_mutex_};
    if (const auto it = map_counter_.find(name); it != map_counter_.end()) {
        Counter* counter = &it->second->Add(labels);
        all_counters_.insert({{name, labels}, counter});
        counter->Increment();
    } else {
        arcticdb::log::version().warn("Unregistered counter metric {}", name);
    }
}

void PrometheusInstance::setGauge(const std::string& name, double value, const Labels& labels) {
    if (registry_.use_count() == 0)
        return;

    std::scoped_lock lock{metrics_mutex_};
    if (const auto it = map_gauge_.find(name); it != map_gauge_.end()) {
        Gauge* gauge = &it->second->Add(labels);
        all_gauges_.insert({{name, labels}, gauge});
        gauge->Set(value);
    } else {
        arcticdb::log::version().warn("Unregistered gauge metric {}", name);
    }
}

void PrometheusInstance::setGaugeCurrentTime(const std::string& name, const Labels& labels) {
    if (registry_.use_count() == 0)
        return;

    std::scoped_lock lock{metrics_mutex_};
    if (const auto it = map_gauge_.find(name); it != map_gauge_.end()) {
        Gauge* gauge = &it->second->Add(labels);
        all_gauges_.insert({{name, labels}, gauge});
        gauge->SetToCurrentTime();
    } else {
        arcticdb::log::version().warn("Unregistered gauge metric {}", name);
    }
}

void PrometheusInstance::observeHistogram(const std::string& name, double value, const Labels& labels) {
    if (registry_.use_count() == 0)
        return;

    std::scoped_lock lock{metrics_mutex_};
    if (const auto it = map_histogram_.find(name); it != map_histogram_.end()) {
        Histogram* histogram = &it->second.histogram_->Add(labels, it->second.buckets_list_);
        all_histograms_.insert({{name, labels}, histogram});
        histogram->Observe(value);
    } else {
        arcticdb::log::version().warn("Unregistered Histogram metric {}", name);
    }
}

void PrometheusInstance::observeSummary(const std::string& name, double value, const Labels& labels) {
    if (registry_.use_count() == 0)
        return;

    std::scoped_lock lock{metrics_mutex_};
    if (const auto it = map_summary_.find(name); it != map_summary_.end()) {
        Summary* summary = &it->second->Add(
                labels,
                Summary::Quantiles{
                        {0.1, 0.05},
                        {0.2, 0.05},
                        {0.3, 0.05},
                        {0.4, 0.05},
                        {0.5, 0.05},
                        {0.6, 0.05},
                        {0.7, 0.05},
                        {0.8, 0.05},
                        {0.9, 0.05},
                        {0.9, 0.05},
                        {1.0, 0.05}
                },
                std::chrono::seconds{SUMMARY_MAX_AGE},
                SUMMARY_AGE_BUCKETS
        );
        all_summaries_.insert({{name, labels}, summary});
        summary->Observe(value);
    } else {
        arcticdb::log::version().warn("Unregistered summary metric {}", name);
    }
}

std::string PrometheusInstance::getHostName() {
    char hostname[1024];
    if (::gethostname(hostname, sizeof(hostname))) {
        return {};
    }
    return hostname;
}

int PrometheusInstance::push() {
    if (gateway_.use_count() > 0) {
        return gateway_->PushAdd();
    } else {
        return 0;
    }
}

std::vector<prometheus::MetricFamily> PrometheusInstance::get_metrics() {
    util::check(registry_, "registry_ not set");
    return registry_->Collect();
}

} // Namespace arcticdb
