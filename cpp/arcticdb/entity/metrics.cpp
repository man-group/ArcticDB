/* Copyright 2026 Man Group Operations Limited
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
    {
        std::scoped_lock lock{metrics_mutex_};
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
                        "New Prometheus job_name is different from the existing: {} vs {}",
                        config.job_name,
                        cfg_.job_name
                );
            }
            if (config.instance != cfg_.instance) {
                arcticdb::log::version().warn(
                        "New Prometheus instance is different from the existing: {} vs {}",
                        config.instance,
                        cfg_.instance
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
    }

    std::shared_ptr<prometheus::Gateway> new_gateway;
    std::shared_ptr<prometheus::Exposer> new_exposer;
    std::shared_ptr<prometheus::Registry> new_registry;
    std::string endpoint;

    if (config.model_ == MetricsConfig::Model::PUSH) {
        // IMP: This is the GROUPING_KEY - every push overwrites the previous grouping key
        auto labels = prometheus::Gateway::GetInstanceLabel(getHostName());
        labels.try_emplace(MONGO_INSTANCE_LABEL, config.instance);
        labels.try_emplace(PROMETHEUS_ENV_LABEL, config.prometheus_env);
        new_gateway = std::make_shared<prometheus::Gateway>(config.host, config.port, config.job_name, labels);
        new_registry = std::make_shared<prometheus::Registry>();
        new_gateway->RegisterCollectable(new_registry);
    } else if (config.model_ == MetricsConfig::Model::PULL) {
        // create an http server ie "http://hostname:"+port()+"/metrics"
        endpoint = config.host + ":" + config.port;
        // default to 2 threads
        new_exposer = std::make_shared<prometheus::Exposer>(endpoint, 2);
        // create a metrics registry with component=main labels applied to all its
        new_registry = std::make_shared<prometheus::Registry>();
        // 2nd arg defaults to /metrics, make explicit or parameterise
        new_exposer->RegisterCollectable(new_registry, "/metrics");
    }

    std::shared_ptr<prometheus::Exposer> old_exposer;
    std::shared_ptr<prometheus::Registry> old_registry;
    {
        std::scoped_lock lock{metrics_mutex_};
        if (configured_) {
            arcticdb::log::version().warn("Prometheus already configured; Existing setting will be used");
            return;
        }
        cfg_ = config;
        if (cfg_.model_ == MetricsConfig::Model::PUSH) {
            mongo_instance_ = cfg_.instance;
            gateway_ = std::move(new_gateway);
            registry_ = std::move(new_registry);
            for (const auto& c : extra_collectables_) {
                if (!c.expired()) {
                    gateway_->RegisterCollectable(c);
                }
            }
        } else if (cfg_.model_ == MetricsConfig::Model::PULL) {
            old_exposer = std::move(exposer_);
            old_registry = std::move(registry_);
            exposer_ = std::move(new_exposer);
            registry_ = std::move(new_registry);
            for (const auto& c : extra_collectables_) {
                if (!c.expired()) {
                    exposer_->RegisterCollectable(c, "/metrics");
                }
            }
        }
        configured_ = true;
    }

    if (old_exposer && old_registry) {
        old_exposer->RemoveCollectable(old_registry, "/metrics");
    }

    if (config.model_ == MetricsConfig::Model::PUSH) {
        arcticdb::log::version().info("Prometheus Push created with settings {}", config);
    } else if (config.model_ == MetricsConfig::Model::PULL) {
        arcticdb::log::version().info("Prometheus endpoint created on {}/metrics", endpoint);
    } else {
        arcticdb::log::version().info("Prometheus not configured {}", config);
    }
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
    std::shared_ptr<prometheus::Gateway> gw;
    {
        std::scoped_lock lock{metrics_mutex_};
        gw = gateway_;
    }
    if (gw) {
        return gw->PushAdd();
    }
    return 0;
}

void PrometheusInstance::registerCollectable(const std::weak_ptr<prometheus::Collectable>& collectable) {
    std::scoped_lock lock{metrics_mutex_};
    std::erase_if(extra_collectables_, [](const auto& w) { return w.expired(); });
    for (const auto& existing : extra_collectables_) {
        if (!existing.owner_before(collectable) && !collectable.owner_before(existing)) {
            return;
        }
    }
    extra_collectables_.push_back(collectable);
    if (exposer_) {
        exposer_->RegisterCollectable(collectable, "/metrics");
    }
    if (gateway_) {
        gateway_->RegisterCollectable(collectable);
    }
}

void PrometheusInstance::unregisterCollectable(const std::weak_ptr<prometheus::Collectable>& collectable) {
    std::scoped_lock lock{metrics_mutex_};
    std::erase_if(extra_collectables_, [&](const auto& w) {
        return w.expired() || (!w.owner_before(collectable) && !collectable.owner_before(w));
    });
    if (exposer_) {
        exposer_->RemoveCollectable(collectable, "/metrics");
    }
}

std::vector<prometheus::MetricFamily> PrometheusInstance::get_metrics() {
    std::shared_ptr<prometheus::Registry> reg;
    std::vector<std::shared_ptr<prometheus::Collectable>> live;
    {
        std::scoped_lock lock{metrics_mutex_};
        reg = registry_;
        live.reserve(extra_collectables_.size());
        for (const auto& w : extra_collectables_) {
            if (auto sp = w.lock()) {
                live.push_back(std::move(sp));
            }
        }
    }
    util::check(reg, "registry_ not set");
    auto families = reg->Collect();
    for (const auto& c : live) {
        auto extra = c->Collect();
        families.insert(families.end(), std::make_move_iterator(extra.begin()), std::make_move_iterator(extra.end()));
    }
    return families;
}

} // Namespace arcticdb
