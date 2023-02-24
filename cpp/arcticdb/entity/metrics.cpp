/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <folly/system/ThreadName.h>
#include <folly/container/F14Map.h>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/pb_util.hpp>

using namespace prometheus;

namespace arcticdb {

    std::shared_ptr<PrometheusInstance> PrometheusInstance::instance(){
        std::call_once(PrometheusInstance::init_flag_, &PrometheusInstance::init);
        return PrometheusInstance::instance_;
    }

    std::shared_ptr<PrometheusInstance> PrometheusInstance::instance_;
    std::once_flag PrometheusInstance::init_flag_;

    std::shared_ptr<PrometheusConfigInstance> PrometheusConfigInstance::instance(){
        std::call_once(PrometheusConfigInstance::init_flag_, &PrometheusConfigInstance::init);
        return PrometheusConfigInstance::instance_;
    }

    std::shared_ptr<PrometheusConfigInstance> PrometheusConfigInstance::instance_;
    std::once_flag PrometheusConfigInstance::init_flag_;

    PrometheusInstance::PrometheusInstance() {

        auto cfg = PrometheusConfigInstance::instance()->config;

        if (cfg.prometheus_model() == PrometheusConfigInstance::Proto::PUSH) {
            // PUSH MODE
            if (cfg.instance().empty() || cfg.host().empty() || cfg.port().empty() || cfg.job_name().empty()) {
                util::raise_rte( "Invalid Push PrometheusConfig {}", arcticdb::util::format(cfg));
            }

            // IMP: This is the GROUPING_KEY - every push overwrites the previous grouping key
            auto labels = prometheus::Gateway::GetInstanceLabel(getHostName());
            mongo_instance_ = cfg.instance();
            labels.insert(std::pair<std::string, std::string>(MONGO_INSTANCE_LABEL, mongo_instance_));
            labels.insert(std::pair<std::string, std::string>(PROMETHEUS_ENV_LABEL, cfg.prometheus_env()));
            gateway_= std::make_shared<prometheus::Gateway>(cfg.host(), cfg.port(), cfg.job_name(), labels);
            registry_ = std::make_shared<prometheus::Registry>();
            gateway_->RegisterCollectable(registry_);

            arcticdb::log::version().info("Prometheus Push created with settings {}", arcticdb::util::format(cfg));

        } else if (cfg.prometheus_model()  == PrometheusConfigInstance::Proto::WEB) {

            // WEB SERVER MODE
            if (cfg.port().empty()) {
                util::raise_rte( "PrometheusConfig web mode port not set {}", arcticdb::util::format(cfg));
            }

            // create an http server ie "http://hostname:"+port()+"/metrics"
            std::string hostname = getHostName();
            std::string endpoint = hostname + ":" + cfg.port();

            // default to 2 threads
            exposer_ = std::make_shared<prometheus::Exposer>(endpoint, 2);

            // create a metrics registry with component=main labels applied to all its
            registry_ = std::make_shared<prometheus::Registry>();

            // 2nd arg defaults to /metrics, make explicit or parameterise
            exposer_->RegisterCollectable(registry_, "/metrics");

            arcticdb::log::version().info("Prometheus endpoint created on {}/metrics", endpoint);
        }
        else {
            arcticdb::log::version().info("Prometheus not configured {}", arcticdb::util::format(cfg));
        }
    }

    // new mechanism, labels at runtime
    void PrometheusInstance::registerMetric(
            prometheus::MetricType type,
            const std::string& name,
            const std::string& help,
            const std::map<std::string, std::string>& staticLabels,
            const std::vector<double>& buckets_list
    ) {
        if (registry_.use_count() == 0) {
            return;
        }

        if (type == prometheus::MetricType::Counter) {
            // Counter is actually a unique_ptr object which has a life of registry
            map_counter_[name] = &prometheus::BuildCounter()
                    .Name(name)
                    .Help(help)
                    .Labels(staticLabels)
                    .Register(*registry_);
        } else if (type == prometheus::MetricType::Gauge) {
            map_gauge_[name] = &prometheus::BuildGauge()
                    .Name(name)
                    .Help(help)
                    .Labels(staticLabels)
                    .Register(*registry_);
        } else if (type == prometheus::MetricType::Histogram) {
            map_histogram_[name].histogram = &prometheus::BuildHistogram()
                    .Name(name)
                    .Help(help)
                    .Labels(staticLabels)
                    .Register(*registry_);
            map_histogram_[name].buckets_list = buckets_list;
        } else if (type == prometheus::MetricType::Summary) {
            map_summary_[name] = &prometheus::BuildSummary()
                    .Name(name)
                    .Help(help)
                    .Labels(staticLabels)
                    .Register(*registry_);
        } else {
            arcticdb::log::version().warn("Unsupported metric type");
        }
}

    // update new cardinal counter
    void PrometheusInstance::incrementCounter(const std::string& name, const std::map<std::string, std::string>& labels) {
        if (registry_.use_count() == 0)
            return;

        if (map_counter_.count(name) != 0) {
            // Add returns Counter&
            map_counter_[name]->Add(labels).Increment();
        } else {
            arcticdb::log::version().warn("Unregistered counter metric {}", name);
        }
    }
    void PrometheusInstance::incrementCounter(const std::string& name, double value, const std::map<std::string, std::string>& labels) {
        if (registry_.use_count() == 0)
            return;

        if (map_counter_.count(name) != 0) {
            // Add returns Counter&
            map_counter_[name]->Add(labels).Increment(value);
        } else {
            arcticdb::log::version().warn("Unregistered counter metric {}", name);
        }
    }
    void PrometheusInstance::setGauge(const std::string& name, double value, const std::map<std::string, std::string>& labels) {
        if (registry_.use_count() == 0)
            return;

        if (map_gauge_.count(name) != 0) {
            map_gauge_[name]->Add(labels).Set(value);
        } else {
            arcticdb::log::version().warn("Unregistered gauge metric {}", name);
        }
    }
    void PrometheusInstance::setGaugeCurrentTime(const std::string& name, const std::map<std::string, std::string>& labels) {
        if (registry_.use_count() == 0)
            return;

        if (map_gauge_.count(name) != 0) {
            map_gauge_[name]->Add(labels).SetToCurrentTime();
        } else {
            arcticdb::log::version().warn("Unregistered gauge metric {}", name);
        }
    }
    void PrometheusInstance::observeHistogram(const std::string& name, double value, const std::map<std::string, std::string>& labels) {
        if (registry_.use_count() == 0)
            return;
        if (auto it=map_histogram_.find(name); it != map_histogram_.end()) {
            it->second.histogram->Add(labels, it->second.buckets_list).Observe(value);
        } else {
            arcticdb::log::version().warn("Unregistered Histogram metric {}", name);
        }
    }
    void PrometheusInstance::DeleteHistogram(const std::string& name, const std::map<std::string, std::string>& labels) {
        if (registry_.use_count() == 0)
            return;

        if (auto it=map_histogram_.find(name); it != map_histogram_.end()) {
            it->second.histogram->Remove(&it->second.histogram->Add(labels, it->second.buckets_list));
        } else {
            arcticdb::log::version().warn("Unregistered Histogram metric {}", name);
        }
    }
    void PrometheusInstance::observeSummary(const std::string& name, double value, const std::map<std::string, std::string>& labels) {
        if (registry_.use_count() == 0)
            return;

        if (map_summary_.count(name) != 0) {
            //TODO DMK quantiles
            map_summary_[name]->Add(labels,Summary::Quantiles{ {0.1, 0.05}, {0.2, 0.05}, {0.3, 0.05}, {0.4, 0.05}, {0.5, 0.05}, {0.6, 0.05}, {0.7, 0.05}, {0.8, 0.05}, {0.9, 0.05}, {0.9, 0.05}, {1.0, 0.05}}, std::chrono::seconds{SUMMARY_MAX_AGE}, SUMMARY_AGE_BUCKETS).Observe(value);
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

} // Namespace arcticdb

