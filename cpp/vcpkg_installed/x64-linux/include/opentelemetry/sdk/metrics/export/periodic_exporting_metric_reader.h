// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h"
#include "opentelemetry/sdk/metrics/metric_reader.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class PushMetricExporter;

class PeriodicExportingMetricReader : public MetricReader
{

public:
  PeriodicExportingMetricReader(std::unique_ptr<PushMetricExporter> exporter,
                                const PeriodicExportingMetricReaderOptions &option);

  AggregationTemporality GetAggregationTemporality(
      InstrumentType instrument_type) const noexcept override;

private:
  bool OnForceFlush(std::chrono::microseconds timeout) noexcept override;

  bool OnShutDown(std::chrono::microseconds timeout) noexcept override;

  void OnInitialized() noexcept override;

  std::unique_ptr<PushMetricExporter> exporter_;
  std::chrono::milliseconds export_interval_millis_;
  std::chrono::milliseconds export_timeout_millis_;

  void DoBackgroundWork();
  bool CollectAndExportOnce();

  /* The background worker thread */
  std::thread worker_thread_;

  /* Synchronization primitives */
  std::atomic<bool> is_force_flush_pending_{false};
  std::atomic<bool> is_force_wakeup_background_worker_{false};
  std::atomic<bool> is_force_flush_notified_{false};
  std::condition_variable cv_, force_flush_cv_;
  std::mutex cv_m_, force_flush_m_;
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
