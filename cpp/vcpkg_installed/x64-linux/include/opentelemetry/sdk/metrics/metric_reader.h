// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>

#include "opentelemetry/common/spin_lock_mutex.h"
#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/sdk/metrics/data/metric_data.h"
#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
class MetricProducer;
struct ResourceMetrics;

/**
 * MetricReader defines the interface to collect metrics from SDK
 */
class MetricReader
{
public:
  MetricReader();

  void SetMetricProducer(MetricProducer *metric_producer);

  /**
   * Collect the metrics from SDK.
   * @return return the status of the operation.
   */
  bool Collect(nostd::function_ref<bool(ResourceMetrics &metric_data)> callback) noexcept;

  /**
   * Get the AggregationTemporality for given Instrument Type for this reader.
   *
   * @return AggregationTemporality
   */

  virtual AggregationTemporality GetAggregationTemporality(
      InstrumentType instrument_type) const noexcept = 0;

  /**
   * Shutdown the metric reader.
   */
  bool Shutdown(std::chrono::microseconds timeout = std::chrono::microseconds::max()) noexcept;

  /**
   * Force flush the metric read by the reader.
   */
  bool ForceFlush(std::chrono::microseconds timeout = std::chrono::microseconds::max()) noexcept;

  /**
   * Return the status of Metric reader.
   */
  bool IsShutdown() const noexcept;

  virtual ~MetricReader() = default;

private:
  virtual bool OnForceFlush(std::chrono::microseconds timeout) noexcept = 0;

  virtual bool OnShutDown(std::chrono::microseconds timeout) noexcept = 0;

  virtual void OnInitialized() noexcept {}

protected:
private:
  MetricProducer *metric_producer_;
  mutable opentelemetry::common::SpinLockMutex lock_;
  bool shutdown_;
};
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
