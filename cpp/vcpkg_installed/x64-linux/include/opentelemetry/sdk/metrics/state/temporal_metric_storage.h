// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <list>
#include <memory>
#include <unordered_map>

#include "opentelemetry/common/spin_lock_mutex.h"
#include "opentelemetry/common/timestamp.h"
#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
class AggregationConfig;
class AttributesHashMap;
class CollectorHandle;
class MetricData;

struct LastReportedMetrics
{
  std::unique_ptr<AttributesHashMap> attributes_map;
  opentelemetry::common::SystemTimestamp collection_ts;
};

class TemporalMetricStorage
{
public:
  TemporalMetricStorage(InstrumentDescriptor instrument_descriptor,
                        AggregationType aggregation_type,
                        const AggregationConfig *aggregation_config);

  bool buildMetrics(CollectorHandle *collector,
                    nostd::span<std::shared_ptr<CollectorHandle>> collectors,
                    opentelemetry::common::SystemTimestamp sdk_start_ts,
                    opentelemetry::common::SystemTimestamp collection_ts,
                    std::shared_ptr<AttributesHashMap> delta_metrics,
                    nostd::function_ref<bool(MetricData)> callback) noexcept;

private:
  InstrumentDescriptor instrument_descriptor_;
  AggregationType aggregation_type_;

  // unreported metrics stash for all the collectors
  std::unordered_map<CollectorHandle *, std::list<std::shared_ptr<AttributesHashMap>>>
      unreported_metrics_;
  // last reported metrics stash for all the collectors.
  std::unordered_map<CollectorHandle *, LastReportedMetrics> last_reported_metrics_;

  // Lock while building metrics
  mutable opentelemetry::common::SpinLockMutex lock_;
  const AggregationConfig *aggregation_config_;
};
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
