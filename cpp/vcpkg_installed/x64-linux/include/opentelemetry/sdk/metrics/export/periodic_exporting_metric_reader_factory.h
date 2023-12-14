// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h"
#include "opentelemetry/sdk/metrics/metric_reader.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class MetricReader;
class PushMetricExporter;

class PeriodicExportingMetricReaderFactory
{
public:
  static std::unique_ptr<MetricReader> Create(std::unique_ptr<PushMetricExporter> exporter,
                                              const PeriodicExportingMetricReaderOptions &option);
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
