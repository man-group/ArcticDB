// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <iostream>
#include <memory>

#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
class PushMetricExporter;
}  // namespace metrics
}  // namespace sdk

namespace exporter
{
namespace metrics
{

/**
 * Factory class for OStreamMetricExporter.
 */
class OPENTELEMETRY_EXPORT OStreamMetricExporterFactory
{
public:
  /**
   * Creates an OStreamMetricExporter writing to the default location.
   */
  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create();

  /**
   * Creates an OStreamMetricExporter writing to the given location.
   */
  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create(
      std::ostream &sout);

  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create(
      std::ostream &sout,
      sdk::metrics::AggregationTemporality aggregation_temporality);
};

}  // namespace metrics
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
