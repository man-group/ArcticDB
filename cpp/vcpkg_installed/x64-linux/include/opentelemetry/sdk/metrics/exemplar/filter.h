// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/common/attribute_utils.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace context
{
class Context;
}  // namespace context

namespace sdk
{
namespace common
{
class OrderedAttributeMap;
}  // namespace common

namespace metrics
{
using MetricAttributes = opentelemetry::sdk::common::OrderedAttributeMap;

/**
 * Exemplar filters are used to pre-filter measurements before attempting to store them in a
 * reservoir.
 */
class ExemplarFilter
{
public:
  // Returns whether or not a reservoir should attempt to filter a measurement.
  virtual bool ShouldSampleMeasurement(int64_t value,
                                       const MetricAttributes &attributes,
                                       const opentelemetry::context::Context &context) noexcept = 0;

  // Returns whether or not a reservoir should attempt to filter a measurement.
  virtual bool ShouldSampleMeasurement(double value,
                                       const MetricAttributes &attributes,
                                       const opentelemetry::context::Context &context) noexcept = 0;

  virtual ~ExemplarFilter() = default;

  static std::shared_ptr<ExemplarFilter> GetNeverSampleFilter() noexcept;
  static std::shared_ptr<ExemplarFilter> GetAlwaysSampleFilter() noexcept;
  static std::shared_ptr<ExemplarFilter> GetWithTraceSampleFilter() noexcept;
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
