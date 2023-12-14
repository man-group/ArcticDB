// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/sdk/metrics/exemplar/filter.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace context
{
class Context;
}  // namespace context

namespace sdk
{
namespace metrics
{

class AlwaysSampleFilter final : public ExemplarFilter
{
public:
  bool ShouldSampleMeasurement(
      int64_t /* value */,
      const MetricAttributes & /* attributes */,
      const opentelemetry::context::Context & /* context */) noexcept override
  {
    return true;
  }

  bool ShouldSampleMeasurement(
      double /* value */,
      const MetricAttributes & /* attributes */,
      const opentelemetry::context::Context & /* context */) noexcept override
  {
    return true;
  }

  explicit AlwaysSampleFilter() = default;
};
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
