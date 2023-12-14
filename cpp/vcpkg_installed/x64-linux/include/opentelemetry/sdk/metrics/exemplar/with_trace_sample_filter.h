// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/sdk/metrics/exemplar/filter.h"
#include "opentelemetry/trace/context.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class WithTraceSampleFilter final : public ExemplarFilter
{
public:
  bool ShouldSampleMeasurement(int64_t /* value */,
                               const MetricAttributes & /* attributes */,
                               const opentelemetry::context::Context &context) noexcept override
  {
    return hasSampledTrace(context);
  }

  bool ShouldSampleMeasurement(double /* value */,
                               const MetricAttributes & /* attributes */,
                               const opentelemetry::context::Context &context) noexcept override
  {
    return hasSampledTrace(context);
  }

  explicit WithTraceSampleFilter() = default;

private:
  static bool hasSampledTrace(const opentelemetry::context::Context &context)
  {
    return opentelemetry::trace::GetSpan(context)->GetContext().IsValid() &&
           opentelemetry::trace::GetSpan(context)->GetContext().IsSampled();
  }
};
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
