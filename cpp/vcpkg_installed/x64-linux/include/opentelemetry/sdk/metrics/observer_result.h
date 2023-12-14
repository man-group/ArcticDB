// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <unordered_map>

#include "opentelemetry/common/key_value_iterable.h"
#include "opentelemetry/metrics/observer_result.h"
#include "opentelemetry/sdk/metrics/state/attributes_hashmap.h"
#include "opentelemetry/sdk/metrics/view/attributes_processor.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
template <class T>
class ObserverResultT final : public opentelemetry::metrics::ObserverResultT<T>
{
public:
  explicit ObserverResultT(const AttributesProcessor *attributes_processor = nullptr)
      : attributes_processor_(attributes_processor)
  {}

  ~ObserverResultT() override = default;

  void Observe(T value) noexcept override { data_.insert({{}, value}); }

  void Observe(T value, const opentelemetry::common::KeyValueIterable &attributes) noexcept override
  {
    if (attributes_processor_)
    {
      auto attr = attributes_processor_->process(attributes);
      data_.insert({attr, value});
    }
    else
    {
      data_.insert({MetricAttributes{attributes}, value});
    }
  }

  const std::unordered_map<MetricAttributes, T, AttributeHashGenerator> &GetMeasurements()
  {
    return data_;
  }

private:
  std::unordered_map<MetricAttributes, T, AttributeHashGenerator> data_;
  const AttributesProcessor *attributes_processor_;
};
}  // namespace metrics
}  // namespace sdk

OPENTELEMETRY_END_NAMESPACE
