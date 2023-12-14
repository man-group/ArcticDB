// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <vector>

#include "opentelemetry/context/context.h"
#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/sdk/common/attribute_utils.h"
#include "opentelemetry/sdk/metrics/exemplar/reservoir.h"
#include "opentelemetry/sdk/metrics/exemplar/reservoir_cell.h"
#include "opentelemetry/sdk/metrics/exemplar/reservoir_cell_selector.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class FixedSizeExemplarReservoir : public ExemplarReservoir
{

public:
  FixedSizeExemplarReservoir(size_t size,
                             std::shared_ptr<ReservoirCellSelector> reservoir_cell_selector,
                             std::shared_ptr<ExemplarData> (ReservoirCell::*map_and_reset_cell)(
                                 const common::OrderedAttributeMap &attributes))
      : storage_(size),
        reservoir_cell_selector_(reservoir_cell_selector),
        map_and_reset_cell_(map_and_reset_cell)
  {}

  void OfferMeasurement(
      int64_t value,
      const MetricAttributes &attributes,
      const opentelemetry::context::Context &context,
      const opentelemetry::common::SystemTimestamp & /* timestamp */) noexcept override
  {
    if (!reservoir_cell_selector_)
    {
      return;
    }
    auto idx =
        reservoir_cell_selector_->ReservoirCellIndexFor(storage_, value, attributes, context);
    if (idx != -1)
    {
      storage_[idx].RecordLongMeasurement(value, attributes, context);
    }
  }

  void OfferMeasurement(
      double value,
      const MetricAttributes &attributes,
      const opentelemetry::context::Context &context,
      const opentelemetry::common::SystemTimestamp & /* timestamp */) noexcept override
  {
    if (!reservoir_cell_selector_)
    {
      return;
    }
    auto idx =
        reservoir_cell_selector_->ReservoirCellIndexFor(storage_, value, attributes, context);
    if (idx != -1)
    {
      storage_[idx].RecordDoubleMeasurement(value, attributes, context);
    }
  }

  std::vector<std::shared_ptr<ExemplarData>> CollectAndReset(
      const MetricAttributes &pointAttributes) noexcept override
  {
    std::vector<std::shared_ptr<ExemplarData>> results;
    if (!reservoir_cell_selector_)
    {
      return results;
    }
    if (!map_and_reset_cell_)
    {
      reservoir_cell_selector_.reset();
      return results;
    }
    for (auto reservoirCell : storage_)
    {
      auto result = (reservoirCell.*(map_and_reset_cell_))(pointAttributes);
      results.push_back(result);
    }
    reservoir_cell_selector_.reset();
    return results;
  }

private:
  explicit FixedSizeExemplarReservoir() = default;
  std::vector<ReservoirCell> storage_;
  std::shared_ptr<ReservoirCellSelector> reservoir_cell_selector_;
  std::shared_ptr<ExemplarData> (ReservoirCell::*map_and_reset_cell_)(
      const common::OrderedAttributeMap &attributes);
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
