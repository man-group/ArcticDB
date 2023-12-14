// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "opentelemetry/ext/zpages/threadsafe_span_data.h"
#include "opentelemetry/ext/zpages/tracez_shared_data.h"
#include "opentelemetry/sdk/trace/processor.h"
#include "opentelemetry/sdk/trace/recordable.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace ext
{
namespace zpages
{
/*
 * The span processor passes and stores running and completed recordables (casted as span_data)
 * to be used by the TraceZ Data Aggregator.
 */
class TracezSpanProcessor : public opentelemetry::sdk::trace::SpanProcessor
{
public:
  /*
   * Initialize a span processor.
   */
  explicit TracezSpanProcessor(std::shared_ptr<TracezSharedData> shared_data) noexcept
      : shared_data_(shared_data)
  {}

  /*
   * Create a span recordable, which is span_data
   * @return a newly initialized recordable
   */
  std::unique_ptr<opentelemetry::sdk::trace::Recordable> MakeRecordable() noexcept override
  {
    return std::unique_ptr<opentelemetry::sdk::trace::Recordable>(new ThreadsafeSpanData);
  }

  /*
   * OnStart is called when a span starts; the recordable is cast to span_data and added to
   * running_spans.
   * @param span a recordable for a span that was just started
   */
  void OnStart(opentelemetry::sdk::trace::Recordable &span,
               const opentelemetry::trace::SpanContext &parent_context) noexcept override;

  /*
   * OnEnd is called when a span ends; that span_data is moved from running_spans to
   * completed_spans
   * @param span a recordable for a span that was ended
   */
  void OnEnd(std::unique_ptr<opentelemetry::sdk::trace::Recordable> &&span) noexcept override;

  /*
   * For now, does nothing. In the future, it
   * may send all ended spans that have not yet been sent to the aggregator.
   * @param timeout an optional timeout. Currently, timeout does nothing.
   * @return return the status of the operation.
   */
  bool ForceFlush(std::chrono::microseconds timeout OPENTELEMETRY_MAYBE_UNUSED =
                      std::chrono::microseconds::max()) noexcept override
  {
    return true;
  }

  /*
   * Shut down the processor and do any cleanup required, which is none.
   * After the call to Shutdown, subsequent calls to OnStart, OnEnd, ForceFlush
   * or Shutdown will return immediately without doing anything.
   * @param timeout an optional timeout, the default timeout of 0 means that no
   * timeout is applied. Currently, timeout does nothing.
   * @return return the status of the operation.
   */
  bool Shutdown(std::chrono::microseconds timeout OPENTELEMETRY_MAYBE_UNUSED =
                    std::chrono::microseconds::max()) noexcept override
  {
    return true;
  }

private:
  std::shared_ptr<TracezSharedData> shared_data_;
};
}  // namespace zpages
}  // namespace ext
OPENTELEMETRY_END_NAMESPACE
