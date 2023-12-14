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
class TracezSharedData
{
public:
  struct CollectedSpans
  {
    std::unordered_set<ThreadsafeSpanData *> running;
    std::vector<std::unique_ptr<ThreadsafeSpanData>> completed;
  };

  /*
   * Initialize a shared data storage.
   */
  explicit TracezSharedData() noexcept {}

  /*
   * Called when a span has been started.
   */
  void OnStart(ThreadsafeSpanData *span) noexcept;

  /*
   * Called when a span has ended.
   */
  void OnEnd(std::unique_ptr<ThreadsafeSpanData> &&span) noexcept;

  /*
   * Returns a snapshot of all spans stored. This snapshot has a copy of the
   * stored running_spans and gives ownership of completed spans to the caller.
   * Stored completed_spans are cleared from the processor. Currently,
   * copy-on-write is utilized where possible to minimize contention, but locks
   * may be added in the future.
   * @return snapshot of all currently running spans and newly completed spans
   * (spans never sent while complete) at the time that the function is called
   */
  CollectedSpans GetSpanSnapshot() noexcept;

private:
  mutable std::mutex mtx_;
  CollectedSpans spans_;
};
}  // namespace zpages
}  // namespace ext
OPENTELEMETRY_END_NAMESPACE
