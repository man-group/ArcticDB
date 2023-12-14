// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <iostream>
#include <list>
#include <string>

#include "opentelemetry/ext/zpages/threadsafe_span_data.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/trace/span_data.h"
#include "opentelemetry/trace/canonical_code.h"
#include "opentelemetry/trace/span_id.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/version.h"

using opentelemetry::ext::zpages::ThreadsafeSpanData;
using opentelemetry::trace::CanonicalCode;
using opentelemetry::trace::SpanId;
using opentelemetry::trace::TraceId;

OPENTELEMETRY_BEGIN_NAMESPACE
namespace ext
{
namespace zpages
{

/**
 * kMaxNumberOfSampleSpans is the maximum number of running, completed or error
 * sample spans stored at any given time for a given span name.
 * This limit is introduced to reduce memory usage by trimming sample spans
 * stored.
 */
const int kMaxNumberOfSampleSpans = 5;

/**
 * TracezData is the data to be displayed for tracez zpages that is stored for
 * each span name.
 */
struct TracezData
{
  /**
   * TODO: At this time the maximum count is unknown but a larger data type
   * might have to be used in the future to store these counts to avoid overflow
   */
  unsigned int running_span_count;
  unsigned int error_span_count;

  /**
   * completed_span_count_per_latency_bucket is an array that stores the count
   * of spans for each of the 9 latency buckets.
   */
  std::array<unsigned int, kLatencyBoundaries.size()> completed_span_count_per_latency_bucket;

  /**
   * sample_latency_spans is an array of lists, each index of the array
   * corresponds to a latency boundary(of which there are 9).
   * The list in each index stores the sample spans for that latency boundary.
   */
  std::array<std::list<ThreadsafeSpanData>, kLatencyBoundaries.size()> sample_latency_spans;

  /**
   * sample_error_spans is a list that stores the error samples for a span name.
   */
  std::list<ThreadsafeSpanData> sample_error_spans;

  /**
   * sample_running_spans is a list that stores the running span samples for a
   * span name.
   */
  std::list<ThreadsafeSpanData> sample_running_spans;

  TracezData()
  {
    running_span_count = 0;
    error_span_count   = 0;
    completed_span_count_per_latency_bucket.fill(0);
  }
};

}  // namespace zpages
}  // namespace ext
OPENTELEMETRY_END_NAMESPACE
