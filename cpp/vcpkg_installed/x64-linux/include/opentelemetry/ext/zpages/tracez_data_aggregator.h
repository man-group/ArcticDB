// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>

#include "opentelemetry/ext/zpages/latency_boundaries.h"
#include "opentelemetry/ext/zpages/tracez_data.h"
#include "opentelemetry/ext/zpages/tracez_shared_data.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/trace/span_data.h"
#include "opentelemetry/trace/canonical_code.h"

using opentelemetry::trace::CanonicalCode;

OPENTELEMETRY_BEGIN_NAMESPACE
namespace ext
{
namespace zpages
{
/**
 * TracezDataAggregator object is responsible for collecting raw data and
 * converting it to useful information that can be made available to
 * display on the tracez zpage.
 *
 * When this object is created it starts a thread that calls a function
 * periodically to update the aggregated data with new spans.
 *
 * The only exposed function is a getter that returns a copy of the aggregated
 * data when requested. This function is ensured to be called in sequence to the
 * aggregate spans function which is called periodically.
 *
 * TODO: Consider a singleton pattern for this class, not sure if multiple
 * instances of this class should exist.
 */
class TracezDataAggregator
{
public:
  /**
   * Constructor creates a thread that calls a function to aggregate span data
   * at regular intervals.
   * @param shared_data is the shared set of spans to expose.
   * @param update_interval the time duration for updating the aggregated data.
   */
  TracezDataAggregator(std::shared_ptr<TracezSharedData> shared_data,
                       milliseconds update_interval = milliseconds(10));

  /** Ends the thread set up in the constructor and destroys the object **/
  ~TracezDataAggregator();

  /**
   * GetAggregatedTracezData returns a copy of the updated data.
   * @returns a map with the span name as key and the tracez span data as value.
   */
  std::map<std::string, TracezData> GetAggregatedTracezData();

private:
  /**
   * AggregateSpans is the function that is called to update the aggregated data
   * with newly completed and running span data
   */
  void AggregateSpans();

  /**
   * AggregateCompletedSpans is the function that is called to update the
   * aggregation with the data of newly completed spans.
   * @param completed_spans are the newly completed spans.
   */
  void AggregateCompletedSpans(std::vector<std::unique_ptr<ThreadsafeSpanData>> &completed_spans);

  /**
   * AggregateRunningSpans aggregates the data for all running spans received
   * from the span processor. Running spans are not cleared by the span
   * processor and multiple calls to this function may contain running spans for
   * which data has already been collected in a previous call. Additionally,
   * span names can change while span is running and there seems to be
   * no trivial to way to know if it is a new or old running span so at every
   * call to this function the available running span data is reset and
   * recalculated. At this time there is no unique way to identify a span
   * object once this is done, there might be some better ways to do this.
   * TODO : SpanProcessor is never notified when a span name is changed while it
   * is running and that is propogated to the data aggregator. The running span
   * name if changed while it is running will not be updated in the data
   * aggregator till the span is completed.
   * @param running_spans is the running spans to be aggregated.
   */
  void AggregateRunningSpans(std::unordered_set<ThreadsafeSpanData *> &running_spans);

  /**
   * AggregateStatusOKSpans is the function called to update the data of spans
   * with status code OK.
   * @param ok_span is the span who's data is to be aggregated
   */
  void AggregateStatusOKSpan(std::unique_ptr<ThreadsafeSpanData> &ok_span);

  /**
   * AggregateStatusErrorSpans is the function that is called to update the
   * data of error spans
   * @param error_span is the error span who's data is to be aggregated
   */
  void AggregateStatusErrorSpan(std::unique_ptr<ThreadsafeSpanData> &error_span);

  /**
   * ClearRunningSpanData is a function that is used to clear all running span
   * at the beginning of a call to AggregateSpan data.
   * Running span data has to be cleared before aggregation because running
   * span data is recalculated at every call to AggregateSpans.
   */
  void ClearRunningSpanData();

  /**
   * FindLatencyBoundary finds the latency boundary to which the duration of
   * the given span_data belongs to
   * @ param span_data is the ThreadsafeSpanData whose duration for which the latency
   * boundary is to be found
   * @ returns LatencyBoundary is the latency boundary that the duration belongs
   * to
   */
  LatencyBoundary FindLatencyBoundary(std::unique_ptr<ThreadsafeSpanData> &ok_span);

  /**
   * InsertIntoSampleSpanList is a helper function that is called to insert
   * a given span into a sample span list. A function is used for insertion
   * because list size is to be limited at a set maximum.
   * @param sample_spans the sample span list into which span is to be inserted
   * @param span_data the span_data to be inserted into list
   */
  void InsertIntoSampleSpanList(std::list<ThreadsafeSpanData> &sample_spans,
                                ThreadsafeSpanData &span_data);

  /** Instance of shared spans used to collect raw data **/
  std::shared_ptr<TracezSharedData> tracez_shared_data_;

  /**
   * Tree map with key being the name of the span and value being a unique ptr
   * that stores the tracez span data for the given span name
   * A tree map is preferred to a hash map because the the data is to be ordered
   * in alphabetical order of span name.
   * TODO : A possible memory concern if there are too many unique
   * span names, one solution could be to implement a LRU cache that trims the
   * DS based on frequency of usage of a span name.
   */
  std::map<std::string, TracezData> aggregated_tracez_data_;
  std::mutex mtx_;

  /** A boolean that is set to true in the constructor and false in the
   * destructor to start and end execution of aggregate spans **/
  std::atomic<bool> execute_{false};

  /** Thread that executes aggregate spans at regurlar intervals during this
  object's lifetime**/
  std::thread aggregate_spans_thread_;

  /** Condition variable that notifies the thread when object is about to be
  destroyed **/
  std::condition_variable cv_;
};

}  // namespace zpages
}  // namespace ext
OPENTELEMETRY_END_NAMESPACE
