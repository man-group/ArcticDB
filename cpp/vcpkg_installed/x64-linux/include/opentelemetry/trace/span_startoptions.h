// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/common/timestamp.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace trace
{

/**
 * StartSpanOptions provides options to set properties of a Span at the time of
 * its creation
 */
struct StartSpanOptions
{
  // Optionally sets the start time of a Span.
  //
  // If the start time of a Span is set, timestamps from both the system clock
  // and steady clock must be provided.
  //
  // Timestamps from the steady clock can be used to most accurately measure a
  // Span's duration, while timestamps from the system clock can be used to most
  // accurately place a Span's
  // time point relative to other Spans collected across a distributed system.
  common::SystemTimestamp start_system_time;
  common::SteadyTimestamp start_steady_time;

  // Explicitly set the parent of a Span.
  //
  // This defaults to an invalid span context. In this case, the Span is
  // automatically parented to the currently active span.
  nostd::variant<SpanContext, context::Context> parent = SpanContext::GetInvalid();

  // TODO:
  // SpanContext remote_parent;
  // Links
  SpanKind kind = SpanKind::kInternal;
};

}  // namespace trace
OPENTELEMETRY_END_NAMESPACE
