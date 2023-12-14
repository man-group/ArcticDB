// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
class SpanExporter;
class SpanProcessor;

/**
 * Factory class for SimpleSpanProcessor.
 */
class OPENTELEMETRY_EXPORT SimpleSpanProcessorFactory
{
public:
  /**
   * Create a SimpleSpanProcessor.
   */
  static std::unique_ptr<SpanProcessor> Create(std::unique_ptr<SpanExporter> &&exporter);
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
