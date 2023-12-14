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
struct BatchSpanProcessorOptions;

/**
 * Factory class for BatchSpanProcessor.
 */
class OPENTELEMETRY_EXPORT BatchSpanProcessorFactory
{
public:
  /**
   * Create a BatchSpanProcessor.
   */
  static std::unique_ptr<SpanProcessor> Create(std::unique_ptr<SpanExporter> &&exporter,
                                               const BatchSpanProcessorOptions &options);
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
