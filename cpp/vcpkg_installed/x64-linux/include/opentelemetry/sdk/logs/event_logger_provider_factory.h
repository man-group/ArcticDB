// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0/

#pragma once

#include <memory>
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace logs
{
class EventLoggerProvider;
}  // namespace logs

namespace sdk
{
namespace logs
{

/**
 * Factory class for EventLoggerProvider.
 */
class EventLoggerProviderFactory
{
public:
  /**
   * Create a EventLoggerProvider.
   */
  static std::unique_ptr<opentelemetry::logs::EventLoggerProvider> Create();
};

}  // namespace logs
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
