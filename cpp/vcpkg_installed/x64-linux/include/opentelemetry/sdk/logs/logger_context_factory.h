// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <vector>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace resource
{
class Resource;
}  // namespace resource

namespace logs
{
class LoggerContext;
class LogRecordProcessor;

/**
 * Factory class for LoggerContext.
 */
class LoggerContextFactory
{
public:
  /**
   * Create a LoggerContext.
   */
  static std::unique_ptr<LoggerContext> Create(
      std::vector<std::unique_ptr<LogRecordProcessor>> &&processors);

  /**
   * Create a LoggerContext.
   */
  static std::unique_ptr<LoggerContext> Create(
      std::vector<std::unique_ptr<LogRecordProcessor>> &&processors,
      const opentelemetry::sdk::resource::Resource &resource);
};

}  // namespace logs
}  // namespace sdk

OPENTELEMETRY_END_NAMESPACE
