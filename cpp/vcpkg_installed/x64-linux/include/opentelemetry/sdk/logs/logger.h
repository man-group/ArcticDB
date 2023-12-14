// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <string>

#include "opentelemetry/common/macros.h"
#include "opentelemetry/logs/logger.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/unique_ptr.h"
#include "opentelemetry/sdk/instrumentationscope/instrumentation_scope.h"
#include "opentelemetry/sdk/logs/logger_context.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace logs
{
class LoggerProvider;

class Logger final : public opentelemetry::logs::Logger
{
public:
  /**
   * Initialize a new logger.
   * @param name The name of this logger instance
   * @param context The logger provider that owns this logger.
   */
  explicit Logger(
      opentelemetry::nostd::string_view name,
      std::shared_ptr<LoggerContext> context,
      std::unique_ptr<instrumentationscope::InstrumentationScope> instrumentation_scope =
          instrumentationscope::InstrumentationScope::Create("")) noexcept;

  /**
   * Returns the name of this logger.
   */
  const opentelemetry::nostd::string_view GetName() noexcept override;

  nostd::unique_ptr<opentelemetry::logs::LogRecord> CreateLogRecord() noexcept override;

  using opentelemetry::logs::Logger::EmitLogRecord;

  void EmitLogRecord(
      nostd::unique_ptr<opentelemetry::logs::LogRecord> &&log_record) noexcept override;

  /** Returns the associated instrumentation scope */
  const opentelemetry::sdk::instrumentationscope::InstrumentationScope &GetInstrumentationScope()
      const noexcept;

  OPENTELEMETRY_DEPRECATED_MESSAGE("Please use GetInstrumentationScope instead")
  const opentelemetry::sdk::instrumentationscope::InstrumentationScope &GetInstrumentationLibrary()
      const noexcept
  {
    return GetInstrumentationScope();
  }

private:
  // The name of this logger
  std::string logger_name_;

  // order of declaration is important here - instrumentation scope should destroy after
  // logger-context.
  std::unique_ptr<instrumentationscope::InstrumentationScope> instrumentation_scope_;
  std::shared_ptr<LoggerContext> context_;
};

}  // namespace logs
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
