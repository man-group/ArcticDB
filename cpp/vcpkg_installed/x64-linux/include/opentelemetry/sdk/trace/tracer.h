// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/common/macros.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/instrumentationscope/instrumentation_scope.h"
#include "opentelemetry/sdk/trace/samplers/always_on.h"
#include "opentelemetry/sdk/trace/tracer_context.h"
#include "opentelemetry/trace/tracer.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace resource
{
class Resource;
}  // namespace resource

namespace trace
{
class IdGenerator;
class SpanProcessor;

using namespace opentelemetry::sdk::instrumentationscope;

class Tracer final : public opentelemetry::trace::Tracer,
                     public std::enable_shared_from_this<Tracer>
{
public:
  /** Construct a new Tracer with the given context pipeline. */
  explicit Tracer(std::shared_ptr<TracerContext> context,
                  std::unique_ptr<InstrumentationScope> instrumentation_scope =
                      InstrumentationScope::Create("")) noexcept;

  nostd::shared_ptr<opentelemetry::trace::Span> StartSpan(
      nostd::string_view name,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links,
      const opentelemetry::trace::StartSpanOptions &options = {}) noexcept override;

  void ForceFlushWithMicroseconds(uint64_t timeout) noexcept override;

  void CloseWithMicroseconds(uint64_t timeout) noexcept override;

  /** Returns the configured span processor. */
  SpanProcessor &GetProcessor() noexcept { return context_->GetProcessor(); }

  /** Returns the configured Id generator */
  IdGenerator &GetIdGenerator() const noexcept { return context_->GetIdGenerator(); }

  /** Returns the associated instrumentation scope */
  const InstrumentationScope &GetInstrumentationScope() const noexcept
  {
    return *instrumentation_scope_;
  }

  OPENTELEMETRY_DEPRECATED_MESSAGE("Please use GetInstrumentationScope instead")
  const InstrumentationScope &GetInstrumentationLibrary() const noexcept
  {
    return GetInstrumentationScope();
  }

  /** Returns the currently configured resource **/
  const opentelemetry::sdk::resource::Resource &GetResource() { return context_->GetResource(); }

  // Note: Test only
  Sampler &GetSampler() { return context_->GetSampler(); }

private:
  // order of declaration is important here - instrumentation scope should destroy after
  // tracer-context.
  std::shared_ptr<InstrumentationScope> instrumentation_scope_;
  std::shared_ptr<TracerContext> context_;
};
}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
