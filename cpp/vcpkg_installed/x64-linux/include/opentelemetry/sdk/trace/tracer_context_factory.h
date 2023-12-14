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

namespace trace
{
class IdGenerator;
class Sampler;
class SpanProcessor;
class TracerContext;

/**
 * Factory class for TracerContext.
 */
class OPENTELEMETRY_EXPORT TracerContextFactory
{
public:
  /**
   * Create a TracerContext.
   */
  static std::unique_ptr<TracerContext> Create(
      std::vector<std::unique_ptr<SpanProcessor>> &&processors);

  /**
   * Create a TracerContext.
   */
  static std::unique_ptr<TracerContext> Create(
      std::vector<std::unique_ptr<SpanProcessor>> &&processors,
      const opentelemetry::sdk::resource::Resource &resource);

  /**
   * Create a TracerContext.
   */
  static std::unique_ptr<TracerContext> Create(
      std::vector<std::unique_ptr<SpanProcessor>> &&processors,
      const opentelemetry::sdk::resource::Resource &resource,
      std::unique_ptr<Sampler> sampler);

  /**
   * Create a TracerContext.
   */
  static std::unique_ptr<TracerContext> Create(
      std::vector<std::unique_ptr<SpanProcessor>> &&processors,
      const opentelemetry::sdk::resource::Resource &resource,
      std::unique_ptr<Sampler> sampler,
      std::unique_ptr<IdGenerator> id_generator);
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
