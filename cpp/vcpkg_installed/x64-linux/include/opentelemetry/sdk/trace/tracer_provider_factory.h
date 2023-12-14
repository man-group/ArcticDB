// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <vector>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace trace
{
class TracerProvider;
}  // namespace trace

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
 * Factory class for TracerProvider.
 * See @ref TracerProvider.
 */
class OPENTELEMETRY_EXPORT TracerProviderFactory
{
public:
  /* Serie of builders with a single processor. */

  static std::unique_ptr<opentelemetry::trace::TracerProvider> Create(
      std::unique_ptr<SpanProcessor> processor);

  static std::unique_ptr<opentelemetry::trace::TracerProvider> Create(
      std::unique_ptr<SpanProcessor> processor,
      const opentelemetry::sdk::resource::Resource &resource);

  static std::unique_ptr<opentelemetry::trace::TracerProvider> Create(
      std::unique_ptr<SpanProcessor> processor,
      const opentelemetry::sdk::resource::Resource &resource,
      std::unique_ptr<Sampler> sampler);

  static std::unique_ptr<opentelemetry::trace::TracerProvider> Create(
      std::unique_ptr<SpanProcessor> processor,
      const opentelemetry::sdk::resource::Resource &resource,
      std::unique_ptr<Sampler> sampler,
      std::unique_ptr<IdGenerator> id_generator);

  /* Serie of builders with a vector of processor. */

  static std::unique_ptr<opentelemetry::trace::TracerProvider> Create(
      std::vector<std::unique_ptr<SpanProcessor>> &&processors);

  static std::unique_ptr<opentelemetry::trace::TracerProvider> Create(
      std::vector<std::unique_ptr<SpanProcessor>> &&processors,
      const opentelemetry::sdk::resource::Resource &resource);

  static std::unique_ptr<opentelemetry::trace::TracerProvider> Create(
      std::vector<std::unique_ptr<SpanProcessor>> &&processors,
      const opentelemetry::sdk::resource::Resource &resource,
      std::unique_ptr<Sampler> sampler);

  static std::unique_ptr<opentelemetry::trace::TracerProvider> Create(
      std::vector<std::unique_ptr<SpanProcessor>> &&processors,
      const opentelemetry::sdk::resource::Resource &resource,
      std::unique_ptr<Sampler> sampler,
      std::unique_ptr<IdGenerator> id_generator);

  /* Create with a tracer context. */

  static std::unique_ptr<opentelemetry::trace::TracerProvider> Create(
      std::unique_ptr<TracerContext> context);
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
