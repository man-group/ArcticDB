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
class Sampler;

/**
 * Factory class for ParentBasedSampler.
 */
class ParentBasedSamplerFactory
{
public:
  /**
   * Create a ParentBasedSampler.
   */
  static std::unique_ptr<Sampler> Create(std::shared_ptr<Sampler> delegate_sampler);
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
