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
class IdGenerator;

/**
 * Factory class for RandomIdGenerator.
 */
class RandomIdGeneratorFactory
{
public:
  /**
   * Create a RandomIdGenerator.
   */
  static std::unique_ptr<IdGenerator> Create();
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
