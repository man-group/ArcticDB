// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class ViewRegistry;

class ViewRegistryFactory
{
public:
  static std::unique_ptr<ViewRegistry> Create();
};
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
