// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class MeterSelector;

class MeterSelectorFactory
{
public:
  static std::unique_ptr<MeterSelector> Create(opentelemetry::nostd::string_view name,
                                               opentelemetry::nostd::string_view version,
                                               opentelemetry::nostd::string_view schema);
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
