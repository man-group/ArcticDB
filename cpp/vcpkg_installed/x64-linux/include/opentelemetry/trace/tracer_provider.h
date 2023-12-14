// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace trace
{

class Tracer;

/**
 * Creates new Tracer instances.
 */
class OPENTELEMETRY_EXPORT TracerProvider
{
public:
  virtual ~TracerProvider() = default;
  /**
   * Gets or creates a named tracer instance.
   *
   * Optionally a version can be passed to create a named and versioned tracer
   * instance.
   */
  virtual nostd::shared_ptr<Tracer> GetTracer(nostd::string_view library_name,
                                              nostd::string_view library_version = "",
                                              nostd::string_view schema_url      = "") noexcept = 0;
};
}  // namespace trace
OPENTELEMETRY_END_NAMESPACE
